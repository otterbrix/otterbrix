// ============================================================================
// BATCHED CREATE INDEX backfill.
//
// operator_create_index_backfill_t STREAMS the table through the
// storage_fetch_next_batch cursor primitive (the same fetch-next source the
// streaming scans use), so peak memory = one batch + index state — never the
// whole table materialized in a single storage_scan_segment(0, total_rows).
//
// WHAT THESE TESTS ASSERT:
//   (a) BATCHING — a CREATE INDEX over a table LARGER than one scan batch
//       (kRowCount >> DEFAULT_VECTOR_CAPACITY) must consume MORE THAN ONE batch,
//       observed via components::operators::create_index_backfill_batches()
//       (DEV_MODE instrumentation bumped once per non-empty fetched batch).
//   (b) VISIBILITY — the backfilled index is usable after commit: an equality
//       predicate on the indexed column returns exactly the pre-existing rows
//       (the PENDING -> commit_inserts contract must still publish the entries).
//   (c) ATOMICITY — a CREATE INDEX aborted inside an explicit transaction leaves
//       NO index behind: re-creating it by the same name in a fresh autocommit
//       txn succeeds (a lingering half-built index would fail "index already
//       exists").
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_create_index_backfill.hpp>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024): the backfill scan must span MANY fetch
    // batches, so the batched loop iterates well past 1.
    constexpr unsigned kRowCount = 6000;
    constexpr int kGroups = 2; // grp in {0,1} -> each group ~3000 rows

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto cur = seed_rows(dispatcher, "IdxDb.t", "id, grp, val", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % kGroups) << ", " << (i * 10) << ")";
            return s.str();
        });
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

TEST_CASE("integration::cpp::create_index_backfill::large_table_streams_multiple_batches") {
    // disk ON: the backfill branch only runs when a disk actor is wired, and
    // storage_fetch_next_batch is the disk-backed streaming scan under test.
    auto config = make_test_config("/tmp/otterbrix/integration/test_create_index_backfill/batched",
                                   /*disk_on=*/true,
                                   /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());

    // Populate BEFORE the index exists, so CREATE INDEX must backfill the existing rows.
    seed(dispatcher);

    // BATCHING: the streaming backfill must consume more than one fetch batch.
    const auto batches_before = components::operators::create_index_backfill_batches();
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    const auto batches_after = components::operators::create_index_backfill_batches();
    REQUIRE(batches_after > batches_before + 1); // > 1 batch iteration -> streamed, not materialized

    // VISIBILITY + CORRECTNESS: the backfilled index is usable after commit. An
    // equality predicate on the indexed grp column returns exactly the group.
    const unsigned kExpectedInGroup0 = (kRowCount + 1) / kGroups; // even ids -> grp 0
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM IdxDb.t WHERE grp = 0;");
        INFO("indexed SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kExpectedInGroup0);
    }
    // Scalar aggregate over the indexed predicate: COUNT matches the backfilled count.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE grp = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / kGroups));
    }
}

TEST_CASE("integration::cpp::create_index_backfill::aborted_create_index_leaves_no_index") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_create_index_backfill/abort",
                                   /*disk_on=*/true,
                                   /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    seed(dispatcher);

    // ATOMICITY: create the index inside an explicit transaction, then ROLLBACK.
    // The still-uncommitted index (and its PENDING backfill entries) must be dropped
    // so no half-built index survives the abort.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "BEGIN;")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "ROLLBACK;")->is_success());
    }

    // The aborted index left nothing behind: re-creating it by the same name in a
    // fresh autocommit txn succeeds (a lingering index would fail "already exists").
    {
        auto cur = exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);");
        INFO("re-CREATE INDEX error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    // And that re-created (committed) index is usable + correct.
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == (kRowCount + 1) / kGroups);
    }
}

TEST_CASE("integration::cpp::create_index_backfill::backfill_after_delete_maps_correct_row_ids") {
    // ROW-ID ALIGNMENT: the batched backfill streams the table through an
    // MVCC-filtered fetch-next scan, which SKIPS deleted rows — the physical row
    // ids of the scanned rows are GAPPED. The index must be built with the
    // batches' TRUE row ids; a re-derived contiguous 0..N-1 stamping shifts every
    // entry after the first gap onto the wrong storage row and index-backed
    // lookups return wrong rows.
    auto config = make_test_config("/tmp/otterbrix/integration/test_create_index_backfill/after_delete",
                                   /*disk_on=*/true,
                                   /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    seed(dispatcher);

    // Punch a hole at the physical HEAD of the table: every surviving row's
    // physical id is now strictly greater than its backfill-scan position.
    constexpr unsigned kDeleted = 100; // ids [0, 100)
    {
        auto cur = exec(dispatcher, "DELETE FROM IdxDb.t WHERE id < 100;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kDeleted);
    }

    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());

    // Index-backed lookups return exactly the surviving group rows. Alive ids are
    // [100, 6000) and grp = id % 2, so each group holds (6000-100)/2 rows.
    const unsigned kAlivePerGroup = (kRowCount - kDeleted) / kGroups;
    {
        auto cur = exec(dispatcher, "SELECT id, grp FROM IdxDb.t WHERE grp = 1;");
        INFO("indexed SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kAlivePerGroup);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kAlivePerGroup));
    }
    // Value-level spot check through the indexed predicate: the smallest
    // surviving id in group 1 is 101.
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE grp = 1 ORDER BY id LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 101);
    }
}
