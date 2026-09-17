// operator_create_index_backfill_t streams the table via storage_fetch_next_batch (peak memory =
// one batch + index state) instead of materializing the whole table; these tests cover batching,
// post-commit visibility, abort atomicity, and correct row-id mapping across a mid-table gap.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_create_index_backfill.hpp>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024), so the backfill scan spans many fetch batches.
    constexpr unsigned kRowCount = 6000;
    constexpr int kGroups = 2;

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
    // disk ON: the backfill branch only runs with a disk actor wired.
    auto config = make_test_config(integration_fixture_path("test_create_index_backfill/batched"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());

    seed(dispatcher);

    const auto batches_before = components::operators::create_index_backfill_batches();
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    const auto batches_after = components::operators::create_index_backfill_batches();
    REQUIRE(batches_after > batches_before + 1);

    const unsigned kExpectedInGroup0 = (kRowCount + 1) / kGroups;
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM IdxDb.t WHERE grp = 0;");
        INFO("indexed SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kExpectedInGroup0);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE grp = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / kGroups));
    }
}

TEST_CASE("integration::cpp::create_index_backfill::aborted_create_index_leaves_no_index") {
    auto config = make_test_config(integration_fixture_path("test_create_index_backfill/abort"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    seed(dispatcher);

    // ATOMICITY: rolling back must drop the uncommitted index and its PENDING backfill entries.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "BEGIN;")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "ROLLBACK;")->is_success());
    }

    {
        auto cur = exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);");
        INFO("re-CREATE INDEX error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == (kRowCount + 1) / kGroups);
    }
}

TEST_CASE("integration::cpp::create_index_backfill::backfill_after_delete_maps_correct_row_ids") {
    // The backfill's MVCC-filtered scan skips deleted rows, so physical row ids are gapped; a
    // re-derived contiguous 0..N-1 stamping would shift entries after the gap onto the wrong row.
    auto config = make_test_config(integration_fixture_path("test_create_index_backfill/after_delete"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    seed(dispatcher);

    constexpr unsigned kDeleted = 100;
    {
        auto cur = exec(dispatcher, "DELETE FROM IdxDb.t WHERE id < 100;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kDeleted);
    }

    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());

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
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE grp = 1 ORDER BY id LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 101);
    }
}

TEST_CASE("integration::cpp::create_index_backfill::backfill_over_a_mid_batch_hole") {
    // Pins WHY the backfill may hand its batch to the index by move instead of copying it. The batch's
    // row ids are synthesized contiguously by the operator itself (fetch_ids[k] = base + k) and read
    // back with fetch_visibility_t::RAW, which keeps tombstoned rows in the answer -- so a batch is
    // ALWAYS one run, run_len == sz always holds, and the partial_copy leg is unreachable by
    // construction rather than merely untested. This case punches holes in the MIDDLE of two fetch
    // windows -- the shape that WOULD split a run if the fetch ever filtered -- and requires the
    // partial-copy counter to stay at zero. If someone switches that fetch to a filtered visibility,
    // this goes red and says so: the leg has become live and now needs coverage of its own.
    auto config = make_test_config(integration_fixture_path("test_create_index_backfill/mid_hole"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    seed(dispatcher);

    // Both even, so both belong to grp 0; both sit mid-window (500 in [0,1024), 3000 in [2048,3072)).
    constexpr int64_t kHoleA = 500;
    constexpr int64_t kHoleB = 3000;
    REQUIRE(exec(dispatcher, "DELETE FROM IdxDb.t WHERE id = 500;")->is_success());
    REQUIRE(exec(dispatcher, "DELETE FROM IdxDb.t WHERE id = 3000;")->is_success());

    const auto partials_before = components::operators::create_index_backfill_partial_copies();
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    const auto partials_after = components::operators::create_index_backfill_partial_copies();

    INFO("RAW visibility keeps the deleted rows in the batch, so the ids stay contiguous and the "
         "partial_copy leg stays unreachable -- if this fires, that leg went live and is untested");
    REQUIRE(partials_after == partials_before);

    // grp 0 is the even ids: 0, 2, ... 5998 -- 3000 of them, minus the two holes.
    constexpr unsigned kGrp0Alive = kRowCount / kGroups - 2;
    {
        auto cur = exec(dispatcher, "SELECT id, grp FROM IdxDb.t WHERE grp = 0;");
        INFO("indexed SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kGrp0Alive);
        // The teeth: a partial_copy that shifted its rows would hand the index entries for one row id
        // while naming another, and the answer for grp = 0 would carry ODD ids. Checking every row is
        // what makes an off-by-one in that leg visible.
        for (std::size_t r = 0; r < cur->size(); ++r) {
            const auto id = cur->value(0, r).value<int64_t>();
            const auto grp = cur->value(1, r).value<int32_t>();
            INFO("row " << r << " answered id=" << id << " grp=" << grp);
            REQUIRE(grp == 0);
            REQUIRE(id % 2 == 0);
            REQUIRE(id != kHoleA);
            REQUIRE(id != kHoleB);
        }
    }
    // The untouched half must be exact too: a shifted mapping would have stolen rows into it.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE grp = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / kGroups));
    }
}
