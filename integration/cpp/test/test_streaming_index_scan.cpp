// ============================================================================
// Streaming INDEX SCAN — operator_index_scan as a push-based SOURCE.
//
// operator_index_scan (operator_type::index_scan) is the scan the planner builds
// for a pure-compare predicate over an INDEXED column (e.g. WHERE grp = 7 when an
// index exists on grp). It is now role()==source: an index-rooted SELECT/DML runs
// through the push-based STREAMING executor (execute_pipeline).
//
// The index search is ONE-SHOT (the whole matched row-id set returns in a single
// future), so the source materializes the ids ONCE and then WINDOWS the per-row-id
// storage_fetch: each source_next() fetches exactly one <= DEFAULT_VECTOR_CAPACITY
// slice of the matched ids and returns it as ONE chunk. Peak fetch memory is one
// window, not the whole matched set. source_next() and the retained materialized
// await_async_and_resume share ONE windowing core (open_index_window + fetch_window),
// so results are identical regardless of which path runs (R6).
//
// WHAT THESE TESTS ASSERT:
//   (a) PATH — an index-rooted SELECT over a matched set >> DEFAULT_VECTOR_CAPACITY
//       (so the source emits MANY windows) routes through execute_pipeline, proven
//       by streaming_pipeline_runs() bumping.
//   (b) CORRECTNESS — the streamed index scan returns exactly the rows the
//       materialize path produced, including point-lookups, OFFSET/LIMIT on the
//       indexed scan, and index-rooted DELETE/UPDATE (RETURNING + index mirror).
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024): a single grp value matches MANY rows so the
    // index-scan source must fetch the matched ids across MULTIPLE windows — the
    // bounded windowed-fetch property under test.
    constexpr unsigned kRowCount = 6000;
    constexpr int kGroups = 2; // grp in {0,1} -> each group ~3000 rows (>> 1024)

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        std::stringstream q;
        q << "INSERT INTO IdxDb.t (id, grp, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << (i % kGroups) << ", " << (i * 10) << ")"
              << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_index_scan::indexed_select_streams_many_windows") {
    auto config = test_create_config("/tmp/test_streaming_index_scan_select");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    seed(dispatcher);

    const unsigned kExpectedInGroup0 = (kRowCount + 1) / kGroups; // even ids -> grp 0

    // PATH + CORRECTNESS: an equality predicate on the INDEXED grp column -> index_scan
    // (role()==source). The matched set (~3000 ids) >> DEFAULT_VECTOR_CAPACITY, so the
    // source emits MANY windows. Streams through execute_pipeline and returns exactly
    // the rows in the group.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM IdxDb.t WHERE grp = 0;");
        INFO("indexed SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kExpectedInGroup0);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed through execute_pipeline (windowed source)

    // CORRECTNESS via a scalar aggregate over the same indexed predicate: COUNT equals
    // the matched-row count (exercises the empty/real-window path through a sink).
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kExpectedInGroup0));
    }
}

TEST_CASE("integration::cpp::streaming_index_scan::point_lookup_and_empty_result") {
    auto config = test_create_config("/tmp/test_streaming_index_scan_point");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_id ON IdxDb.t (id);")->is_success());
    seed(dispatcher);

    // Point-lookup on the unique indexed id -> exactly one matched id, one single-window
    // fetch.
    {
        auto cur = exec(dispatcher, "SELECT id, val FROM IdxDb.t WHERE id = 4000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 4000);  // col id, row 0
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 40000); // col val, row 0
    }

    // No matched id -> the source drains immediately; a scalar aggregate must still
    // emit COUNT=0 via the one-time schema'd empty-guard.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM IdxDb.t WHERE id = 999999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE id = 999999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::streaming_index_scan::offset_limit_on_indexed_scan") {
    auto config = test_create_config("/tmp/test_streaming_index_scan_offlim");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    seed(dispatcher);

    // OFFSET/LIMIT applied to the [pos_, end_) window over the matched ids (the shared
    // windowing core), spanning a window boundary (LIMIT 1500 > DEFAULT_VECTOR_CAPACITY).
    constexpr unsigned kLimit = 1500;
    {
        auto cur = exec(dispatcher,
                        "SELECT id FROM IdxDb.t WHERE grp = 0 LIMIT " + std::to_string(kLimit) + " OFFSET 100;");
        INFO("OFFSET/LIMIT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kLimit);
    }
}

TEST_CASE("integration::cpp::streaming_index_scan::indexed_delete_and_update") {
    auto config = test_create_config("/tmp/test_streaming_index_scan_dml");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE IdxDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE IdxDb.t (id bigint, grp int, val bigint);")->is_success());
    // Index on grp drives the index_scan SOURCE feeding the DML sink; index on id is the
    // mirror-consistency check for the streamed DELETE/UPDATE.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON IdxDb.t (grp);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_id ON IdxDb.t (id);")->is_success());
    seed(dispatcher);

    const unsigned kInGroup1 = kRowCount / kGroups; // odd ids -> grp 1

    // Index-rooted DELETE: index_scan(source) -> DELETE(sink). Removes the whole group;
    // the matched set >> DEFAULT_VECTOR_CAPACITY so the source windows the fetch.
    {
        auto cur = exec(dispatcher, "DELETE FROM IdxDb.t WHERE grp = 1 RETURNING id;");
        INFO("indexed DELETE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup1);
    }
    // Index mirror consistency: a deleted (odd-id) row is gone via the id index.
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE id = 4001;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    // A surviving (even-id) row is still present.
    {
        auto cur = exec(dispatcher, "SELECT id FROM IdxDb.t WHERE id = 4000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // Index-rooted UPDATE over the surviving group: index_scan(source) -> UPDATE(sink).
    constexpr int kBump = 7;
    {
        auto cur = exec(dispatcher, "UPDATE IdxDb.t SET val = val + " + std::to_string(kBump) +
                                        " WHERE grp = 0 RETURNING id;");
        INFO("indexed UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == (kRowCount + 1) / kGroups);
    }
    // The bump landed.
    {
        auto cur = exec(dispatcher, "SELECT val FROM IdxDb.t WHERE id = 4000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 40000 + kBump);
    }
}
