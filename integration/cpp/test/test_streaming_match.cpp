// ============================================================================
// Streaming MATCH — filter operator on the push-based read path.
//
// operator_match_t (operator_type::match) is the filter/projection operator the
// planner builds for a WHERE predicate that is NOT a pure compare (e.g. LIKE,
// which lowers to compare_type::regex) — a pure compare is pushed into the scan
// (full_scan/index_scan) instead. The non-pushable predicate yields a
//   match(streaming) -> full_scan(source)
// chain, so a filtered SELECT now runs through the push-based STREAMING executor
// (execute_pipeline): the source emits one batch at a time, match's push()
// filters that batch, and surviving rows flow up — peak memory is one batch
// instead of the whole materialized scan.
//
// WHAT THESE TESTS ASSERT:
//   (a) PATH — a filtered SELECT over a MULTI-BATCH scan (>> DEFAULT_VECTOR_CAPACITY
//       rows) routes through execute_pipeline, proven by streaming_pipeline_runs()
//       bumping. Stubbing operator_match_t::role() back to none makes this RED.
//   (b) CORRECTNESS — the streamed filter returns exactly the rows the materialize
//       path produced, including a LIMIT that must cap the TOTAL emitted across
//       batch boundaries (the cross-batch limit_total_ state), not per batch.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024): forces the scan source to emit MANY
    // batches so match's push() must filter across batch boundaries and carry the
    // LIMIT/OFFSET counter across them — the bounded streaming property under test.
    constexpr unsigned kRowCount = 5000;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_match::like_filter_streams_and_lands") {
    auto config = test_create_config("/tmp/test_streaming_match_like");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.t (id bigint, name text);")->is_success());

    // Seed a multi-batch row set. names: row i -> "match_<i>" when i is even,
    // "other_<i>" when i is odd, so LIKE 'match%' selects exactly the even-id rows.
    {
        std::stringstream q;
        q << "INSERT INTO MatchDb.t (id, name) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            const char* prefix = (i % 2 == 0) ? "match_" : "other_";
            q << "(" << i << ", '" << prefix << i << "')" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    const unsigned kExpectedMatches = kRowCount / 2; // even ids -> "match_*"

    // PATH + CORRECTNESS: a LIKE filter (non-pure-compare -> operator_match_t over a
    // full_scan source) over a multi-batch scan routes through the streaming
    // pipeline and returns exactly the matching rows.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "SELECT id, name FROM MatchDb.t WHERE name LIKE 'match%';");
        INFO("LIKE filter error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kExpectedMatches);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed through execute_pipeline

    // CORRECTNESS via an aggregate over the same predicate: COUNT equals the
    // matching-row count.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM MatchDb.t WHERE name LIKE 'match%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kExpectedMatches));
    }
}

TEST_CASE("integration::cpp::streaming_match::like_filter_with_limit_caps_across_batches") {
    auto config = test_create_config("/tmp/test_streaming_match_limit");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchDb.t (id bigint, name text);")->is_success());

    // Every row matches the LIKE predicate ("row_<i>" all start with "row"), so the
    // result is bounded ONLY by the LIMIT, not by predicate selectivity. (A selective
    // predicate would interact with the scan's offset+limit head-cap and obscure the
    // streaming LIMIT logic under test.)
    {
        std::stringstream q;
        q << "INSERT INTO MatchDb.t (id, name) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", 'row_" << i << "')" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // LIMIT must cap the TOTAL number of emitted rows across ALL streamed batches.
    // kLimit (2000) > DEFAULT_VECTOR_CAPACITY (1024), so match's push() filters at
    // least two batches and must carry the running LIMIT counter across the batch
    // boundary — a per-batch reset (the streaming bug this guards against) would
    // over-emit. Exactly kLimit rows must come back.
    constexpr unsigned kLimit = 2000;
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "SELECT id, name FROM MatchDb.t WHERE name LIKE 'row%' LIMIT " + std::to_string(kLimit) + ";");
        INFO("LIKE+LIMIT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kLimit);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed through execute_pipeline
}

// ============================================================================
// operator_match OVER A SINK — the crux of unconditional streaming.
//
// A match over a SCAN source carries REAL absolute row_ids (the scan stamps
// them). A match over a SINK output (a GROUP BY / JOIN result) carries NO real
// row_ids: those operators never write data_chunk_t::row_ids, so the slots are
// zero-filled. Two operator_match defects bite ONLY over a sink:
//   (a) row_ids: filter_batch_ copies the input chunk's row_ids forward. Over a
//       sink they are all 0 → a downstream DML/index consumer dereferences
//       storage at the bogus absolute id 0 (a real row), corrupting/deleting the
//       wrong row.
//   (b) predicate resource: the cached predicate was allocated on the FIRST
//       pushed batch's arena; over a sink the finalize chunk's arena can differ,
//       so the cached value-getter closures dangle on the 2nd chunk.
//
// These shapes were RED (segfault / wrong rows) when operator_match_t::role()
// was flipped to UNCONDITIONALLY streaming, before the two defect fixes. They
// assert: correct rows for the SELECT shapes, correct DML effect (and no crash)
// for the DML-over-sink shapes.
// ============================================================================

namespace {
    // Seed a tiny employees/departments pair (mirrors test_subqueries) so the
    // match-over-sink shapes have a GROUP BY / JOIN sink to filter above.
    void setup_match_over_sink_db(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE SinkDb;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE SinkDb.emp (id bigint, dept_id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE SinkDb.dept (id bigint, name text);")->is_success());
        // dept_id 1 has 3 rows, dept_id 2 has 2 rows, dept_id 3 has 1 row.
        REQUIRE(exec(dispatcher,
                     "INSERT INTO SinkDb.emp (id, dept_id, name) VALUES "
                     "(1,1,'alice'), (2,1,'amy'), (3,1,'bob'), "
                     "(4,2,'carol'), (5,2,'amanda'), "
                     "(6,3,'dave');")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "INSERT INTO SinkDb.dept (id, name) VALUES (1,'eng'), (2,'sales'), (3,'hr');")
                    ->is_success());
    }
} // namespace

TEST_CASE("integration::cpp::streaming_match::having_count_filter_returns_correct_rows") {
    // SELECT correctness over a GROUP BY sink: a HAVING predicate filters the
    // grouped result. dept_id 1 (3 rows) and dept_id 2 (2 rows) survive c > 1;
    // dept_id 3 (1 row) does not. This match (over a group sink) must return
    // exactly the surviving groups — defect (b) would dangle the predicate on a
    // multi-chunk grouped result.
    auto config = test_create_config("/tmp/test_streaming_match_having");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    auto cur = exec(dispatcher,
                    "SELECT dept_id, COUNT(*) AS c FROM SinkDb.emp GROUP BY dept_id HAVING COUNT(*) > 1;");
    INFO("HAVING error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2); // dept_id 1 and 2
}

TEST_CASE("integration::cpp::streaming_match::join_with_nonpushdown_filter_returns_correct_rows") {
    // SELECT correctness over a JOIN sink: a non-pushdown LIKE predicate filters
    // the joined result. Joining emp to dept on dept_id then keeping names that
    // LIKE 'a%' must return alice, amy, amanda (3) — match sits above the join
    // sink, whose output carries no row_ids.
    auto config = test_create_config("/tmp/test_streaming_match_join_filter");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    auto cur = exec(dispatcher,
                    "SELECT e.id, e.name FROM SinkDb.emp e "
                    "JOIN SinkDb.dept d ON e.dept_id = d.id "
                    "WHERE e.name LIKE 'a%';");
    INFO("JOIN+LIKE error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3); // alice, amy, amanda
}

TEST_CASE("integration::cpp::streaming_match::delete_where_in_group_subquery_lands") {
    // DML over a GROUP BY sink-derived subquery: DELETE the duplicate-key rows.
    // dept_id values with COUNT(*) > 1 are {1, 2}; deleting employees whose
    // dept_id is in that set removes the 5 rows of dept_id 1 and 2, leaving the
    // single dept_id 3 row. The subquery's grouped output has NO real row_ids;
    // defect (a) would feed a bogus absolute id 0 to the storage delete and
    // segfault / delete the wrong row. After the fix the right rows are deleted.
    auto config = test_create_config("/tmp/test_streaming_match_delete_group");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    {
        auto cur = exec(dispatcher,
                        "DELETE FROM SinkDb.emp WHERE dept_id IN "
                        "(SELECT dept_id FROM SinkDb.emp GROUP BY dept_id HAVING COUNT(*) > 1);");
        INFO("DELETE IN group-subquery error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5); // 3 (dept 1) + 2 (dept 2)
    }
    // CORRECTNESS: exactly the single dept_id 3 row remains, with the right id.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM SinkDb.emp WHERE dept_id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 6); // dave
    }
}

TEST_CASE("integration::cpp::streaming_match::delete_using_with_nonpushdown_filter_lands") {
    // DML over a JOIN sink: DELETE ... USING with a non-pushdown filter on the
    // target. Delete emp rows that join dept on dept_id AND whose name LIKE 'a%'.
    // Matching emp: alice(1,d1), amy(2,d1), amanda(5,d2) → 3 rows. The match over
    // the join sink carries no real row_ids; defect (a) would feed bogus id 0 to
    // the storage delete. After the fix exactly those 3 rows are deleted.
    auto config = test_create_config("/tmp/test_streaming_match_delete_using");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup_match_over_sink_db(dispatcher);

    {
        auto cur = exec(dispatcher,
                        "DELETE FROM SinkDb.emp USING SinkDb.dept "
                        "WHERE SinkDb.emp.dept_id = SinkDb.dept.id AND SinkDb.emp.name LIKE 'a%';");
        INFO("DELETE USING+LIKE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // alice, amy, amanda
    }
    // CORRECTNESS: 3 rows remain (bob, carol, dave); the 'a%' names are gone.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) AS c FROM SinkDb.emp WHERE name LIKE 'a%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 0);
    }
}
