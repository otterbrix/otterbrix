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
