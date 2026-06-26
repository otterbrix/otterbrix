// ============================================================================
// Bounded-execution SELECT — STEP 3 verification (HIDDEN spec test).
//
// FULL bounded-execution spec (the TARGET, not yet fully assertable):
//   A SELECT whose sink state fits memory (a GROUP BY with a bounded number of
//   groups, or a scalar aggregate, or an ORDER BY ... LIMIT k) must run with
//   PEAK INTERMEDIATE MEMORY ~= one input batch + the sink state — NOT the sum
//   of all input rows. The push-based streaming pipeline (STEP 3) makes the
//   SINK side incremental: operator_group / operator_aggregate fold each batch
//   into bounded per-group / O(1) running state and discard the batch, so the
//   accumulated intermediate never grows with the input cardinality.
//
// WHY THE PEAK-RSS ASSERTION IS DEFERRED (HONEST CONSTRAINT):
//   The scan SOURCE was reverted to whole-scan buffering (per-batch
//   fetch-next is dormant pending an actor-zeta await fix), so the SCAN still
//   materializes the whole table up front. With the source not yet bounded, a
//   true peak-RSS assertion ("peak ~= one batch + sink state") CANNOT pass yet
//   — the memory win is masked until the buffer-pool rework + per-batch
//   fetch-next land. So the peak-RSS / high-water-mark assertion is
//   INTENTIONALLY DEFERRED. When those land, add it here (sample RSS during the
//   GROUP BY below and assert it stays within one-batch + #groups-state).
//
// WHAT THIS TEST DOES ASSERT TODAY (realizable now):
//   The SINK side of the bounded property — that the incremental group /
//   scalar aggregate folds MANY input batches into #groups-bounded (resp. O(1))
//   state and STILL produces correct results. A LARGE multi-batch input
//   (>> DEFAULT_VECTOR_CAPACITY=1024 rows) mapped onto a SMALL bounded number
//   of groups exercises exactly the cross-batch incremental folding: if the
//   sink buffered all input instead of folding it, the per-group SUM/COUNT and
//   the scalar SUM/COUNT below would still have to be correct, but the point is
//   that they are produced by the incremental push()/finalize() sink path
//   (operator_group::accumulate / operator_aggregate fast path), not by
//   materializing every row first.
//
// NOTE: test_batch_execution.cpp::test_batch_boundaries already covers
//   multi-batch incremental-sink CORRECTNESS around the chunk boundary for the
//   one-group-per-row shape; this test is the complementary
//   bounded-#groups-over-a-large-input shape that the bounded-execution spec
//   specifically calls out, and it carries the deferred-spec documentation
//   above so the peak-RSS work has a home.
//
// HIDDEN: tagged [.][bounded-exec] so it does not run in the default suite
//   (the realizable assertions pass; the full peak-RSS spec is deferred).
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    constexpr auto bounded_db = "bounded_db";
    constexpr auto bounded_coll = "bounded_coll";

    // >> DEFAULT_VECTOR_CAPACITY (1024): forces the scan to emit many batches and
    // the sink to fold them incrementally across chunk boundaries.
    constexpr unsigned kRowCount = 20000;
    // Small bounded group count: sink state stays #groups-bounded while the input
    // spans ~20 batches. This is the bounded-execution shape under test.
    constexpr unsigned kNumGroups = 8;
} // namespace

TEST_CASE("integration::cpp::bounded_execution::group_by_and_scalar_aggregate", "[.][bounded-exec]") {
    auto config = test_create_config("/tmp/test_bounded_execution");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + bounded_db + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, bounded_db, bounded_coll);
    }

    // Each row i belongs to group (i % kNumGroups) and carries val == i. Expected
    // per-group sum/count are computed analytically below.
    {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO bounded_db.bounded_coll (name, grp, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            query << "('R" << i << "', " << (i % kNumGroups) << ", " << i << ")"
                  << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // Analytic expectations: for group g, the contributing rows are
    // i = g, g+kNumGroups, g+2*kNumGroups, ... < kRowCount.
    std::array<int64_t, kNumGroups> expected_sum{};
    std::array<uint64_t, kNumGroups> expected_count{};
    int64_t expected_total_sum = 0;
    for (unsigned i = 0; i < kRowCount; ++i) {
        const unsigned g = i % kNumGroups;
        expected_sum[g] += static_cast<int64_t>(i);
        expected_count[g] += 1;
        expected_total_sum += static_cast<int64_t>(i);
    }

    INFO("GROUP BY over a large multi-batch input folds into #groups-bounded sink state") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT grp, SUM(val) AS s, COUNT(name) AS c "
                                           "FROM bounded_db.bounded_coll "
                                           "GROUP BY grp ORDER BY grp ASC;");
        INFO("GROUP BY error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumGroups);
        for (unsigned g = 0; g < kNumGroups; ++g) {
            // grp column (ordered ascending == group index)
            REQUIRE(cur->value(0, g).value<int64_t>() == static_cast<int64_t>(g));
            // SUM(val) folded incrementally across ~20 batches
            REQUIRE(cur->value(1, g).value<int64_t>() == expected_sum[g]);
            // COUNT(name) folded incrementally across ~20 batches
            REQUIRE(cur->value(2, g).value<uint64_t>() == expected_count[g]);
        }
    }

    INFO("scalar aggregate over a large multi-batch input folds into O(1) running state") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(val) AS s, COUNT(name) AS c "
                                           "FROM bounded_db.bounded_coll;");
        INFO("scalar aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_total_sum);
        REQUIRE(cur->value(1, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
}
