// ============================================================================
// Aggregate-pushdown TRAFFIC-REDUCTION verification.
//
// The whole POINT of pushing a mergeable aggregate down to the owning agent is
// that only the FINALIZED partial result crosses the agent->coordinator mailbox
// — one scalar row for a scalar aggregate, or one row per group for a GROUP BY —
// NOT the raw scanned rows. Every other pushdown test asserts the RESULT is
// correct (which is true whether or not pushdown runs, by design); none asserts
// the traffic actually shrank. These tests do.
//
// Instrumentation: services::disk::pushdown_reply_rows() is a DEV_MODE-only
// process-global counter bumped inside storage_fetch_next_batch_inner's reduce OPEN by the
// sum of data_chunk_t::size() over the reply chunks — i.e. EXACTLY the rows that
// will cross the mailbox. Each test resets it, runs ONE aggregate over a large
// filtered table, then asserts the reply-row count is TINY (== 1 for a scalar,
// == #groups for a GROUP BY) — orders of magnitude below the thousands of rows
// the agent scanned, proving only the partial crossed the wire. This is the
// UNIQUE value of this file; result-VALUE correctness is covered by
// test_aggregate_pushdown_e2e and deliberately not re-asserted here.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/disk/agent_disk.hpp>
#include <sstream>

using namespace test_helpers;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024): the agent's local scan streams several
    // batches, so a non-pushed plan would ship thousands of rows to the coordinator.
    constexpr unsigned kRowCount = 5000;

    // WHERE v >= kFilterLo keeps rows [kFilterLo, kRowCount): a LARGE matched set
    // (thousands of scanned rows) that must still collapse to a tiny reply.
    constexpr int64_t kFilterLo = 1000;
    constexpr unsigned kGroups = 4; // g = id % 4 — low-cardinality GROUP BY key

    // Seed `db.t(id bigint, g bigint, v bigint)` with kRowCount rows via one
    // multi-row VALUES insert: id=i, g=i%kGroups, v=i.
    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& fq_table) {
        auto cur = seed_rows(dispatcher, fq_table, "id, g, v", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % kGroups) << ", " << i << ")";
            return s.str();
        });
        INFO("seed error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

// ---------------------------------------------------------------------------
// (1) SCALAR aggregate over thousands of filtered rows ships exactly ONE row.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::pushdown_traffic::scalar_ships_one_row") {
    auto config = make_test_config(integration_fixture_path("test_pushdown_traffic/scalar"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TrafficDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TrafficDb.t (id bigint, g bigint, v bigint);")->is_success());
    seed(dispatcher, "TrafficDb.t");

    // matched rows over the slice v in [kFilterLo, kRowCount) — thousands scanned
    // agent-side, and the yardstick the tiny reply must undercut.
    const uint64_t matched = kRowCount - static_cast<unsigned>(kFilterLo);

    // Reset the mailbox-traffic counter, run ONE scalar aggregate, read the delta.
    services::disk::reset_pushdown_reply_rows();
    {
        std::stringstream q;
        q << "SELECT SUM(v) AS s, COUNT(*) AS c FROM TrafficDb.t WHERE v >= " << kFilterLo << ";";
        auto cur = exec(dispatcher, q.str());
        INFO("scalar aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // Value correctness lives in test_aggregate_pushdown_e2e; this file owns the
        // TRAFFIC assertion below.
    }
    const uint64_t reply_rows = services::disk::pushdown_reply_rows();

    // TRAFFIC REDUCTION: exactly ONE row crossed the agent->coordinator mailbox,
    // even though the agent scanned `matched` (thousands of) rows. This is the
    // property under test — a non-pushed plan would ship all `matched` rows.
    REQUIRE(reply_rows == 1);
    REQUIRE(reply_rows < matched); // sanity: tiny reply << scanned input
}

// ---------------------------------------------------------------------------
// (2) GROUP BY on a low-cardinality key ships exactly ONE row PER GROUP.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::pushdown_traffic::grouped_ships_one_row_per_group") {
    auto config = make_test_config(integration_fixture_path("test_pushdown_traffic/grouped"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TrafficDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TrafficDb.t (id bigint, g bigint, v bigint);")->is_success());
    seed(dispatcher, "TrafficDb.t");

    // matched rows over the slice v in [kFilterLo, kRowCount) — thousands scanned
    // agent-side; the grouped reply must still be just one row per group.
    const uint64_t matched = kRowCount - static_cast<unsigned>(kFilterLo);

    services::disk::reset_pushdown_reply_rows();
    {
        std::stringstream q;
        q << "SELECT g, COUNT(*) AS c FROM TrafficDb.t WHERE v >= " << kFilterLo << " GROUP BY g ORDER BY g ASC;";
        auto cur = exec(dispatcher, q.str());
        INFO("grouped aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kGroups);
        // Per-group value correctness lives in test_aggregate_pushdown_e2e.
    }
    const uint64_t reply_rows = services::disk::pushdown_reply_rows();

    // TRAFFIC REDUCTION: exactly kGroups rows crossed the mailbox — one finalized
    // partial per group — not the thousands of raw rows the agent scanned.
    REQUIRE(reply_rows == kGroups);
    REQUIRE(reply_rows < matched); // sanity: tiny reply << scanned input
}
