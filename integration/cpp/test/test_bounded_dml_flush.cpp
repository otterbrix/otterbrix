// ============================================================================
// Bounded / spillable DML sink — MID-PUMP FLUSH verification.
//
// The bounded DML sinks (insert / update / delete) flush INCREMENTALLY mid-pump
// whenever a per-op buffered_rows() >= config.execution.dml_flush_row_threshold.
// The default threshold is 0 == DISABLED (a single post-pump flush), so NO other
// test exercises the incremental path. These tests
// set a SMALL threshold on a dedicated spaces instance and drive multi-batch DML
// so the sink flushes MORE THAN ONCE, then assert (via the DEV_MODE
// executor::dml_flush_count() counter) that the mid-flush path actually ran AND
// that results stay correct + atomic across flush boundaries.
//
// WHAT THESE TESTS ASSERT:
//   (1) INSERT...SELECT over a multi-batch scan flushes >1 time and every row
//       lands with values intact.
//   (2) A multi-batch UPDATE (with and without RETURNING) flushes >1 time and the
//       affected count / returned rows / persisted values are correct.
//   (3) ATOMICITY: a statement that ERRORS after one or more mid-flushes reverts
//       ALL rows — none are visible (the mid-flushed physical appends are lifted).
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>
#include <sstream>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024) so a SELECT scan emits several batches:
    // with a small flush threshold the DML sink must flush once per batch, i.e.
    // MANY times across the pump — the property under test.
    constexpr unsigned kRowCount = 3000;

    // Small enough that any 1024-row scan batch immediately trips the mid-pump
    // gate: one mid-flush per scan batch => strictly more than one flush.
    constexpr uint64_t kFlushThreshold = 512;

    // Seed `db.tbl(id bigint, grp int, val bigint)` with kRowCount rows via a
    // single multi-row VALUES insert (id=i, grp=i%8, val=i*2).
    void seed_source(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& fq_table) {
        auto cur = seed_rows(dispatcher, fq_table, "id, grp, val", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % 8) << ", " << (i * 2) << ")";
            return s.str();
        });
        INFO("seed error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

// ---------------------------------------------------------------------------
// (1) INSERT...SELECT mid-flushes >1 time and every row lands correctly.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::bounded_dml_flush::insert_select_mid_flushes") {
    auto config = make_test_config("/tmp/test_bounded_dml_flush/insert_select");
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.dst (id bigint, grp int, val bigint);")->is_success());
    seed_source(dispatcher, "FlushDb.src");

    // Drive the streaming INSERT...SELECT and measure the mid-flush delta ACROSS
    // just this statement.
    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "INSERT INTO FlushDb.dst (id, grp, val) SELECT id, grp, val FROM FlushDb.src;");
        INFO("INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();

    // The mid-flush path RAN more than once (a scan of 3000 rows over 1024-row
    // batches, threshold 512 => one mid-flush per batch). With threshold==0 this
    // delta would be 0 (single post-pump flush) — proving the incremental path.
    REQUIRE(flushes_after - flushes_before > 1);

    // CORRECTNESS: every source row landed with values intact across the flushes.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
}

// ---------------------------------------------------------------------------
// (2) Multi-batch UPDATE — WITHOUT and WITH RETURNING — is correct across the
//     mid-flush boundaries (affected count, returned rows, persisted values).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::bounded_dml_flush::update_mid_flushes") {
    auto config = make_test_config("/tmp/test_bounded_dml_flush/update");
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.t (id bigint, grp int, val bigint);")->is_success());
    seed_source(dispatcher, "FlushDb.t");

    // UPDATE WITHOUT RETURNING over the full multi-batch scan: SET val = val + 1.
    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "UPDATE FlushDb.t SET val = val + 1;");
        INFO("UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        // Affected-row count: every row matched.
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();
    REQUIRE(flushes_after - flushes_before > 1);

    // Every row got exactly +1: SUM(val) == original + kRowCount.
    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2 + 1;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
    // Row count unchanged (UPDATE, not INSERT).
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    // UPDATE WITH RETURNING over the full multi-batch scan: SET val = val + 10
    // RETURNING id. The returned-row COUNT must equal the matched-row count, and
    // the mid-flush path must have run again for THIS statement.
    const auto flushes_before2 = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "UPDATE FlushDb.t SET val = val + 10 RETURNING id;");
        INFO("UPDATE RETURNING error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        // One returned row per updated row, accumulated across every flush.
        REQUIRE(cur->size() == kRowCount);
    }
    const auto flushes_after2 = services::collection::executor::dml_flush_count();
    REQUIRE(flushes_after2 - flushes_before2 > 1);

    // Persisted values reflect BOTH updates (+1 then +10 => +11 total).
    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2 + 11;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM FlushDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
}

// ---------------------------------------------------------------------------
// (3) ATOMICITY: a UNIQUE-constraint violation on a row that scans AFTER an
//     already-flushed batch must revert EVERY mid-flushed append. None visible.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::bounded_dml_flush::error_after_mid_flush_reverts_all") {
    // disk ON: constraint enforcement + revert path exercised on disk.
    auto config = make_test_config("/tmp/test_bounded_dml_flush/atomicity", /*disk_on=*/true);
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.acc (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE FlushDb.acc ADD CONSTRAINT uq_acc_id UNIQUE (id);")->is_success());
    seed_source(dispatcher, "FlushDb.src"); // ids 0..kRowCount-1

    // Pre-seed acc with a SINGLE row whose id collides with a MID-RANGE source row
    // (id = kRowCount/2). The INSERT...SELECT scans ids 0..kRowCount-1: the first
    // batches (ids 0..~1023) flush cleanly BEFORE the colliding id is reached, so
    // the collision surfaces only at the constraint finalize — after >=1 mid-flush.
    const int64_t collide_id = static_cast<int64_t>(kRowCount / 2);
    {
        std::stringstream q;
        q << "INSERT INTO FlushDb.acc (id, grp, val) VALUES (" << collide_id << ", 0, 0);";
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1u);
    }

    // The failing statement: mid-flushes clean batches, then the UNIQUE check
    // rejects the whole statement at finalize.
    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher, "INSERT INTO FlushDb.acc (id, grp, val) SELECT id, grp, val FROM FlushDb.src;");
        REQUIRE(cur->is_error());
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();
    // The mid-flush path RAN before the error surfaced (>=1 clean flush).
    REQUIRE(flushes_after - flushes_before >= 1);

    // ATOMICITY: none of the mid-flushed rows are visible — acc still holds only
    // the single pre-seeded row.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM FlushDb.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1u);
    }
    // And the surviving row is the original seed, unchanged.
    {
        std::stringstream q;
        q << "SELECT val FROM FlushDb.acc WHERE id = " << collide_id << ";";
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 0);
    }
}

// ---------------------------------------------------------------------------
// (4) A PRODUCING sourceless bottom (recursive-CTE fixpoint) feeding the INSERT
//     sink pumps through the pumpable-ancestors branch — the mid-flush gate must
//     fire there exactly as it does on the scan-source pump, or the sink buffers
//     the ENTIRE produced row set and the configured memory bound is a no-op.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::bounded_dml_flush::insert_from_recursive_cte_mid_flushes") {
    auto config = make_test_config("/tmp/test_bounded_dml_flush/recursive_cte_insert");
    config.execution.dml_flush_row_threshold = kFlushThreshold;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kCteRows = 1500; // > kFlushThreshold (512): the gate must trip mid-pump

    REQUIRE(exec(dispatcher, "CREATE DATABASE FlushDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FlushDb.dst (n bigint);")->is_success());

    const auto flushes_before = services::collection::executor::dml_flush_count();
    {
        auto cur = exec(dispatcher,
                        "INSERT INTO FlushDb.dst (n) "
                        "WITH RECURSIVE seq AS ("
                        "  SELECT 1 AS n "
                        "  UNION ALL "
                        "  SELECT n + 1 FROM seq WHERE n < 1500"
                        ") "
                        "SELECT n FROM seq;");
        INFO("INSERT from recursive CTE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kCteRows);
    }
    const auto flushes_after = services::collection::executor::dml_flush_count();

    // The produced rows crossed the threshold mid-pump, so at least one
    // incremental flush ran BEFORE the finalize.
    REQUIRE(flushes_after > flushes_before);

    // And the fixpoint result landed intact.
    {
        auto cur = exec(dispatcher, "SELECT COUNT(n) AS c FROM FlushDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == kCteRows);
    }
}
