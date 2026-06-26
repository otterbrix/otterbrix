// ============================================================================
// Streaming nested-loop JOIN / UNION / DISTINCT — the sink-side read path.
//
// operator_join_t (nested-loop, all non-equi join types), operator_union_t and
// operator_distinct_t are SINKS on the push-based read path: the build (right)
// side — and, for union, the left side too — is materialized by a separate
// sub-plan, the probe (left) chain is pumped one batch at a time through push(),
// and the accumulated result is drained at finalize(). A SINGLE per-operator core
// (probe_batch_ + emit_unmatched_build_ for join; emit_union_ for union;
// emit_distinct_ for distinct) serves BOTH that streaming entry and the
// materialized on_execute_impl, so the two paths produce identical output.
//
// WHAT THESE TESTS ASSERT:
//   (a) PATH — a non-equi JOIN over a MULTI-BATCH scan (>> DEFAULT_VECTOR_CAPACITY)
//       routes through execute_pipeline, proven by streaming_pipeline_runs() bumping.
//   (b) CORRECTNESS — inner/left/right/full/cross cardinality + NULL padding, and
//       UNION / UNION ALL / SELECT DISTINCT dedup, all over multi-batch inputs that
//       force chunk boundaries inside the streamed probe.
//
// JOIN conditions use the range form `a >= b AND a <= b` (a compound AND, NOT a
// single equi-comparison) so the optimizer keeps the nested-loop operator_join_t
// rather than substituting the hash-join fast path (covered by test_hash_join.cpp).
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <services/collection/executor.hpp>

#include <sstream>

using namespace components;

namespace {
    cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    // > DEFAULT_VECTOR_CAPACITY (1024) so the probe (left) side spans several
    // batches and the streamed probe_batch_ must carry the join across boundaries.
    constexpr int kN = 1500;
} // namespace

TEST_CASE("integration::cpp::streaming_join::nested_loop_streams_and_is_correct") {
    auto config = test_create_config("/tmp/test_streaming_join/nested");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE SJ;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ.l();")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ.r();")->is_success());

    // left keys [0, kN); right keys [kN/2, kN + kN/2). Overlap = [kN/2, kN) = kN/2
    // keys, each unique on both sides -> kN/2 matched rows.
    {
        std::stringstream l, r;
        l << "INSERT INTO SJ.l (k, lv) VALUES ";
        r << "INSERT INTO SJ.r (k, rv) VALUES ";
        for (int i = 0; i < kN; ++i) {
            l << "(" << i << ", " << i * 10 << ")" << (i == kN - 1 ? ";" : ", ");
            r << "(" << (i + kN / 2) << ", " << i << ")" << (i == kN - 1 ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, l.str())->is_success());
        REQUIRE(exec(dispatcher, r.str())->is_success());
    }

    const std::string on = " ON SJ.l.k >= SJ.r.k AND SJ.l.k <= SJ.r.k";

    // PATH: the multi-batch nested-loop INNER JOIN runs through execute_pipeline.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "SELECT * FROM SJ.l INNER JOIN SJ.r" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(kN / 2)); // overlap [kN/2, kN)
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed through the push-based pipeline

    // LEFT: every left row emitted at least once -> kN (kN/2 matched + kN/2 left-only).
    {
        auto cur = exec(dispatcher, "SELECT * FROM SJ.l LEFT JOIN SJ.r" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(kN));
    }
    // RIGHT: every right row emitted at least once -> kN (kN/2 matched + kN/2 right-only).
    {
        auto cur = exec(dispatcher, "SELECT * FROM SJ.l RIGHT JOIN SJ.r" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(kN));
    }
    // FULL: matched (kN/2) + left-only (kN/2) + right-only (kN/2) = 3*kN/2.
    {
        auto cur = exec(dispatcher, "SELECT * FROM SJ.l FULL JOIN SJ.r" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(3 * (kN / 2)));
    }
}

TEST_CASE("integration::cpp::streaming_join::left_join_null_padding_content") {
    auto config = test_create_config("/tmp/test_streaming_join/leftpad");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE SJ2;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ2.l();")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ2.r();")->is_success());

    // left lv in {10,20,30} keyed on k {1,2,3}; right rv keyed on k {2,3,4}. The
    // non-equi range condition keeps the nested-loop join. k=1 is left-only
    // (NULL-padded right), k=2 and k=3 match. SELECT * -> columns [l.k, l.lv, r.k, r.rv].
    REQUIRE(exec(dispatcher, "INSERT INTO SJ2.l (k, lv) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO SJ2.r (k, rv) VALUES (2, 200), (3, 300), (4, 400);")->is_success());

    auto cur = exec(dispatcher,
                    "SELECT * FROM SJ2.l LEFT JOIN SJ2.r "
                    "ON SJ2.l.k >= SJ2.r.k AND SJ2.l.k <= SJ2.r.k ORDER BY lv ASC;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3); // k=1 left-only, k=2, k=3 matched
    // Ordered by lv: row0 lv=10 (k=1, left-only -> right NULL); row1 lv=20 (k=2 -> rv=200);
    // row2 lv=30 (k=3 -> rv=300). Right columns are positions 2 (r.k) and 3 (r.rv).
    REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    REQUIRE(cur->value(3, 0).is_null());
    REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
    REQUIRE(cur->value(3, 1).value<int64_t>() == 200);
    REQUIRE(cur->value(0, 2).value<int64_t>() == 3);
    REQUIRE(cur->value(3, 2).value<int64_t>() == 300);
}

TEST_CASE("integration::cpp::streaming_join::cross_join_materialized") {
    auto config = test_create_config("/tmp/test_streaming_join/cross");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE SJ3;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ3.a();")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ3.b();")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO SJ3.a (x) VALUES (1), (2), (3);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO SJ3.b (y) VALUES (10), (20);")->is_success());

    // Cross join: 3 * 2 = 6 rows.
    auto cur = exec(dispatcher, "SELECT * FROM SJ3.a CROSS JOIN SJ3.b;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 6);
}

TEST_CASE("integration::cpp::streaming_join::cross_join_streams_multibatch") {
    auto config = test_create_config("/tmp/test_streaming_join/cross_stream");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE SJ5;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ5.a();")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ5.b();")->is_success());

    // Multi-batch probe (left) side (> DEFAULT_VECTOR_CAPACITY) forces the cross
    // product across chunk boundaries through the streamed probe path. Small right
    // (build) side keeps the cartesian product bounded: kN * 2 rows.
    {
        std::stringstream l;
        l << "INSERT INTO SJ5.a (x) VALUES ";
        for (int i = 0; i < kN; ++i) {
            l << "(" << i << ")" << (i == kN - 1 ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, l.str())->is_success());
    }
    REQUIRE(exec(dispatcher, "INSERT INTO SJ5.b (y) VALUES (10), (20);")->is_success());

    // PATH: the multi-batch CROSS JOIN now routes through execute_pipeline (cross is
    // a sink, like every other join type), proven by streaming_pipeline_runs bumping.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    auto cur = exec(dispatcher, "SELECT * FROM SJ5.a CROSS JOIN SJ5.b;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == static_cast<size_t>(kN * 2)); // full cartesian product
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed through the push-based pipeline
}

TEST_CASE("integration::cpp::streaming_join::no_from_constants_stream") {
    auto config = test_create_config("/tmp/test_streaming_join/nofrom");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // No-FROM constants-only SELECT: the INVALID_OID sentinel scan is now a SOURCE
    // emitting one synthetic placeholder row, so `select -> scan` routes through
    // execute_pipeline and yields exactly the single constants row.
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "SELECT 2 + 3 AS five, 10 * 5 AS fifty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 50);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before); // streamed (no longer the legacy materialize path)

    // A bare literal projection still yields one row.
    {
        auto cur = exec(dispatcher, "SELECT 1 AS one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::streaming_join::union_distinct_sinks") {
    auto config = test_create_config("/tmp/test_streaming_join/uniondistinct");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE SJ4;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE SJ4.t();")->is_success());

    // Multi-batch seed: id [0, kN); grp = id % 4 (a handful of distinct values).
    {
        std::stringstream q;
        q << "INSERT INTO SJ4.t (id, grp) VALUES ";
        for (int i = 0; i < kN; ++i) {
            q << "(" << i << ", " << (i % 4) << ")" << (i == kN - 1 ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }

    // SELECT DISTINCT over a multi-batch input collapses grp to its 4 distinct values.
    {
        auto cur = exec(dispatcher, "SELECT DISTINCT grp FROM SJ4.t ORDER BY grp ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        for (size_t row = 0; row < 4; ++row) {
            REQUIRE(cur->value(0, row).value<int64_t>() == static_cast<int64_t>(row));
        }
    }

    // UNION ALL of two halves preserves every row: kN/2 + kN/2 = kN.
    {
        std::stringstream q;
        q << "SELECT id FROM SJ4.t WHERE id < " << (kN / 2) << " UNION ALL "
          << "SELECT id FROM SJ4.t WHERE id >= " << (kN / 2) << ";";
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<size_t>(kN));
    }

    // UNION (distinct) of two fully-overlapping selects dedups back to one copy.
    {
        std::stringstream q;
        q << "SELECT id FROM SJ4.t WHERE id < 100 UNION "
          << "SELECT id FROM SJ4.t WHERE id < 100;";
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
    }

    // UNION (distinct) of overlapping ranges: [0,100) ∪ [50,150) = [0,150) = 150.
    {
        std::stringstream q;
        q << "SELECT id FROM SJ4.t WHERE id < 100 UNION "
          << "SELECT id FROM SJ4.t WHERE id >= 50 AND id < 150;";
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 150);
    }
}
