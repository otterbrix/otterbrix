// ---------------------------------------------------------------------------
// Spill verification tests: exercise the grace hash join, two-phase partitioned
// aggregate, and external merge sort spill paths and assert their results match
// the in-memory equivalents. Spill is opt-in per query via
// config.disk.spill_enabled.
// ---------------------------------------------------------------------------

#include <catch2/catch.hpp>
#include <limits>
#include <sstream>
#include "test_config.hpp"

// ============================================================================
// HASH JOIN SPILL TESTS
// ============================================================================

TEST_CASE("integration::cpp::hash_join::spill_large_build_side", "[hash_join][spill]") {
    // Verify grace hash join spill with large build side (100K rows)
    // Build side should be partitioned and spilled to disk during hash table build

    auto config = test_create_config("/tmp/test_hash_join_spill");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE hashjoinspill;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hashjoinspill.left_table (join_key BIGINT, probe_val BIGINT);")->is_success());
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hashjoinspill.right_table (join_key BIGINT, build_val BIGINT);")->is_success());

    // Insert small probe side (1000 rows) — one batched multi-row INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO hashjoinspill.left_table (join_key, probe_val) VALUES ";
        for (int i = 0; i < 1000; ++i) {
            q << "(" << i << ", " << (i * 10) << ")" << (i == 999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    // Insert large build side (100000 rows) — one batched multi-row INSERT (forces spill).
    {
        std::stringstream q;
        q << "INSERT INTO hashjoinspill.right_table (join_key, build_val) VALUES ";
        for (int i = 0; i < 100000; ++i) {
            q << "(" << i << ", " << (i * 100) << ")" << (i == 99999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    // Execute INNER JOIN - should spill right side during hash table build
    auto result = dispatcher->execute_sql(session,
        "SELECT * FROM hashjoinspill.left_table INNER JOIN hashjoinspill.right_table "
        "ON left_table.join_key = right_table.join_key;");

    REQUIRE(result->is_success());
    CHECK(result->size() == 1000);  // Exactly 1000 matching keys (0-999)
}

// A spill I/O failure during the grace hash join build-side spill must surface
// as an ERROR, not a silently-successful empty join. The spill directory is
// forced to a guaranteed-unwritable path: create_directory under /dev/null fails
// with ENOTDIR, so spill_file_t is invalid and partition_and_spill_build_side()
// returns false. The failure mode it guards against: on_execute_impl emitted the
// error only via trace(), built an EMPTY result chunk and returned WITHOUT
// set_error() -> has_error() stayed false -> the executor reported the disk I/O
// failure as a SUCCESSFUL empty join (silent wrong results).
TEST_CASE("integration::cpp::hash_join::spill_io_failure_surfaces_error", "[hash_join][spill]") {
    auto config = test_create_config("/tmp/test_hj_spill_io_fail");
    test_clear_directory(config);
    config.disk.on = true;
    config.disk.spill_enabled = true;
    config.wal.on = false;
    // create_directory under /dev/null fails (ENOTDIR) -> spill_file_t invalid
    // -> partition_and_spill_build_side returns false. /dev/null is a character
    // device on every POSIX platform, so a child path can never be created.
    config.disk.spill_path = "/dev/null/otterbrix_spill_cannot_exist";
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE hjspillio;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hjspillio.l (k BIGINT, lv BIGINT);")->is_success());
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hjspillio.r (k BIGINT, rv BIGINT);")->is_success());

    // A handful of rows each: with spill_enabled=true the optimizer stamps the
    // join exec_strategy=spill, so even this tiny INNER JOIN spills proactively.
    REQUIRE(dispatcher->execute_sql(session,
        "INSERT INTO hjspillio.l (k, lv) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
    REQUIRE(dispatcher->execute_sql(session,
        "INSERT INTO hjspillio.r (k, rv) VALUES (1, 100), (2, 200), (3, 300);")->is_success());

    auto result = dispatcher->execute_sql(session,
        "SELECT * FROM hjspillio.l INNER JOIN hjspillio.r ON l.k = r.k;");

    // The build-side spill cannot write to the unwritable dir, so the join must
    // fail loudly. The bug it guards against: this is_success() returned TRUE
    // (silent empty success).
    REQUIRE_FALSE(result->is_success());
}

// ============================================================================
// AGGREGATE SPILL TESTS
// ============================================================================

TEST_CASE("integration::cpp::aggregate::spill_many_groups", "[aggregate][spill]") {
    // Verify two-phase partitioned aggregate spill with many groups. spill_enabled
    // makes the grace aggregate proactively spill (R6: no runtime threshold); 20K
    // distinct groups exercise grace partitioning + cross-partition merge.
    auto config = test_create_config("/tmp/test_aggregate_spill");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE aggspill;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE aggspill.large_table (group_key BIGINT, value1 BIGINT);")->is_success());

    // Insert 100K rows with 20K distinct groups (5 rows per group) — one batched INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO aggspill.large_table (group_key, value1) VALUES ";
        for (int i = 0; i < 100000; ++i) {
            q << "(" << (i % 20000) << ", " << i << ")" << (i == 99999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    // GROUP BY: the grace aggregate partitions, spills, and merges partitions.
    auto result = dispatcher->execute_sql(session,
        "SELECT group_key, COUNT(*) as cnt, SUM(value1) as total FROM aggspill.large_table GROUP BY group_key;");

    REQUIRE(result->is_success());
    CHECK(result->size() == 20000);  // 20K distinct groups
}

// Commutative aggregate merge correctness. Grace partitioning splits each group
// across partitions; the partition merge must COMBINE commutative aggregates
// (SUM +=, COUNT +=, AVG via sum+count) rather than overwriting the existing
// partial with the new one. With an `existing = std::move(new)` merge, SUM/COUNT
// come out as the value from only ONE partition (wrong).
TEST_CASE("integration::cpp::aggregate::spill_merge_correctness", "[aggregate][spill][b5]") {
    auto config = test_create_config("/tmp/test_aggregate_merge_b5");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    // spill_enabled makes the grace aggregate proactively spill (R6: no runtime
    // threshold); the interleaved rows of each group hash across partition_count
    // partitions, so the partition merge must COMBINE commutative aggregates.
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE aggmergeb5;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE aggmergeb5.t (g BIGINT, v BIGINT);")->is_success());

    // 3 groups (a, b, c). Each group gets many rows so SUM/COUNT/AVG are
    // deterministic and large enough that an overwrite bug visibly corrupts them.
    // Rows are interleaved across groups so a group lands in several partitions.
    int64_t expected_sum_a = 0;
    int64_t expected_sum_b = 0;
    int64_t expected_sum_c = 0;
    const int rows_per_group = 400;
    {
        std::stringstream q;
        q << "INSERT INTO aggmergeb5.t (g, v) VALUES ";
        for (int i = 0; i < rows_per_group; ++i) {
            int64_t va = i;            // 0..399  -> sum = 79800, count = 400, avg = 199.5
            int64_t vb = i + 1000;     // 1000..1399
            int64_t vc = i + 2000;     // 2000..2399
            expected_sum_a += va;
            expected_sum_b += vb;
            expected_sum_c += vc;
            // Keep the interleaved (1,2,3,1,2,3,...) row order so each group's rows
            // are still spread across multiple partitions after grace partitioning.
            q << "(1, " << va << "), (2, " << vb << "), (3, " << vc << ")"
              << (i == rows_per_group - 1 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    auto result = dispatcher->execute_sql(session,
        "SELECT g, COUNT(*) AS cnt, SUM(v) AS total, AVG(v) AS avg_v "
        "FROM aggmergeb5.t GROUP BY g ORDER BY g;");
    REQUIRE(result->is_success());
    REQUIRE(result->size() == 3);

    // Verify each group's commutative aggregates combined correctly across the
    // partitions they were spilled into.
    struct row { int64_t g; int64_t cnt; int64_t sum; double avg; };
    std::vector<row> got;
    for (size_t i = 0; i < result->size(); ++i) {
        got.push_back({
            result->value(0, i).value<int64_t>(),
            result->value(1, i).value<int64_t>(),
            result->value(2, i).value<int64_t>(),
            result->value(3, i).value<double>(),
        });
    }
    REQUIRE(got[0].g == 1);
    REQUIRE(got[0].cnt == rows_per_group);                 // COUNT combined
    REQUIRE(got[0].sum == expected_sum_a);                 // SUM combined
    {
        const double expected_avg_a = static_cast<double>(expected_sum_a) / rows_per_group;
        const double diff_a = got[0].avg > expected_avg_a ? got[0].avg - expected_avg_a : expected_avg_a - got[0].avg;
        REQUIRE(diff_a < 0.001);                            // AVG combined
    }
    REQUIRE(got[1].g == 2);
    REQUIRE(got[1].cnt == rows_per_group);
    REQUIRE(got[1].sum == expected_sum_b);
    REQUIRE(got[2].g == 3);
    REQUIRE(got[2].cnt == rows_per_group);
    REQUIRE(got[2].sum == expected_sum_c);
}

TEST_CASE("integration::cpp::aggregate::spill_count_over_large_table", "[aggregate][spill]") {
    // COUNT(*) over a large table runs through the streaming accumulator without
    // OOM.

    auto config = test_create_config("/tmp/test_count_spill");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE countspill;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE countspill.big_table (id BIGINT, value BIGINT);")->is_success());

    // Insert 100K rows (sufficient for spill verification) — one batched INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO countspill.big_table (id, value) VALUES ";
        for (int i = 0; i < 100000; ++i) {
            q << "(" << i << ", " << (i * 2) << ")" << (i == 99999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    auto result = dispatcher->execute_sql(session, "SELECT COUNT(*) as total FROM countspill.big_table;");
    REQUIRE(result->is_success());
    REQUIRE(result->size() == 1);
    REQUIRE(result->value(0, 0).value<int64_t>() == 100000);
}

// ============================================================================
// LEFT OUTER JOIN SPILL TESTS
// ============================================================================

// A matched left row must be emitted EXACTLY once (as a join row), never twice
// (join row + spurious left-only row). The bug it guards against: the LEFT
// branch discarded its `matched` flag, so left_matched[] stayed 0 for matched
// rows and the deferred left-only pass emitted them again — a result row count
// of (matched*2 + unmatched) instead of (matched + unmatched).
TEST_CASE("integration::cpp::hash_join::spill_left_outer_no_double_emit", "[hash_join][spill]") {
    auto config = test_create_config("/tmp/test_hash_join_left_spill");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE hjleftspill;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hjleftspill.l (k BIGINT, lv BIGINT);")->is_success());
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE hjleftspill.r (k BIGINT, rv BIGINT);")->is_success());

    // Probe side: keys 0..999 (500 will match, 500 won't) — one batched INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO hjleftspill.l (k, lv) VALUES ";
        for (int i = 0; i < 1000; ++i) {
            q << "(" << i << ", " << i << ")" << (i == 999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }
    // Build side: 100000 rows with UNIQUE keys (0,2,4,...,199998). Only the
    // first 500 (0..998 even) match a probe row; the rest are unmatched build
    // rows (irrelevant for LEFT join). A large unique-key build side still
    // selects the grace spill strategy. One batched INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO hjleftspill.r (k, rv) VALUES ";
        for (int i = 0; i < 100000; ++i) {
            q << "(" << (i * 2) << ", " << i << ")" << (i == 99999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    auto result = dispatcher->execute_sql(session,
        "SELECT * FROM hjleftspill.l LEFT JOIN hjleftspill.r ON l.k = r.k ORDER BY l.k;");

    REQUIRE(result->is_success());
    // 1000 probe rows, 1:1 match cardinality (unique build keys): exactly 1000
    // result rows. Double-emit of matched left rows would inflate this.
    REQUIRE(result->size() == 1000);
}

// ============================================================================
// SORT SPILL TESTS
// ============================================================================

TEST_CASE("integration::cpp::sort::spill_large_dataset", "[sort][spill]") {
    // Verify external merge sort with large dataset (100K rows)
    // Sorted runs should be spilled to disk and merged

    // Own a UNIQUE temp dir: test_sort_spill.cpp's [step4] cases use
    // /tmp/test_sort_spill/<subdir>, and this test's test_clear_directory() does
    // remove_all() on its config dir. Sharing the /tmp/test_sort_spill root made
    // the two collide under `ctest -j` (remove_all hit "Directory not empty" and
    // clobbered the concurrent step4 run). Distinct roots = isolated.
    auto config = test_create_config("/tmp/test_spill_red_sort_dataset");
    test_clear_directory(config);
    config.disk.on = true;
    // Opt into the spill plan: the configuration default is spill_enabled=false
    // (in-memory). These tests deliberately exercise the grace/external
    // operators, so they must explicitly enable spilling.
    config.disk.spill_enabled = true;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE sortspill;");
    REQUIRE(dispatcher->execute_sql(session,
        "CREATE TABLE sortspill.big_table (id BIGINT, value BIGINT);")->is_success());

    // Insert 100K rows in random order (sorted by id, but we order by value) — one batched INSERT.
    {
        std::stringstream q;
        q << "INSERT INTO sortspill.big_table (id, value) VALUES ";
        for (int i = 0; i < 100000; ++i) {
            q << "(" << i << ", " << (100000 - i) << ")" << (i == 99999 ? ";" : ", ");
        }
        REQUIRE(dispatcher->execute_sql(session, q.str())->is_success());
    }

    // Execute ORDER BY - should spill during external merge sort
    auto result = dispatcher->execute_sql(session,
        "SELECT * FROM sortspill.big_table ORDER BY value ASC;");

    REQUIRE(result->is_success());
    REQUIRE(result->size() == 100000);

    // Verify sorted order (ascending by value)
    int64_t prev_val = std::numeric_limits<int64_t>::min();
    for (size_t i = 0; i < result->size(); ++i) {
        auto cur_val = result->value(1, i).value<int64_t>();  // value column is index 1
        REQUIRE(cur_val >= prev_val);  // Non-decreasing order
        prev_val = cur_val;
    }
}
