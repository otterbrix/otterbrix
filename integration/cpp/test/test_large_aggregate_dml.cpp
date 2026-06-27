// ============================================================================
// RED reproducers for the 4 oversized-chunk / aliasing crashes (rule 18:
// red-first). Each TEST_CASE drives MORE than DEFAULT_VECTOR_CAPACITY (1024)
// rows/groups through an aggregate or a DML-with-secondary-source path, which
// is where the bug trips: a single materialized chunk grows past the 1024-row
// vector capacity (oversized chunk) or a per-batch buffer is reused/aliased
// across the 1024 boundary.
//
// These tests are EXPECTED to crash / abort until the fix lands. That is the
// point: they pin the defect with a deterministic reproducer BEFORE the fix,
// and become the green regression guard after it.
//
//   A) GROUP BY with > 1024 DISTINCT groups.
//   B) Non-vectorizable aggregate (COUNT(DISTINCT)) over ONE hot group > 1024 rows.
//   C) DELETE ... USING a secondary table > 1024 rows.
//   D) ON DELETE CASCADE with > 1024 matched children.
//
// Harness mirrors test_streaming_dml.cpp: test_create_config / test_clear_directory
// / test_spaces / dispatcher->execute_sql(session, sql). Assertions stay on the
// merged cursor API only: cur->is_success() / cur->is_error() / cur->size() /
// cur->value(col, row). NO chunk_data().
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <algorithm>
#include <functional>
#include <sstream>
#include <string>

using namespace components;
using namespace components::cursor;

namespace {
    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    // Find the per-group aggregate value for a given key in a GROUP BY result.
    // GROUP BY output row order is NOT guaranteed, so scan every row for the
    // matching key column (col 0) and return its aggregate column (col 1).
    // Returns -1 if the key is absent (caller asserts on the real value).
    int64_t group_value_for_key(const cursor_t_ptr& cur, int64_t key) {
        for (uint64_t r = 0; r < cur->size(); ++r) {
            if (cur->value(0, r).value<int64_t>() == key) {
                return cur->value(1, r).value<int64_t>();
            }
        }
        return -1;
    }

    // Batch-insert helper: the VALUES list is chunked so no single statement
    // assembles an unbounded VALUES clause. Each batch is its own statement.
    void insert_in_batches(otterbrix::wrapper_dispatcher_t* dispatcher,
                           const std::string& into_clause, // e.g. "Db.t (k, v)"
                           unsigned total,
                           unsigned batch,
                           const std::function<std::string(unsigned)>& row /* "(k, v)" for i */) {
        for (unsigned start = 0; start < total; start += batch) {
            const unsigned end = std::min(start + batch, total);
            std::stringstream q;
            q << "INSERT INTO " << into_clause << " VALUES ";
            for (unsigned i = start; i < end; ++i) {
                q << row(i) << (i + 1 == end ? ";" : ", ");
            }
            auto cur = exec(dispatcher, q.str());
            INFO("batch insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(end - start));
        }
    }
} // namespace

// ----------------------------------------------------------------------------
// A) GROUP BY with > 1024 DISTINCT groups.
//
// 2000 rows, each a DISTINCT group key → 2000 output groups, well past the
// 1024 vector capacity. The aggregate result is one oversized chunk; the
// per-group COUNT(*) read trips the defect.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::large_aggregate_dml::group_by_over_1024_distinct_groups") {
    auto config = test_create_config("/tmp/test_large_aggregate_dml_group_by");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kGroups = 2000; // > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.t (k bigint, v bigint);")->is_success());

    // Two rows per distinct key → COUNT(*) per group == 2, kGroups groups total.
    insert_in_batches(dispatcher, "AggDb.t (k, v)", kGroups * 2, 500, [](unsigned i) {
        const unsigned key = i / 2; // 0,0,1,1,2,2,...
        return "(" + std::to_string(key) + ", " + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher, "SELECT k, COUNT(*) AS c FROM AggDb.t GROUP BY k;");
    INFO("GROUP BY error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    // One output row per distinct key — more than the 1024 vector capacity.
    REQUIRE(cur->size() == static_cast<std::size_t>(kGroups));

    // A couple of per-group counts: every group has exactly 2 rows. Probe a
    // group BELOW the 1024 boundary and one ABOVE it (where the oversized-chunk
    // read fails).
    REQUIRE(group_value_for_key(cur, 5) == 2);
    REQUIRE(group_value_for_key(cur, 1500) == 2);
}

// ----------------------------------------------------------------------------
// B) Non-vectorizable aggregate over ONE hot group > 1024 rows.
//
// 3000 rows, ALL the same group key, COUNT(DISTINCT val). The distinct-set
// folds 3000 inputs into a single group; the non-vectorizable distinct path
// buffers across the 1024 boundary, tripping the aliasing/oversized defect.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::large_aggregate_dml::count_distinct_over_hot_group_over_1024_rows") {
    auto config = test_create_config("/tmp/test_large_aggregate_dml_count_distinct");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kRows = 3000; // all one group, > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.t (k bigint, val bigint);")->is_success());

    // Single hot key (k == 1); val is unique per row → COUNT(DISTINCT val) == kRows.
    insert_in_batches(dispatcher, "AggDb.t (k, val)", kRows, 500, [](unsigned i) {
        return "(1, " + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher, "SELECT k, COUNT(DISTINCT val) AS d FROM AggDb.t GROUP BY k;");
    INFO("COUNT(DISTINCT) error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1); // one hot group
    REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    // All 3000 vals are distinct → the distinct set spans past 1024 rows.
    REQUIRE(cur->value(1, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
}

// ----------------------------------------------------------------------------
// C) DELETE ... USING a secondary table > 1024 rows.
//
// The USING table is seeded with 2000 rows (> 1024). The join-driven DELETE
// scans the whole USING side; a single oversized match chunk (or a reused
// per-batch row_id buffer across the 1024 boundary) trips the defect.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::large_aggregate_dml::delete_using_secondary_table_over_1024_rows") {
    auto config = test_create_config("/tmp/test_large_aggregate_dml_delete_using");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kUsing = 2000; // > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.target (k bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.using_tbl (k bigint);")->is_success());

    // Target: keys 0..(kUsing-1), one row each.
    insert_in_batches(dispatcher, "AggDb.target (k, v)", kUsing, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ", " + std::to_string(i * 10) + ")";
    });
    // Also a couple of target rows whose key is NOT in the USING table (must survive).
    REQUIRE(exec(dispatcher,
                 "INSERT INTO AggDb.target (k, v) VALUES (1000000, 1), (1000001, 2);")
                ->is_success());

    // USING table: every key 0..(kUsing-1) → joins each of the first kUsing target rows.
    insert_in_batches(dispatcher, "AggDb.using_tbl (k)", kUsing, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher,
                    "DELETE FROM AggDb.target USING AggDb.using_tbl "
                    "WHERE AggDb.target.k = AggDb.using_tbl.k;");
    INFO("DELETE USING error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    // Exactly the kUsing matched rows are deleted.
    REQUIRE(cur->size() == static_cast<std::size_t>(kUsing));

    // CORRECTNESS: only the 2 non-matching rows remain.
    {
        auto rem = exec(dispatcher, "SELECT COUNT(*) AS c FROM AggDb.target;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->value(0, 0).value<int64_t>() == 2);
    }
    // A matched key (above the 1024 boundary) is gone.
    {
        auto gone = exec(dispatcher, "SELECT k FROM AggDb.target WHERE k = 1500;");
        REQUIRE(gone->is_success());
        REQUIRE(gone->size() == 0);
    }
    // A non-matched key survives.
    {
        auto kept = exec(dispatcher, "SELECT k FROM AggDb.target WHERE k = 1000000;");
        REQUIRE(kept->is_success());
        REQUIRE(kept->size() == 1);
    }
}

// ----------------------------------------------------------------------------
// D) Cascade DELETE with > 1024 matched children.
//
// One parent with > 1024 FK children (ON DELETE CASCADE). Deleting the parent
// cascades into a single oversized matched-children chunk; the cascade's
// child-row buffer trips the aliasing/oversized defect past the 1024 boundary.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::large_aggregate_dml::cascade_delete_over_1024_children") {
    auto config = test_create_config("/tmp/test_large_aggregate_dml_cascade");
    test_clear_directory(config);
    config.disk.on = true; // FK constraints exercised with disk on, like test_sql_features cascade
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kChildren = 2000; // > 1024 children of the deleted parent
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.parent (id bigint, val text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE AggDb.child ADD CONSTRAINT fk_c "
                 "FOREIGN KEY (parent_id) REFERENCES AggDb.parent (id) ON DELETE CASCADE;")
                ->is_success());

    // Two parents: 1 (the one we delete) and 2 (whose children must survive).
    REQUIRE(exec(dispatcher, "INSERT INTO AggDb.parent (id, val) VALUES (1, 'p1'), (2, 'p2');")->is_success());

    // kChildren children of parent 1 (> 1024), plus a handful of parent-2 children.
    insert_in_batches(dispatcher, "AggDb.child (id, parent_id)", kChildren, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ", 1)";
    });
    REQUIRE(exec(dispatcher,
                 "INSERT INTO AggDb.child (id, parent_id) VALUES "
                 "(9000001, 2), (9000002, 2), (9000003, 2);")
                ->is_success());

    // Delete parent 1: cascade must remove all kChildren matched children.
    auto cur = exec(dispatcher, "DELETE FROM AggDb.parent WHERE id = 1;");
    INFO("cascade delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());

    // CORRECTNESS: parent-1 children are all gone; parent-2 children survive.
    {
        auto gone = exec(dispatcher, "SELECT COUNT(*) AS c FROM AggDb.child WHERE parent_id = 1;");
        REQUIRE(gone->is_success());
        REQUIRE(gone->value(0, 0).value<int64_t>() == 0);
    }
    {
        auto kept = exec(dispatcher, "SELECT COUNT(*) AS c FROM AggDb.child WHERE parent_id = 2;");
        REQUIRE(kept->is_success());
        REQUIRE(kept->value(0, 0).value<int64_t>() == 3);
    }
}
