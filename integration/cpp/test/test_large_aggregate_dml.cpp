// RED-first reproducers for oversized-chunk / aliasing bugs past DEFAULT_VECTOR_CAPACITY (1024):
// expected to crash/abort until the fix lands, then serve as the regression guard. Harness mirrors
// test_streaming_dml.cpp; assertions stay on the merged cursor API only, never chunk_data().

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <algorithm>
#include <catch2/catch_test_macros.hpp>
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

    // GROUP BY row order is not guaranteed, so this scans every row for the matching key.
    int64_t group_value_for_key(const cursor_t_ptr& cur, int64_t key) {
        for (uint64_t r = 0; r < cur->size(); ++r) {
            if (cur->value(0, r).value<int64_t>() == key) {
                return cur->value(1, r).value<int64_t>();
            }
        }
        return -1;
    }

    // Chunked so no single statement assembles an unbounded VALUES clause.
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

// 2000 distinct groups make the aggregate result one oversized chunk, tripping the per-group COUNT(*) read.
TEST_CASE("integration::cpp::large_aggregate_dml::group_by_over_1024_distinct_groups") {
    auto config = test_create_config(integration_fixture_path("test_large_aggregate_dml_group_by"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kGroups = 2000; // > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.t (k bigint, v bigint);")->is_success());

    insert_in_batches(dispatcher, "AggDb.t (k, v)", kGroups * 2, 500, [](unsigned i) {
        const unsigned key = i / 2; // 0,0,1,1,2,2,...
        return "(" + std::to_string(key) + ", " + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher, "SELECT k, COUNT(*) AS c FROM AggDb.t GROUP BY k;");
    INFO("GROUP BY error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == static_cast<std::size_t>(kGroups));

    // Probe a group below the 1024 boundary (5) and one above it (1500), where the oversized-chunk read fails.
    REQUIRE(group_value_for_key(cur, 5) == 2);
    REQUIRE(group_value_for_key(cur, 1500) == 2);
}

// 3000 rows in ONE group fold into a single COUNT(DISTINCT) buffer that spans the 1024 boundary.
TEST_CASE("integration::cpp::large_aggregate_dml::count_distinct_over_hot_group_over_1024_rows") {
    auto config = test_create_config(integration_fixture_path("test_large_aggregate_dml_count_distinct"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kRows = 3000; // all one group, > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.t (k bigint, val bigint);")->is_success());

    insert_in_batches(dispatcher, "AggDb.t (k, val)", kRows, 500, [](unsigned i) {
        return "(1, " + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher, "SELECT k, COUNT(DISTINCT val) AS d FROM AggDb.t GROUP BY k;");
    INFO("COUNT(DISTINCT) error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1); // one hot group
    REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    REQUIRE(cur->value(1, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
}

// The USING side (2000 rows) makes the join-driven DELETE's match / row_id buffer span the 1024 boundary.
TEST_CASE("integration::cpp::large_aggregate_dml::delete_using_secondary_table_over_1024_rows") {
    auto config = test_create_config(integration_fixture_path("test_large_aggregate_dml_delete_using"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kUsing = 2000; // > 1024
    REQUIRE(exec(dispatcher, "CREATE DATABASE AggDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.target (k bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AggDb.using_tbl (k bigint);")->is_success());

    insert_in_batches(dispatcher, "AggDb.target (k, v)", kUsing, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ", " + std::to_string(i * 10) + ")";
    });
    // A couple of target rows whose key is NOT in the USING table (must survive).
    REQUIRE(exec(dispatcher, "INSERT INTO AggDb.target (k, v) VALUES (1000000, 1), (1000001, 2);")->is_success());

    insert_in_batches(dispatcher, "AggDb.using_tbl (k)", kUsing, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ")";
    });

    auto cur = exec(dispatcher,
                    "DELETE FROM AggDb.target USING AggDb.using_tbl "
                    "WHERE AggDb.target.k = AggDb.using_tbl.k;");
    INFO("DELETE USING error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == static_cast<std::size_t>(kUsing));

    {
        auto rem = exec(dispatcher, "SELECT COUNT(*) AS c FROM AggDb.target;");
        REQUIRE(rem->is_success());
        REQUIRE(rem->value(0, 0).value<int64_t>() == 2);
    }
    // 1500 is above the 1024 boundary.
    {
        auto gone = exec(dispatcher, "SELECT k FROM AggDb.target WHERE k = 1500;");
        REQUIRE(gone->is_success());
        REQUIRE(gone->size() == 0);
    }
    {
        auto kept = exec(dispatcher, "SELECT k FROM AggDb.target WHERE k = 1000000;");
        REQUIRE(kept->is_success());
        REQUIRE(kept->size() == 1);
    }
}

// Deleting a parent with 2000 FK children makes the CASCADE's matched-children buffer span the 1024 boundary.
TEST_CASE("integration::cpp::large_aggregate_dml::cascade_delete_over_1024_children") {
    auto config = test_create_config(integration_fixture_path("test_large_aggregate_dml_cascade"));
    test_clear_directory(config);
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

    REQUIRE(exec(dispatcher, "INSERT INTO AggDb.parent (id, val) VALUES (1, 'p1'), (2, 'p2');")->is_success());

    insert_in_batches(dispatcher, "AggDb.child (id, parent_id)", kChildren, 500, [](unsigned i) {
        return "(" + std::to_string(i) + ", 1)";
    });
    REQUIRE(exec(dispatcher,
                 "INSERT INTO AggDb.child (id, parent_id) VALUES "
                 "(9000001, 2), (9000002, 2), (9000003, 2);")
                ->is_success());

    auto cur = exec(dispatcher, "DELETE FROM AggDb.parent WHERE id = 1;");
    INFO("cascade delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());

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
