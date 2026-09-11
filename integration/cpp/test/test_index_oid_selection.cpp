// Per-oid index selection: table_oid_dependencies() is an unordered_set, so a shared per-table
// vector overwritten each loop pass picked whichever table enumerated last, letting one table
// borrow or hide another's indexes and mismatch the row count. Only UNION branches and
// scalar-sub-queries reach create_plan_match_'s multi-table selection; JOIN lowers to
// Filter-over-Seq-Scan first.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <string>

using namespace components;
using namespace components::cursor;

namespace {

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    std::size_t count_occurrences(const std::string& hay, const std::string& needle) {
        std::size_t count = 0;
        for (auto pos = hay.find(needle); pos != std::string::npos; pos = hay.find(needle, pos + needle.size())) {
            ++count;
        }
        return count;
    }

    // ta: id 1..20, ka = id (INDEXED), kb = id % 2 (unindexed).
    // tb: id 1..20, ka = id % 2 (unindexed), kb = id (INDEXED).
    constexpr unsigned kRows = 20;

    void create_and_seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE oiddb;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE oiddb.ta (id bigint, ka bigint, kb bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE oiddb.tb (id bigint, ka bigint, kb bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE INDEX idx_ta_ka ON oiddb.ta (ka);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE INDEX idx_tb_kb ON oiddb.tb (kb);")->is_success());

        std::stringstream qa;
        qa << "INSERT INTO oiddb.ta (id, ka, kb) VALUES ";
        for (unsigned i = 1; i <= kRows; ++i) {
            qa << "(" << i << ", " << i << ", " << (i % 2) << ")" << (i == kRows ? ";" : ", ");
        }
        auto ca = exec(dispatcher, qa.str());
        REQUIRE(ca->is_success());
        REQUIRE(ca->size() == kRows);

        std::stringstream qb;
        qb << "INSERT INTO oiddb.tb (id, ka, kb) VALUES ";
        for (unsigned i = 1; i <= kRows; ++i) {
            qb << "(" << i << ", " << (i % 2) << ", " << i << ")" << (i == kRows ? ";" : ", ");
        }
        auto cb = exec(dispatcher, qb.str());
        REQUIRE(cb->is_success());
        REQUIRE(cb->size() == kRows);
    }

} // namespace

// An oid-blind planner could apply another table's index set (or none) to this scan, so the
// unindexed predicate column would silently return zero rows instead of matching.
TEST_CASE("integration::cpp::index_oid_selection::unindexed_predicate_column_returns_rows") {
    auto config = test_create_config(integration_fixture_path("test_index_oid_selection/rows"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    create_and_seed(dispatcher);

    INFO("symmetric UNION ALL: each branch filters on ITS OWN unindexed column "
         "(indexed on the other table), so either enumeration order poisons one "
         "branch. Odd ids match in both branches -> 20 rows total.");
    {
        auto cur = exec(dispatcher,
                        "SELECT id FROM oiddb.ta WHERE kb = 1 "
                        "UNION ALL "
                        "SELECT id FROM oiddb.tb WHERE ka = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }

    INFO("scalar sub-query (acceptance shape): the scan target ta has NO index "
         "on kb; the sub-query's tb carries an index on a same-named column. "
         "The 10 odd-id rows must come back.");
    {
        auto cur = exec(dispatcher,
                        "SELECT id, (SELECT MAX(kb) FROM oiddb.tb) AS m "
                        "FROM oiddb.ta WHERE kb = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows / 2);
    }
}

// Mirror: an oid-blind planner could hide this table's own index behind another table's key set,
// silently degrading its Index Scan to a full scan.
TEST_CASE("integration::cpp::index_oid_selection::each_table_uses_its_own_index") {
    auto config = test_create_config(integration_fixture_path("test_index_oid_selection/explain"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    create_and_seed(dispatcher);

    INFO("symmetric UNION ALL EXPLAIN: each branch filters on ITS OWN indexed "
         "column, so the plan must carry TWO Index Scans regardless of "
         "enumeration order.");
    {
        auto cur = exec(dispatcher,
                        "EXPLAIN SELECT id FROM oiddb.ta WHERE ka = 3 "
                        "UNION ALL "
                        "SELECT id FROM oiddb.tb WHERE kb = 3;");
        REQUIRE(cur->is_success());
        auto text = plan_text(cur);
        INFO("plan:\n" << text);
        REQUIRE(count_occurrences(text, "Index Scan") == 2);
    }

    INFO("scalar sub-query EXPLAIN + result (acceptance mirror): the scan "
         "target ta HAS the index on ka; the sub-query's tb does not — tb's "
         "index-free key set must not hide ta's real index.");
    {
        auto cur = exec(dispatcher,
                        "EXPLAIN SELECT id, (SELECT MAX(ka) FROM oiddb.tb) AS m "
                        "FROM oiddb.ta WHERE ka = 3;");
        REQUIRE(cur->is_success());
        auto text = plan_text(cur);
        INFO("plan:\n" << text);
        REQUIRE(count_occurrences(text, "Index Scan") == 1);

        auto rows = exec(dispatcher,
                         "SELECT id, (SELECT MAX(ka) FROM oiddb.tb) AS m "
                         "FROM oiddb.ta WHERE ka = 3;");
        REQUIRE(rows->is_success());
        REQUIRE(rows->size() == 1);
        REQUIRE(rows->value(0, 0).value<int64_t>() == 3);
    }
}
