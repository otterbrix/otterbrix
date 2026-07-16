// Predicate pushdown INTO a (non-recursive) CTE / FROM-subquery body.
//
// A non-recursive CTE reference is INLINED by the SQL transformer as a table-scan
// aggregate (the CTE body) directly below the consumer aggregate that carries the
// outer WHERE. The optimizer (pushdown_filter) pushes the consumer's pushable WHERE
// conjuncts INTO that body so the filter reaches the base scan: `create_plan_match`
// then lowers a plain compare to a full_scan predicate (Seq Scan) = disk pushdown +
// column pruning, instead of a Filter above a Project above a Seq Scan.
//
// EXPLAIN signal used below:
//   * pushed     -> the pushable conjunct fuses into the scan; there is NO "Filter"
//                   line for it (matches a direct `... FROM t WHERE ...`, which renders
//                   `Project -> Seq Scan on t`).
//   * NOT pushed -> a "Filter" line remains above the body's Project / Limit.
//
// A pushed key keeps its path (body-OUTPUT coordinates) and the predicate reads columns
// by PATH INDEX against the BASE scan, so only a LEADING-PREFIX IDENTITY projection is
// pushable. A reorder (SELECT b,a) or a column whose output ordinal != base index stays
// as residual above the body. Renamed/computed CTE-output columns (SELECT a AS x) are a
// separate PRE-EXISTING resolution limitation (`WHERE x` fails to resolve regardless of
// pushdown) and are intentionally not exercised here.

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <string>
#include <string_view>

using namespace components::cursor;

namespace {

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    bool contains(const std::string& hay, const std::string& needle) {
        return hay.find(needle) != std::string::npos;
    }

    // TestDatabase.big rows: a in {1,2,3,6,7,8}; a>5 -> {6,7,8}; a<3 -> {1,2}.
    // b = a*10, c = a*100 (so c>50 holds for every a>5 row).
    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto s = otterbrix::session_id_t();
            dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.big(a int, b int, c int);")->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s,
                                      "INSERT INTO TestDatabase.big (a, b, c) VALUES "
                                      "(1,10,100),(2,20,200),(3,30,300),(6,60,600),(7,70,700),(8,80,800);")
                        ->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cte_pushdown::pushes_into_body") {
    auto config = test_create_config("/tmp/test_cte_pushdown/pos");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("POSITIVE identity projection: WHERE reaches big's scan (no residual Filter)");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s, "EXPLAIN WITH c AS (SELECT a,b FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(plan->is_success());
        const auto t = plan_text(plan);
        REQUIRE(contains(t, "Seq Scan on big"));
        REQUIRE_FALSE(contains(t, "Filter")); // fused into the scan -> pushed

        auto s2 = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s2, "WITH c AS (SELECT a,b FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("POSITIVE SELECT * body: filter fuses into the scan");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s, "EXPLAIN WITH c AS (SELECT * FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(plan->is_success());
        const auto t = plan_text(plan);
        REQUIRE(contains(t, "Seq Scan on big"));
        REQUIRE_FALSE(contains(t, "Filter"));

        auto s2 = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s2, "WITH c AS (SELECT * FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("POSITIVE body with its own WHERE: outer filter MERGES into the body scan");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s, "EXPLAIN WITH c AS (SELECT a,b FROM TestDatabase.big WHERE b > 0) SELECT * FROM c WHERE a > 5;");
        REQUIRE(plan->is_success());
        const auto t = plan_text(plan);
        REQUIRE(contains(t, "Seq Scan on big"));
        REQUIRE_FALSE(contains(t, "Filter")); // both b>0 and a>5 fused into the scan

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s2, "WITH c AS (SELECT a,b FROM TestDatabase.big WHERE b > 0) SELECT * FROM c WHERE a > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // b>0 keeps all; a>5 -> {6,7,8}
    }

    INFO("POSITIVE partial push: a (prefix-identity) reaches the scan, c (ordinal!=base index) stays residual");
    {
        // Body SELECT a, c: a is output ordinal 0 == base index 0 (pushable);
        // c is output ordinal 1 but base index 2 (NOT prefix-identity) -> stays as a Filter
        // above the Project. Both `c` refs are real base columns (no rename), so both resolve.
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s, "EXPLAIN WITH c AS (SELECT a,c FROM TestDatabase.big) SELECT * FROM c WHERE a > 5 AND c > 50;");
        REQUIRE(plan->is_success());
        const auto t = plan_text(plan);
        REQUIRE(contains(t, "Seq Scan on big"));
        REQUIRE(contains(t, "Filter")); // residual c>50 remains above the body

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s2, "WITH c AS (SELECT a,c FROM TestDatabase.big) SELECT * FROM c WHERE a > 5 AND c > 50;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // a>5 -> {6,7,8}; their c = {600,700,800} all > 50
    }
}

TEST_CASE("integration::cte_pushdown::negatives_stay_correct") {
    auto config = test_create_config("/tmp/test_cte_pushdown/neg");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("NEGATIVE LIMIT: filter must NOT sink below ORDER BY .. LIMIT");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s,
            "EXPLAIN WITH c AS (SELECT a FROM TestDatabase.big ORDER BY a LIMIT 3) SELECT * FROM c WHERE a > 5;");
        REQUIRE(plan->is_success());
        const auto t = plan_text(plan);
        REQUIRE(contains(t, "Filter")); // WHERE stays ABOVE the Limit
        REQUIRE(contains(t, "Limit"));
        REQUIRE(t.find("Filter") < t.find("Limit")); // Filter is above Limit in the tree

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s2, "WITH c AS (SELECT a FROM TestDatabase.big ORDER BY a LIMIT 3) SELECT * FROM c WHERE a > 5;");
        REQUIRE(cur->is_success());
        // ORDER BY a LIMIT 3 -> {1,2,3}; then a>5 -> {} (0). A wrong push-below-LIMIT -> {6,7,8} (3).
        REQUIRE(cur->size() == 0);
    }

    INFO("NEGATIVE reorder projection: SELECT b,a is not prefix-identity -> not pushed, still correct");
    {
        // If the filter were pushed keeping its output-ordinal path, `a`'s ordinal (1) would
        // index base column `b` at the scan -> wrong rows. The prefix-identity guard forbids it.
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(
            s, "EXPLAIN WITH c AS (SELECT b,a FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(plan->is_success());
        REQUIRE(contains(plan_text(plan), "Filter")); // stays above the Project

        auto s2 = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s2, "WITH c AS (SELECT b,a FROM TestDatabase.big) SELECT * FROM c WHERE a > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // a>5 -> {6,7,8}, NOT b>5
    }

    INFO("NEGATIVE multi-reference: each inlined copy filtered independently, no cross-contamination");
    {
        // c is referenced twice with DIFFERENT filters. Each reference is inlined as its own
        // body; pushing a>5 into one and a<3 into the other must not corrupt the other arm.
        auto s2 = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s2,
                                    "WITH c AS (SELECT a FROM TestDatabase.big) "
                                    "SELECT a FROM c WHERE a > 5 "
                                    "UNION ALL "
                                    "SELECT a FROM c WHERE a < 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5); // {6,7,8} + {1,2}
    }
}

TEST_CASE("integration::cte_pushdown::recursive_untouched") {
    auto config = test_create_config("/tmp/test_cte_pushdown/rec");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE RC;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE RC.OrgChart(id int, manager_id int, name string);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "INSERT INTO RC.OrgChart (id, manager_id, name) VALUES "
                                  "(1,0,'ceo'),(2,1,'vp'),(3,2,'dir'),(4,3,'mgr'),(5,4,'ic');")
                    ->is_success());
    }

    INFO("NEGATIVE recursive CTE: outer WHERE must NOT be pushed into the recursive body");
    {
        // Levels by id: ceo(1), vp(2), dir(3), mgr(4), ic(5). Outer WHERE id > 2 -> {3,4,5}.
        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s2,
                                           "WITH RECURSIVE h AS ("
                                           "  SELECT id, manager_id, name FROM RC.OrgChart WHERE manager_id = 0 "
                                           "  UNION ALL "
                                           "  SELECT e.id, e.manager_id, e.name "
                                           "  FROM RC.OrgChart e JOIN h ON e.manager_id = h.id"
                                           ") "
                                           "SELECT id FROM h WHERE id > 2 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3); // {3,4,5}
        REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
        REQUIRE(cur->value(0, 2).value<int64_t>() == 5);
    }
}
