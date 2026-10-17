#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <vector>

// TODO: edge case with connecting query and it's subquery
/*
SELECT ... FROM A WHERE EXISTS ( SELECT ... FROM B WHERE A.id = B.id););
This requires replanning into a join
*/

namespace {

    void setup_subquery_db(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.Departments "
                                               "(id bigint, name string, budget bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.Departments (id, name, budget) VALUES "
                                               "(1, 'Engineering', 100000), "
                                               "(2, 'Marketing',    50000), "
                                               "(3, 'HR',           30000), "
                                               "(4, 'Sales',        80000), "
                                               "(5, 'Finance',      70000);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 5);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.Employees "
                                               "(id bigint, name string, dept_id bigint, salary bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.Employees (id, name, dept_id, salary) VALUES "
                                               "(1,  'Alice',   1, 90000), "
                                               "(2,  'Bob',     1, 80000), "
                                               "(3,  'Charlie', 2, 60000), "
                                               "(4,  'Diana',   2, 55000), "
                                               "(5,  'Eve',     3, 45000), "
                                               "(6,  'Frank',   3, 40000), "
                                               "(7,  'Grace',   4, 70000), "
                                               "(8,  'Henry',   4, 65000), "
                                               "(9,  'Iris',    5, 75000), "
                                               "(10, 'Jack',    5, 72000);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 10);
        }
    }

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto cell0 = cur->get<std::string_view>(0, r);
            REQUIRE_FALSE(cell0.has_error());
            out += std::string(cell0.value());
            out += '\n';
        }
        return out;
    }

    bool contains(const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; }

} // namespace

TEST_CASE("integration::cpp::test_subqueries::where_clause") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/where_clause"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("scalar subquery in WHERE with equality");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary = (SELECT MAX(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell1 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == "Alice");
    }

    INFO("scalar subquery in WHERE with greater-than");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("scalar subquery in WHERE with less-than against aggregated outer");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary < ("
                                           "  SELECT MIN(budget) FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("IN subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
    }

    INFO("NOT IN subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id NOT IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("IN empty subquery matches nothing (PostgreSQL: IN () -> 0 rows, not an error)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT count(*) AS c FROM TestDatabase.Employees "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE 1 = 0"
                                           ");");
        REQUIRE(cur->is_success());
        auto cell2 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value() == 0);
    }

    INFO("NOT IN empty subquery matches everything (PostgreSQL: NOT IN () -> all rows)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT count(*) AS c FROM TestDatabase.Employees "
                                           "WHERE dept_id NOT IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE 1 = 0"
                                           ");");
        REQUIRE(cur->is_success());
        auto cell3 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == 10);
    }

    INFO("EXISTS non-correlated subquery — rows found");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE EXISTS (SELECT 1 FROM TestDatabase.Employees WHERE salary > 85000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("EXISTS non-correlated subquery — no rows found");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT name FROM TestDatabase.Departments "
                                    "WHERE EXISTS (SELECT 1 FROM TestDatabase.Employees WHERE salary > 999999);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("NOT EXISTS non-correlated subquery — subquery empty");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT name FROM TestDatabase.Departments "
                                    "WHERE NOT EXISTS (SELECT 1 FROM TestDatabase.Employees WHERE salary > 999999);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    // TODO: those ones have to be replanned in 'planner' into a join
    /*
    INFO("EXISTS correlated subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name FROM TestDatabase.Departments d "
            "WHERE EXISTS ("
            "  SELECT 1 FROM TestDatabase.Employees e "
            "  WHERE e.dept_id = d.id AND e.salary > 85000"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto sub1 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(sub1.has_error());
        REQUIRE(sub1.value() == "Engineering");
    }

    INFO("NOT EXISTS correlated subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name FROM TestDatabase.Departments d "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM TestDatabase.Employees e "
            "  WHERE e.dept_id = d.id AND e.salary > 50000"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto sub2 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(sub2.has_error());
        REQUIRE(sub2.value() == "HR");
    }

    INFO("correlated subquery comparing to own-department average");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e1.name FROM TestDatabase.Employees e1 "
            "WHERE e1.salary > ("
            "  SELECT AVG(e2.salary) FROM TestDatabase.Employees e2 "
            "  WHERE e2.dept_id = e1.dept_id"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
    */

    INFO("ANY subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE budget > ANY (SELECT salary FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("ALL subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE budget > ALL (SELECT salary FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell4 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == "Engineering");
    }

    INFO("scalar subquery returning NULL (empty result)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary = ("
                                           "  SELECT MAX(salary) FROM TestDatabase.Employees WHERE dept_id = 999"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// Both correlated EXISTS and NOT EXISTS lower to a LATERAL join binding the correlation per outer row.

TEST_CASE("integration::cpp::test_subqueries::correlated_exists_semi_anti") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/correlated_exists_semi_anti"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("correlated EXISTS -> semi-join: departments with a >85000 earner");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id AND e.salary > 85000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell5 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == "Engineering");
    }

    INFO("correlated NOT EXISTS -> anti-join: departments with no >50000 earner");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE NOT EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id AND e.salary > 50000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell6 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == "HR");
    }

    INFO("correlated EXISTS -> semi-join: every dept has employees (all rows)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("correlated NOT EXISTS -> anti-join: inner never matches (all rows)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE NOT EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id AND e.salary > 999999"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("correlated EXISTS -> anti-join: inner always matches (no rows)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE NOT EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id AND e.salary > 30000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("EXPLAIN: correlated EXISTS lowers to a join (Nested Loop), not a Filter");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "EXPLAIN SELECT d.name FROM TestDatabase.Departments d "
                                           "WHERE EXISTS ("
                                           "  SELECT 1 FROM TestDatabase.Employees e "
                                           "  WHERE e.dept_id = d.id AND e.salary > 85000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "Nested Loop"));
    }
}

// NOTE: NOT IN is deliberately not routed to an anti-join — a plain anti-join can't reproduce SQL
// three-valued logic (a NULL in the subquery makes `x NOT IN (S)` never TRUE), so it keeps the membership-test path.

TEST_CASE("integration::cpp::test_subqueries::select_list_and_from") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/select_list_and_from"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    // TODO: those have to be replanned into join
    /*
    INFO("scalar correlated subquery in SELECT list");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e.name, "
            "  (SELECT d.name FROM TestDatabase.Departments d WHERE d.id = e.dept_id) AS dept_name "
            "FROM TestDatabase.Employees e "
            "WHERE e.dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto sub3 = cur->get<std::string_view>(1, 0);
        REQUIRE_FALSE(sub3.has_error());
        REQUIRE(sub3.value() == "Engineering");
        auto sub4 = cur->get<std::string_view>(1, 1);
        REQUIRE_FALSE(sub4.has_error());
        REQUIRE(sub4.value() == "Engineering");
    }

    INFO("aggregate correlated subquery in SELECT list");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name, "
            "  (SELECT COUNT(*) FROM TestDatabase.Employees e WHERE e.dept_id = d.id) AS headcount "
            "FROM TestDatabase.Departments d "
            "ORDER BY d.id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t row = 0; row < 5; ++row) {
            auto sub5 = cur->get<int64_t>(1, row);
            REQUIRE_FALSE(sub5.has_error());
            REQUIRE(sub5.value() == 2);
        }
    }

    INFO("subquery in SELECT list returning maximum of outer group");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e.name, "
            "  (SELECT MAX(e2.salary) FROM TestDatabase.Employees e2 WHERE e2.dept_id = e.dept_id) AS dept_max "
            "FROM TestDatabase.Employees e "
            "WHERE e.dept_id = 1 "
            "ORDER BY e.salary DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto sub6 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(sub6.has_error());
        REQUIRE(sub6.value() == 90000);
        auto sub7 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(sub7.has_error());
        REQUIRE(sub7.value() == 90000);
    }
    */

    INFO("derived table in FROM (basic)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT name FROM "
            "  (SELECT name, salary FROM TestDatabase.Employees WHERE salary > 70000) AS high_earners;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("derived table in FROM with outer WHERE");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT name, salary FROM "
            "  (SELECT name, salary, dept_id FROM TestDatabase.Employees WHERE salary > 60000) AS mid_up "
            "WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("double-nested derived tables in FROM");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM ("
                                           "  SELECT name, salary FROM ("
                                           "    SELECT name, salary FROM TestDatabase.Employees WHERE salary > 60000"
                                           "  ) AS above_60k "
                                           "  WHERE salary < 80000"
                                           ") AS mid_earners;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("derived table aggregated in FROM");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT d.name, stats.avg_sal "
                                           "FROM TestDatabase.Departments d "
                                           "JOIN ("
                                           "  SELECT dept_id, AVG(salary) AS avg_sal "
                                           "  FROM TestDatabase.Employees GROUP BY dept_id"
                                           ") AS stats ON d.id = stats.dept_id "
                                           "WHERE stats.avg_sal > 70000 "
                                           "ORDER BY d.id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_subqueries::join") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/join"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("subquery as right side of JOIN, filter above department average");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT e.name "
                                           "FROM TestDatabase.Employees e "
                                           "JOIN ("
                                           "  SELECT dept_id, AVG(salary) AS avg_sal "
                                           "  FROM TestDatabase.Employees GROUP BY dept_id"
                                           ") AS dept_avg ON e.dept_id = dept_avg.dept_id "
                                           "WHERE e.salary > dept_avg.avg_sal;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("subquery in JOIN ON clause — top earner per department");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT e.name "
                                    "FROM TestDatabase.Employees e "
                                    "JOIN ("
                                    "  SELECT dept_id, MAX(salary) AS max_sal "
                                    "  FROM TestDatabase.Employees GROUP BY dept_id"
                                    ") AS dept_max ON e.dept_id = dept_max.dept_id AND e.salary = dept_max.max_sal;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("subquery in JOIN producing multi-column derived table");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT e.name "
                                    "FROM TestDatabase.Employees e "
                                    "JOIN ("
                                    "  SELECT dept_id, AVG(salary) AS avg_sal, "
                                    "         MAX(salary) AS max_sal, MIN(salary) AS min_sal "
                                    "  FROM TestDatabase.Employees GROUP BY dept_id"
                                    ") AS stats ON e.dept_id = stats.dept_id "
                                    "WHERE e.salary >= stats.avg_sal - 5000 AND e.salary <= stats.avg_sal + 5000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }
}

TEST_CASE("integration::cpp::test_subqueries::having") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/having"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("subquery in HAVING comparing to overall average");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, AVG(salary) AS avg_sal "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING AVG(salary) > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("subquery in HAVING with MIN");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING MIN(salary) > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("subquery in HAVING comparing to specific department budget");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, SUM(salary) AS total_payroll "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING SUM(salary) > ("
                                           "  SELECT budget FROM TestDatabase.Departments WHERE name = 'HR'"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_subqueries::nested") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/nested"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("3-level nested: scalar in scalar in IN");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary > ("
                                           "  SELECT AVG(salary) FROM TestDatabase.Employees "
                                           "  WHERE dept_id IN ("
                                           "    SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           "  )"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // TODO: those have to be replanned in 'planer'
    /*
    INFO("3-level nested: EXISTS inside IN inside scalar");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT name FROM TestDatabase.Employees "
            "WHERE salary = ("
            "  SELECT MAX(salary) FROM TestDatabase.Employees "
            "  WHERE dept_id IN ("
            "    SELECT id FROM TestDatabase.Departments d "
            "    WHERE EXISTS ("
            "      SELECT 1 FROM TestDatabase.Employees e "
            "      WHERE e.dept_id = d.id AND e.salary < ("
            "        SELECT AVG(salary) FROM TestDatabase.Employees"
            "      )"
            "    )"
            "  )"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto sub8 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(sub8.has_error());
        REQUIRE(sub8.value() == "Grace");
    }

    INFO("4-level nested: top earner in each of the best departments");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e.name FROM TestDatabase.Employees e "
            "WHERE e.dept_id IN ("
            "  SELECT id FROM TestDatabase.Departments WHERE budget > ("
            "    SELECT AVG(budget) FROM TestDatabase.Departments"
            "  )"
            ") "
            "AND e.salary = ("
            "  SELECT MAX(e2.salary) FROM TestDatabase.Employees e2 WHERE e2.dept_id = e.dept_id"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    */

    INFO("4-level nested: IN in IN in scalar in scalar");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments "
                                           "  WHERE budget > ("
                                           "    SELECT AVG(budget) FROM TestDatabase.Departments "
                                           "    WHERE id IN ("
                                           "      SELECT dept_id FROM TestDatabase.Employees "
                                           "      WHERE salary > ("
                                           "        SELECT AVG(salary) FROM TestDatabase.Employees"
                                           "      )"
                                           "    )"
                                           "  )"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("5-level nested: scalar chain through both tables");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id = ("
                                           "  SELECT id FROM TestDatabase.Departments "
                                           "  WHERE budget = ("
                                           "    SELECT MAX(budget) FROM TestDatabase.Departments "
                                           "    WHERE budget > ("
                                           "      SELECT AVG(budget) FROM TestDatabase.Departments "
                                           "      WHERE id IN ("
                                           "        SELECT dept_id FROM TestDatabase.Employees "
                                           "        WHERE salary > ("
                                           "          SELECT AVG(salary) FROM TestDatabase.Employees"
                                           "        )"
                                           "      )"
                                           "    )"
                                           "  )"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("5-level nested: second-highest budget department via subquery chain");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary > ("
                                           "  SELECT AVG(salary) FROM TestDatabase.Employees "
                                           "  WHERE dept_id = ("
                                           "    SELECT id FROM TestDatabase.Departments "
                                           "    WHERE budget = ("
                                           "      SELECT MAX(budget) FROM TestDatabase.Departments "
                                           "      WHERE budget < ("
                                           "        SELECT MAX(budget) FROM TestDatabase.Departments "
                                           "        WHERE id IN ("
                                           "          SELECT dept_id FROM TestDatabase.Employees"
                                           "        )"
                                           "      )"
                                           "    )"
                                           "  )"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_subqueries::dml") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/dml"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("INSERT SELECT — copy high earners to new table");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TopEarners (name string, salary bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TopEarners (name, salary) "
                                               "SELECT name, salary FROM TestDatabase.Employees WHERE salary > 70000;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT name FROM TestDatabase.TopEarners;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 4);
        }
    }

    INFO("INSERT SELECT with ORDER BY");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.RankedEarners (name string, salary bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.RankedEarners (name, salary) "
                "SELECT name, salary FROM TestDatabase.Employees ORDER BY salary DESC LIMIT 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 3);
        }
    }

    INFO("DELETE WHERE IN subquery");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Employees "
                                               "WHERE dept_id IN ("
                                               "  SELECT id FROM TestDatabase.Departments WHERE budget = ("
                                               "    SELECT MIN(budget) FROM TestDatabase.Departments"
                                               "  )"
                                               ");");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*) AS cnt FROM TestDatabase.Employees;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell7 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell7.has_error());
            REQUIRE(cell7.value() == 8);
        }
    }

    INFO("UPDATE WHERE scalar subquery");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Employees SET salary = 65200 "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget = 50000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->affected_rows() == 2);
    }

    INFO("DELETE WHERE NOT IN subquery");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Employees "
                                               "WHERE dept_id NOT IN ("
                                               "  SELECT id FROM TestDatabase.Departments WHERE budget = ("
                                               "    SELECT MAX(budget) FROM TestDatabase.Departments"
                                               "  )"
                                               ");");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 6);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*) AS cnt FROM TestDatabase.Employees;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell8 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell8.has_error());
            REQUIRE(cell8.value() == 2);
        }
    }
}

TEST_CASE("integration::cpp::test_subqueries::cte") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/cte"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("simple CTE used in SELECT");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH above_avg AS ("
                                           "  SELECT name, salary, dept_id "
                                           "  FROM TestDatabase.Employees "
                                           "  WHERE salary > (SELECT AVG(salary) FROM TestDatabase.Employees)"
                                           ") "
                                           "SELECT name FROM above_avg ORDER BY salary DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        auto cell9 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell9.has_error());
        REQUIRE(cell9.value() == "Alice");
    }

    INFO("CTE joined with base table");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH dept_stats AS ("
                                           "  SELECT dept_id, AVG(salary) AS avg_sal, MAX(salary) AS max_sal "
                                           "  FROM TestDatabase.Employees "
                                           "  GROUP BY dept_id"
                                           ") "
                                           "SELECT d.name, ds.avg_sal "
                                           "FROM TestDatabase.Departments d "
                                           "JOIN dept_stats ds ON d.id = ds.dept_id "
                                           "ORDER BY ds.avg_sal DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        auto cell10 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell10.has_error());
        REQUIRE(cell10.value() == "Engineering");
    }

    INFO("multiple CTEs chained");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH above_avg AS ("
                                           "  SELECT name, dept_id, salary "
                                           "  FROM TestDatabase.Employees "
                                           "  WHERE salary > (SELECT AVG(salary) FROM TestDatabase.Employees)"
                                           "), "
                                           "high_budget_depts AS ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           ") "
                                           "SELECT a.name "
                                           "FROM above_avg a "
                                           "WHERE a.dept_id IN (SELECT id FROM high_budget_depts) "
                                           "ORDER BY a.salary DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("CTE used twice in the same query");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH dept_tops AS ("
                                           "  SELECT dept_id, MAX(salary) AS top_sal "
                                           "  FROM TestDatabase.Employees GROUP BY dept_id"
                                           ") "
                                           "SELECT d.name "
                                           "FROM TestDatabase.Departments d "
                                           "JOIN dept_tops dt ON d.id = dt.dept_id "
                                           "WHERE dt.top_sal = (SELECT MAX(top_sal) FROM dept_tops);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell11 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == "Engineering");
    }

    INFO("CTE with subquery in its own WHERE clause");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH eligible AS ("
                                           "  SELECT dept_id, salary "
                                           "  FROM TestDatabase.Employees "
                                           "  WHERE salary > (SELECT MIN(budget) FROM TestDatabase.Departments)"
                                           ") "
                                           "SELECT dept_id, COUNT(*) AS cnt "
                                           "FROM eligible "
                                           "GROUP BY dept_id "
                                           "ORDER BY dept_id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t row = 0; row < 5; ++row) {
            auto cell12 = cur->get<int64_t>(1, row);
            REQUIRE_FALSE(cell12.has_error());
            REQUIRE(cell12.value() == 2);
        }
    }
}

TEST_CASE("integration::cpp::test_subqueries::union") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/union"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("UNION ALL preserves duplicates");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        for (size_t row = 0; row < 4; ++row) {
            auto cell13 = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell13.has_error());
            REQUIRE(cell13.value() == 1);
        }
    }

    INFO("UNION ALL disjoint sets");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("UNION distinct removes duplicates");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE salary >= 80000 "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE salary >= 70000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("UNION distinct same values on both sides");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell14 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell14.has_error());
        REQUIRE(cell14.value() == 1);
    }

    INFO("UNION ALL three operands");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "UNION ALL "
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
    }

    INFO("UNION schema mismatch rejected");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id, salary FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE_FALSE(cur->is_success());
    }
}

// F1: operator_limit applies the MERGED window once, not per UNION arm or GROUP BY scan.
TEST_CASE("integration::cpp::test_subqueries::union_group_limit_offset") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/union_group_limit_offset"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("UNION ALL OFFSET (no ORDER BY) skips the head of the MERGED stream, not each arm");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "OFFSET 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell15 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell15.has_error());
        REQUIRE(cell15.value() == 2);
        auto cell16 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell16.has_error());
        REQUIRE(cell16.value() == 2);
    }

    INFO("UNION ALL LIMIT+OFFSET (no ORDER BY) windows the concatenation once");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.Employees "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.Employees "
                                           "LIMIT 3 OFFSET 4;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("UNION distinct OFFSET (no ORDER BY) offsets the deduped result");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees "
                                           "OFFSET 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("GROUP BY LIMIT+OFFSET (no ORDER BY) windows the GROUPS, not the scan");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, count(*) AS c FROM TestDatabase.Employees "
                                           "GROUP BY dept_id LIMIT 3 OFFSET 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        for (size_t row = 0; row < cur->size(); ++row) {
            auto cell17 = cur->get<uint64_t>(1, row);
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 2);
        }
    }

    INFO("GROUP BY LIMIT+OFFSET returns whole groups with correct counts");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, count(*) AS c FROM TestDatabase.Employees "
                                           "GROUP BY dept_id LIMIT 2 OFFSET 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        for (size_t row = 0; row < cur->size(); ++row) {
            auto cell18 = cur->get<uint64_t>(1, row);
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 2);
        }
    }

    INFO("UNION with ORDER BY LIMIT OFFSET still applies the window post-sort (no regression)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees "
                                           "ORDER BY dept_id LIMIT 2 OFFSET 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell19 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell19.has_error());
        REQUIRE(cell19.value() == 2);
        auto cell20 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell20.has_error());
        REQUIRE(cell20.value() == 3);
    }
}

TEST_CASE("integration::cpp::test_subqueries::union_complex_types") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/union_complex_types"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE point_t AS (x int, y int);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "CREATE TABLE TestDatabase.ShapeA "
                                      "(id bigint, pt point_t, tags bigint[3]);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "CREATE TABLE TestDatabase.ShapeB "
                                      "(id bigint, pt point_t, tags bigint[3]);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.ShapeA (id, pt, tags) VALUES "
                                               "(1, ROW(0, 0), ARRAY[1,2,3]), "
                                               "(2, ROW(1, 1), ARRAY[4,5,6]), "
                                               "(3, ROW(2, 2), ARRAY[7,8,9]);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.ShapeB (id, pt, tags) VALUES "
                                               "(1, ROW(0, 0), ARRAY[1,2,3]), "
                                               "(4, ROW(3, 3), ARRAY[10,11,12]);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 2);
        }
    }

    INFO("UNION ALL id column");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("UNION ALL with UDT and array columns");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id, pt, tags FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id, pt, tags FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("UNION distinct id column removes duplicates");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION "
                                           "SELECT id FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("UNION schema mismatch rejected");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id, pt FROM TestDatabase.ShapeB;");
        REQUIRE_FALSE(cur->is_success());
    }
}

namespace {
    void setup_recursive_db(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.OrgChart (id bigint, name string, manager_id bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.OrgChart (id, name, manager_id) VALUES "
                                               "(1, 'CEO',      0), "
                                               "(2, 'VP Eng',   1), "
                                               "(3, 'VP Mkt',   1), "
                                               "(4, 'Engineer', 2), "
                                               "(5, 'Designer', 3);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 5);
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_subqueries::recursive_cte") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/recursive_cte"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_recursive_db(dispatcher); }

    INFO("full hierarchy traversal");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH RECURSIVE hierarchy AS ("
                                           "  SELECT id, name FROM TestDatabase.OrgChart WHERE manager_id = 0 "
                                           "  UNION ALL "
                                           "  SELECT e.id, e.name "
                                           "  FROM TestDatabase.OrgChart e "
                                           "  JOIN hierarchy h ON e.manager_id = h.id"
                                           ") "
                                           "SELECT name FROM hierarchy ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        auto cell21 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell21.has_error());
        REQUIRE(cell21.value() == "CEO");
        auto cell22 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell22.has_error());
        REQUIRE(cell22.value() == "VP Eng");
        auto cell23 = cur->get<std::string_view>(0, 4);
        REQUIRE_FALSE(cell23.has_error());
        REQUIRE(cell23.value() == "Designer");
    }

    INFO("subtree rooted at VP Eng");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "WITH RECURSIVE subtree AS ("
                                           "  SELECT id, name FROM TestDatabase.OrgChart WHERE id = 2 "
                                           "  UNION ALL "
                                           "  SELECT e.id, e.name "
                                           "  FROM TestDatabase.OrgChart e "
                                           "  JOIN subtree s ON e.manager_id = s.id"
                                           ") "
                                           "SELECT name FROM subtree ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell24 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell24.has_error());
        REQUIRE(cell24.value() == "VP Eng");
        auto cell25 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell25.has_error());
        REQUIRE(cell25.value() == "Engineer");
    }

    INFO("hierarchy with depth");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "WITH RECURSIVE hierarchy AS ("
                                    "  SELECT id, name, 0 AS depth FROM TestDatabase.OrgChart WHERE manager_id = 0 "
                                    "  UNION ALL "
                                    "  SELECT e.id, e.name, h.depth + 1 "
                                    "  FROM TestDatabase.OrgChart e "
                                    "  JOIN hierarchy h ON e.manager_id = h.id"
                                    ") "
                                    "SELECT name, depth FROM hierarchy ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        auto cell26 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell26.has_error());
        REQUIRE(cell26.value() == 0);
        auto cell27 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(cell27.has_error());
        REQUIRE(cell27.value() == 1);
        auto cell28 = cur->get<int64_t>(1, 3);
        REQUIRE_FALSE(cell28.has_error());
        REQUIRE(cell28.value() == 2);
    }

    INFO("filter by depth in outer query");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "WITH RECURSIVE hierarchy AS ("
                                    "  SELECT id, name, 0 AS depth FROM TestDatabase.OrgChart WHERE manager_id = 0 "
                                    "  UNION ALL "
                                    "  SELECT e.id, e.name, h.depth + 1 "
                                    "  FROM TestDatabase.OrgChart e "
                                    "  JOIN hierarchy h ON e.manager_id = h.id"
                                    ") "
                                    "SELECT name FROM hierarchy WHERE depth = 2 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell29 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell29.has_error());
        REQUIRE(cell29.value() == "Engineer");
        auto cell30 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell30.has_error());
        REQUIRE(cell30.value() == "Designer");
    }
}

// Tier-0: every unsupported SubLink form must error cleanly, not crash via assert(false) (Release UB)
// or a null-deref under AND/OR/NOT; EXPR bare boolean predicates are supported.
TEST_CASE("integration::cpp::test_subqueries::tier0_unsupported_sublink_forms") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/tier0_unsupported_sublink_forms"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.flags (id bigint, ok boolean);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.flags (id, ok) VALUES (1, true), (2, false);")
                    ->is_success());
    }

    INFO("EXPR bare boolean predicate WHERE (SELECT flag) is supported (was assert(false))");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT name FROM TestDatabase.Employees WHERE (SELECT ok FROM TestDatabase.flags WHERE id = 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("EXPR bare boolean predicate that is false selects no rows");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT name FROM TestDatabase.Employees WHERE (SELECT ok FROM TestDatabase.flags WHERE id = 2);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("ARRAY(SELECT ...) as a predicate is a clean error, not a crash (was assert(false) UB)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT name FROM TestDatabase.Employees WHERE ARRAY(SELECT id FROM TestDatabase.Departments);");
        REQUIRE(cur->is_error());
    }

    INFO("unsupported sub-query form nested under AND is a clean error, not a null-deref crash");
    {
        // A null intrusive_ptr from transform_sublink_expr must not reach the AND append lambda's
        // child_expr->group() dereference before the top-level has_error() guard runs.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary > 0 AND ARRAY(SELECT id FROM TestDatabase.Departments);");
        REQUIRE(cur->is_error());
    }
}

// F4: a bare boolean-context scalar sub-query is rejected before binding, never silently coerced (PostgreSQL).
TEST_CASE("integration::cpp::test_subqueries::where_having_boolean_required") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/where_having_boolean_required"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.flags (id bigint, ok boolean);")->is_success());
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.flags (id, ok) VALUES (1, true), (2, false);")
                    ->is_success());
    }

    INFO("WHERE (SELECT <int>) is rejected (argument must be boolean)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT name FROM TestDatabase.Employees WHERE (SELECT 1);");
        REQUIRE_FALSE(cur->is_success());
    }

    INFO("WHERE (SELECT <string>) is rejected");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT name FROM TestDatabase.Employees WHERE (SELECT 'x');");
        REQUIRE_FALSE(cur->is_success());
    }

    INFO("WHERE (SELECT <boolean column>) still works (boolean static type accepted)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT name FROM TestDatabase.Employees WHERE (SELECT ok FROM TestDatabase.flags WHERE id = 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("WHERE (SELECT bool WHERE 1=0) — zero rows, static type BOOLEAN — selects nothing, no error");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT name FROM TestDatabase.Employees WHERE (SELECT ok FROM TestDatabase.flags WHERE 1 = 0);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("EXPLAIN of the bad query also errors (the check is static, PostgreSQL-faithful)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT name FROM TestDatabase.Employees WHERE (SELECT 1);");
        REQUIRE_FALSE(cur->is_success());
    }

    INFO("HAVING (SELECT <boolean column>) is supported (new feature)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT dept_id FROM TestDatabase.Employees GROUP BY dept_id "
                                           "HAVING (SELECT ok FROM TestDatabase.flags WHERE id = 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("HAVING (SELECT <int>) is rejected (argument must be boolean)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT dept_id FROM TestDatabase.Employees GROUP BY dept_id "
                                           "HAVING (SELECT 1);");
        REQUIRE_FALSE(cur->is_success());
    }
}

// F7: CAST(<numeric> AS boolean) must be 0->false / non-zero->true (was inverted), and an IMPLICIT
// boolean-vs-numeric comparison is rejected (PostgreSQL: "operator does not exist").
TEST_CASE("integration::cpp::test_subqueries::bool_numeric_coercion") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/bool_numeric_coercion"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    exec("CREATE DATABASE TestDatabase;");
    REQUIRE(exec("CREATE TABLE TestDatabase.flags (id bigint, ok boolean);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.flags (id, ok) VALUES (1, true), (2, false);")->is_success());

    INFO("CAST(non-zero AS boolean) is true, CAST(0 AS boolean) is false (was inverted)");
    {
        auto cur = exec("SELECT CAST(1 AS boolean) AS b;");
        REQUIRE(cur->is_success());
        auto cell31 = cur->get<bool>(0, 0);
        REQUIRE_FALSE(cell31.has_error());
        REQUIRE(cell31.value() == true);
    }
    {
        auto cur = exec("SELECT CAST(0 AS boolean) AS b;");
        REQUIRE(cur->is_success());
        auto cell32 = cur->get<bool>(0, 0);
        REQUIRE_FALSE(cell32.has_error());
        REQUIRE(cell32.value() == false);
    }

    INFO("implicit boolean = numeric is rejected cleanly (operator does not exist), not an abort");
    {
        // Column-vs-column so the numeric side isn't coerced to boolean at bind time (unlike a bare literal);
        // this must be a clean error, not a validate-time throw aborting under -fno-exceptions.
        auto cur = exec("SELECT id FROM TestDatabase.flags WHERE ok = id;");
        REQUIRE_FALSE(cur->is_success());
    }

    INFO("boolean = boolean (same type) still works");
    {
        auto cur = exec("SELECT id FROM TestDatabase.flags WHERE ok = true;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell33 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell33.has_error());
        REQUIRE(cell33.value() == 1);
    }
}

// Tier-1: a compound SELECT dropped its trailing ORDER BY/LIMIT/OFFSET -- gram.y hangs them on the SETOP
// node and the transformer returned before lowering them.
TEST_CASE("integration::cpp::test_subqueries::union_order_by_limit") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/union_order_by_limit"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("UNION ALL with ORDER BY DESC + LIMIT keeps only the top-N in order");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "ORDER BY id DESC LIMIT 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell34 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell34.has_error());
        REQUIRE(cell34.value() == 4);
        auto cell35 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell35.has_error());
        REQUIRE(cell35.value() == 3);
    }

    INFO("UNION ALL with ORDER BY ASC + LIMIT + OFFSET skips correctly");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "ORDER BY id LIMIT 2 OFFSET 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell36 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell36.has_error());
        REQUIRE(cell36.value() == 2);
        auto cell37 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell37.has_error());
        REQUIRE(cell37.value() == 3);
    }

    INFO("bare UNION ALL (no tail clauses) still returns all rows");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("WITH on a compound UNION is visible to the arms (was dropped with the tail clauses)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "WITH e AS (SELECT id FROM TestDatabase.Employees WHERE dept_id = 1) "
                                           "SELECT id FROM e "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        auto cell38 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell38.has_error());
        REQUIRE(cell38.value() == 1);
        auto cell39 = cur->get<int64_t>(0, 3);
        REQUIRE_FALSE(cell39.has_error());
        REQUIRE(cell39.value() == 4);
    }
}

// F5: positional ORDER BY <int> names the n-th output column; a bare integer used to error
// "Unknown node type in ORDER BY".
TEST_CASE("integration::cpp::test_subqueries::positional_order_by") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/positional_order_by"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("plain SELECT: ORDER BY 1 orders by the first output column (dept_id) ascending");
    {
        auto cur = exec("SELECT dept_id, name FROM TestDatabase.Employees ORDER BY 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        auto cell40 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell40.has_error());
        REQUIRE(cell40.value() == 1);
        auto cell41 = cur->get<int64_t>(0, 9);
        REQUIRE_FALSE(cell41.has_error());
        REQUIRE(cell41.value() == 5);
    }

    INFO("plain SELECT: ORDER BY 1 DESC orders by the first output column descending");
    {
        auto cur = exec("SELECT dept_id, name FROM TestDatabase.Employees ORDER BY 1 DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        auto cell42 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell42.has_error());
        REQUIRE(cell42.value() == 5);
    }

    INFO("plain SELECT: ORDER BY 2 orders by the second output column");
    {
        auto cur = exec("SELECT name, salary FROM TestDatabase.Employees ORDER BY 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        auto cell43 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell43.has_error());
        REQUIRE(cell43.value() == 40000);
        auto cell44 = cur->get<int64_t>(1, 9);
        REQUIRE_FALSE(cell44.has_error());
        REQUIRE(cell44.value() == 90000);
    }

    INFO("UNION: positional ORDER BY 1 orders the deduped output by its first column");
    {
        auto cur = exec("SELECT dept_id FROM TestDatabase.Employees "
                        "UNION SELECT dept_id FROM TestDatabase.Employees ORDER BY 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        auto cell45 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell45.has_error());
        REQUIRE(cell45.value() == 1);
        auto cell46 = cur->get<int64_t>(0, 4);
        REQUIRE_FALSE(cell46.has_error());
        REQUIRE(cell46.value() == 5);
    }

    INFO("positional ORDER BY out of range is a clean error, not a crash");
    {
        auto cur = exec("SELECT dept_id FROM TestDatabase.Employees ORDER BY 5;");
        REQUIRE_FALSE(cur->is_success());
    }
}

// Tier-1: chunks().front()-only compaction silently truncated an IN-list past 1024 rows or a multi-chunk UNION ALL.
TEST_CASE("integration::cpp::test_subqueries::in_subquery_spans_all_chunks") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_subquery_spans_all_chunks"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.big (id bigint);")->is_success());
    }
    {
        std::string ins = "INSERT INTO TestDatabase.big (id) VALUES ";
        for (int i = 1; i <= 1100; ++i) {
            ins += "(" + std::to_string(i) + ")";
            ins += (i < 1100) ? "," : ";";
        }
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, ins)->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.probe (v bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.probe (v) VALUES (1050);")->is_success());
    }

    INFO("IN sub-query returning >1024 rows is not truncated at the first chunk");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "SELECT COUNT(*) FROM TestDatabase.probe WHERE v IN (SELECT id FROM TestDatabase.big);");
        REQUIRE(cur->is_success());
        auto cell47 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell47.has_error());
        REQUIRE(cell47.value() == 1);
    }

    INFO("scalar sub-query with 2 rows split across chunks still errors (not chunk-0's value)");
    {
        // A scalar `=` sub-query must see both UNION ALL chunks, not just the first, and error.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "SELECT v FROM TestDatabase.probe WHERE v = "
                                           "(SELECT id FROM TestDatabase.big WHERE id = 1 "
                                           " UNION ALL SELECT id FROM TestDatabase.big WHERE id = 2);");
        REQUIRE(cur->is_error());
    }
}

// Tier-1: recursive UNION (DISTINCT) ran as UNION ALL -- duplicate rows, and cyclic graphs hit the depth cap.
TEST_CASE("integration::cpp::test_subqueries::recursive_cte_union_distinct") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/recursive_cte_union_distinct"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.edges (src bigint, dst bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.edges (src, dst) VALUES (1,2),(1,3),(2,4),(3,4);")
                    ->is_success());
    }

    INFO("recursive UNION de-duplicates the diamond node reachable by two paths");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "WITH RECURSIVE reach AS ("
                                           "  SELECT 1 AS node "
                                           "  UNION "
                                           "  SELECT e.dst FROM TestDatabase.edges e JOIN reach r ON e.src = r.node"
                                           ") SELECT node FROM reach;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("the same shape as UNION ALL keeps both paths (control)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "WITH RECURSIVE reach AS ("
                                           "  SELECT 1 AS node "
                                           "  UNION ALL "
                                           "  SELECT e.dst FROM TestDatabase.edges e JOIN reach r ON e.src = r.node"
                                           ") SELECT node FROM reach;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

// F2: DISTINCT/recursive-UNION dedup must use the canonical typed hash + cells_equal, not a lossy
// 6-sig-digit ostringstream key — the old key collapsed FLOAT/DOUBLE and 128-bit/DECIMAL values to "?".
TEST_CASE("integration::cpp::test_subqueries::distinct_dedup_fidelity") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/distinct_dedup_fidelity"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    exec("CREATE DATABASE TestDatabase;");

    INFO("DISTINCT over a DOUBLE column keeps values that differ beyond 6 significant digits");
    {
        REQUIRE(exec("CREATE TABLE TestDatabase.dvals (v double);")->is_success());
        REQUIRE(exec("INSERT INTO TestDatabase.dvals (v) VALUES "
                     "(1000000.0),(1000001.0),(1000002.0),(1000000.0);")
                    ->is_success());
        auto cur = exec("SELECT DISTINCT v FROM TestDatabase.dvals;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // NOTE: a DISTINCT over a 128-bit column (INT128/hugeint/uuid) would exercise the same hash extension,
    // but DECIMAL-128 column support has other gaps (INSERT/conversion) outside this fix's scope.

    INFO("recursive UNION (DISTINCT) over DOUBLE node ids keeps distinct nodes (no 6-digit collapse)");
    {
        REQUIRE(exec("CREATE TABLE TestDatabase.fedges (src double, dst double);")->is_success());
        REQUIRE(exec("INSERT INTO TestDatabase.fedges (src, dst) VALUES "
                     "(1000000.0, 1000001.0), (1000000.0, 1000002.0), "
                     "(1000001.0, 1000003.0), (1000002.0, 1000003.0);")
                    ->is_success());
        auto cur = exec("WITH RECURSIVE reach AS ("
                        "  SELECT CAST(1000000.0 AS double) AS node "
                        "  UNION "
                        "  SELECT e.dst FROM TestDatabase.fedges e JOIN reach r ON e.src = r.node"
                        ") SELECT count(*) AS c FROM reach;");
        REQUIRE(cur->is_success());
        auto cell48 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell48.has_error());
        REQUIRE(cell48.value() == 4);
    }
}

// Tier-1: a leading WITH on DML was dropped, so `FROM cte` fell through to a base-table lookup.
TEST_CASE("integration::cpp::test_subqueries::with_before_dml") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/with_before_dml"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    exec("CREATE DATABASE TestDatabase;");
    REQUIRE(exec("CREATE TABLE TestDatabase.staging (id bigint, flag bigint);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.staging (id, flag) VALUES (1,1),(2,0),(3,1);")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.t (id bigint);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.t (id) VALUES (1),(2),(3),(4);")->is_success());

    INFO("WITH before DELETE: the CTE feeds the DELETE predicate (was dropped)");
    {
        REQUIRE(exec("WITH c AS (SELECT id FROM TestDatabase.staging WHERE flag = 1) "
                     "DELETE FROM TestDatabase.t WHERE id IN (SELECT id FROM c);")
                    ->is_success());
        auto cur = exec("SELECT id FROM TestDatabase.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell49 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell49.has_error());
        REQUIRE(cell49.value() == 2);
        auto cell50 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell50.has_error());
        REQUIRE(cell50.value() == 4);
    }

    INFO("WITH before INSERT ... SELECT: the CTE feeds the inserted rows");
    {
        REQUIRE(exec("CREATE TABLE TestDatabase.dst (id bigint);")->is_success());
        REQUIRE(exec("WITH c AS (SELECT id FROM TestDatabase.staging WHERE flag = 1) "
                     "INSERT INTO TestDatabase.dst (id) SELECT id FROM c;")
                    ->is_success());
        auto cur = exec("SELECT COUNT(*) FROM TestDatabase.dst;");
        REQUIRE(cur->is_success());
        auto cell51 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell51.has_error());
        REQUIRE(cell51.value() == 2);
    }

    INFO("WITH before UPDATE: the CTE feeds the UPDATE predicate");
    {
        REQUIRE(exec("CREATE TABLE TestDatabase.u (id bigint);")->is_success());
        REQUIRE(exec("INSERT INTO TestDatabase.u (id) VALUES (1),(2),(3);")->is_success());
        REQUIRE(exec("WITH c AS (SELECT id FROM TestDatabase.staging WHERE flag = 1) "
                     "UPDATE TestDatabase.u SET id = 99 WHERE id IN (SELECT id FROM c);")
                    ->is_success());
        auto cur = exec("SELECT COUNT(*) FROM TestDatabase.u WHERE id = 99;");
        REQUIRE(cur->is_success());
        auto cell52 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell52.has_error());
        REQUIRE(cell52.value() == 2);
    }

    INFO("a data-modifying CTE (WITH x AS (DELETE ...)) is rejected cleanly, not a bad cast");
    {
        auto cur = exec("WITH c AS (DELETE FROM TestDatabase.u RETURNING id) SELECT id FROM c;");
        REQUIRE(cur->is_error());
    }
}

// LIMIT unification: operator_limit is the SINGLE authoritative LIMIT/OFFSET operator for every SELECT
// shape, inserted ABOVE DISTINCT/GROUP/JOIN/SORT whenever the window is effective.

TEST_CASE("integration::cpp::test_subqueries::distinct_limit_offset") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/distinct_limit_offset"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("SELECT DISTINCT dept_id LIMIT 3 returns exactly 3 distinct rows (was < 3: scan capped pre-dedup)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT DISTINCT dept_id FROM TestDatabase.Employees LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("SELECT DISTINCT dept_id LIMIT 2 OFFSET 2 returns exactly 2 distinct rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT DISTINCT dept_id FROM TestDatabase.Employees LIMIT 2 OFFSET 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("SELECT DISTINCT dept_id ORDER BY dept_id LIMIT 3 returns the 3 smallest distinct dept_ids in order");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT DISTINCT dept_id FROM TestDatabase.Employees "
                                           "ORDER BY dept_id LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell53 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell53.has_error());
        REQUIRE(cell53.value() == 1);
        auto cell54 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell54.has_error());
        REQUIRE(cell54.value() == 2);
        auto cell55 = cur->get<int64_t>(0, 2);
        REQUIRE_FALSE(cell55.has_error());
        REQUIRE(cell55.value() == 3);
    }

    INFO("SELECT DISTINCT dept_id LIMIT 0 returns 0 rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT DISTINCT dept_id FROM TestDatabase.Employees LIMIT 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("plain SELECT ... LIMIT 0 returns 0 rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT name FROM TestDatabase.Employees LIMIT 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// A non-pushable WHERE (column-vs-column, routes through operator_match) where only tail rows
// match: the inner scan must stay unlimited so LIMIT windows the filtered stream, not a capped scan.
TEST_CASE("integration::cpp::test_subqueries::nonpushable_where_limit_tail") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/nonpushable_where_limit_tail"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    exec("CREATE DATABASE TestDatabase;");
    REQUIRE(exec("CREATE TABLE TestDatabase.tail_match (a bigint, b bigint);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.tail_match (a, b) VALUES "
                 "(100, 1),(100, 2),(100, 3),(100, 4),(100, 5),(100, 6),"
                 "(1, 100),(1, 100),(1, 100),(1, 100);")
                ->is_success());

    INFO("control: WHERE a < b matches exactly the 4 tail rows");
    {
        auto cur = exec("SELECT b FROM TestDatabase.tail_match WHERE a < b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("non-pushable WHERE ... LIMIT 3 returns exactly 3 (inner scan not capped -> filter not starved)");
    {
        auto cur = exec("SELECT b FROM TestDatabase.tail_match WHERE a < b LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("non-pushable WHERE ... LIMIT 2 OFFSET 1 returns exactly 2");
    {
        auto cur = exec("SELECT b FROM TestDatabase.tail_match WHERE a < b LIMIT 2 OFFSET 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_subqueries::limit_unification_regressions") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/limit_unification_regressions"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    INFO("plain SELECT ... LIMIT n returns n rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT name FROM TestDatabase.Employees LIMIT 4;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("pushable WHERE col = const LIMIT n (disk table_filter_t path) still caps correctly");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT name FROM TestDatabase.Employees WHERE dept_id = 3 LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("GROUP BY ... LIMIT n returns n whole groups with correct counts");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, count(*) AS c FROM TestDatabase.Employees "
                                           "GROUP BY dept_id LIMIT 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        for (size_t row = 0; row < cur->size(); ++row) {
            auto cell56 = cur->get<uint64_t>(1, row);
            REQUIRE_FALSE(cell56.has_error());
            REQUIRE(cell56.value() == 2);
        }
    }

    INFO("UNION ALL ... LIMIT n windows the merged concatenation once");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 2 "
                                           "LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

// Top-level VALUES (...) LIMIT/OFFSET: the literal rows are wrapped in an aggregate so
// operator_limit can window them.
TEST_CASE("integration::cpp::test_subqueries::values_top_level_limit") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/values_top_level_limit"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        return dispatcher->execute_sql(s, sql);
    };

    exec("CREATE DATABASE TestDatabase;");

    INFO("VALUES (1),(2),(3) LIMIT 2 returns 2 rows (was a hard parse error)");
    {
        auto cur = exec("VALUES (1),(2),(3) LIMIT 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("VALUES (1),(2),(3) LIMIT 1 OFFSET 1 returns exactly 1 row (the 2nd value)");
    {
        auto cur = exec("VALUES (1),(2),(3) LIMIT 1 OFFSET 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell57 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell57.has_error());
        REQUIRE(cell57.value() == 2);
    }
}

TEST_CASE("integration::cpp::test_subqueries::exists_operand") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/exists_operand"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (x bigint, flag boolean);")->is_success());
    REQUIRE(run("INSERT INTO db.t (x, flag) VALUES (1, true), (2, false);")->is_success());

    INFO("EXISTS is TRUE (sub-query has rows) → matches the flag=true row");
    {
        auto cur = run("SELECT x FROM db.t WHERE flag = EXISTS (SELECT x FROM db.t WHERE x > 0);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell58 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell58.has_error());
        REQUIRE(cell58.value() == 1);
    }
    INFO("EXISTS is FALSE (sub-query empty) → matches the flag=false row");
    {
        auto cur = run("SELECT x FROM db.t WHERE flag = EXISTS (SELECT x FROM db.t WHERE x > 100);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell59 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell59.has_error());
        REQUIRE(cell59.value() == 2);
    }
}

TEST_CASE("integration::cpp::test_subqueries::value_position_scalar") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/value_position_scalar"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (x bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.t (x) VALUES (10), (20), (30);")->is_success());
    REQUIRE(run("CREATE TABLE db.one (id bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.one (id) VALUES (1);")->is_success());
    REQUIRE(run("CREATE TABLE db.empty (y bigint);")->is_success());

    INFO("M4a: scalar sub-query projected in the SELECT list");
    {
        auto cur = run("SELECT (SELECT count(*) FROM db.t) FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell60 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell60.has_error());
        REQUIRE(cell60.value() == 3);
    }
    INFO("M4b: scalar sub-query as an arithmetic operand (per outer row)");
    {
        auto cur = run("SELECT x + (SELECT max(x) FROM db.t) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    INFO("Reentrancy: outer aggregate + value-position sub-query in the same list");
    {
        auto cur = run("SELECT sum(x) + (SELECT count(*) FROM db.t) FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell61 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell61.has_error());
        REQUIRE(cell61.value() == 63);
    }
    INFO("M4-NULL: a 0-row scalar sub-query projects a typed NULL row");
    {
        auto cur = run("SELECT (SELECT max(y) FROM db.empty) FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell62 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell62.has_error());
        REQUIRE(cell62.value());
    }
}

TEST_CASE("integration::cpp::test_subqueries::null_literal_typing") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/null_literal_typing"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.one (id bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.one (id) VALUES (1);")->is_success());

    INFO("bare NULL literal projects a typed NULL row (was rule-6 rejected)");
    {
        auto cur = run("SELECT NULL FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell63 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell63.has_error());
        REQUIRE(cell63.value());
    }
    INFO("NULL::int is a proper NULL, not a garbage non-null value");
    {
        auto cur = run("SELECT NULL::int FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell64 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell64.has_error());
        REQUIRE(cell64.value());
    }
    INFO("SELECT (SELECT NULL ...) — scalar sub-query yielding NULL projects a typed NULL row");
    {
        auto cur = run("SELECT (SELECT NULL FROM db.one) FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell65 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell65.has_error());
        REQUIRE(cell65.value());
    }
}

TEST_CASE("integration::cpp::test_subqueries::like_ilike_family") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/like_ilike_family"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (id bigint, s string);")->is_success());
    REQUIRE(run("INSERT INTO db.t (id, s) VALUES (1, 'apple'), (2, 'Banana'), (3, 'cherry');")->is_success());
    REQUIRE(run("CREATE TABLE db.pat (p string);")->is_success());
    REQUIRE(run("INSERT INTO db.pat (p) VALUES ('A%'), ('C%');")->is_success());

    INFO("LIKE ANY (case-sensitive, %/_): uppercase patterns match no lowercase row");
    {
        auto cur = run("SELECT id FROM db.t WHERE s LIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("ILIKE ANY (case-insensitive): 'A%'/'C%' match apple and cherry");
    {
        auto cur = run("SELECT id FROM db.t WHERE s ILIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    INFO("NOT LIKE ANY: every row fails at least one uppercase pattern (case-sensitive)");
    {
        auto cur = run("SELECT id FROM db.t WHERE s NOT LIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    INFO("scalar ILIKE matches case-insensitively");
    {
        auto cur = run("SELECT id FROM db.t WHERE s ILIKE 'banana';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell66 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell66.has_error());
        REQUIRE(cell66.value() == 2);
    }
    INFO("scalar NOT ILIKE");
    {
        auto cur = run("SELECT id FROM db.t WHERE s NOT ILIKE 'banana';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_subqueries::any_subquery_disk_pushdown") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/any_subquery_disk_pushdown"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (id bigint, v bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);")->is_success());
    REQUIRE(run("CREATE TABLE db.keys (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.keys (k) VALUES (20), (40);")->is_success());

    INFO("= ANY (SELECT ...) is pushed to the disk scan as one membership filter");
    {
        auto cur = run("SELECT id FROM db.t WHERE v = ANY (SELECT k FROM db.keys);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    INFO("IN (SELECT ...)");
    {
        auto cur = run("SELECT id FROM db.t WHERE v IN (SELECT k FROM db.keys);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    INFO("> ALL (SELECT ...) is pushed to the disk scan as one membership filter");
    {
        auto cur = run("SELECT id FROM db.t WHERE v > ALL (SELECT k FROM db.keys);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    INFO("<> ALL (SELECT ...) == NOT IN");
    {
        auto cur = run("SELECT id FROM db.t WHERE v <> ALL (SELECT k FROM db.keys);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    INFO("= ANY (empty sub-query) matches nothing");
    {
        auto cur = run("SELECT id FROM db.t WHERE v = ANY (SELECT k FROM db.keys WHERE k > 1000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("<> ALL (empty sub-query) matches everything");
    {
        auto cur = run("SELECT id FROM db.t WHERE v <> ALL (SELECT k FROM db.keys WHERE k > 1000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }
}

TEST_CASE("integration::cpp::test_subqueries::like_any_disk_pushdown") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/like_any_disk_pushdown"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (id bigint, s string);")->is_success());
    REQUIRE(run("INSERT INTO db.t (id, s) VALUES (1, 'apple'), (2, 'Banana'), (3, 'cherry'), (4, 'avocado');")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.pat (p string);")->is_success());
    REQUIRE(run("INSERT INTO db.pat (p) VALUES ('A%'), ('C%');")->is_success());

    INFO("LIKE ANY (disk, case-sensitive): uppercase patterns match no lowercase row");
    {
        auto cur = run("SELECT id FROM db.t WHERE s LIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("ILIKE ANY (disk, case-insensitive): 'A%'/'C%' match apple, cherry, avocado");
    {
        auto cur = run("SELECT id FROM db.t WHERE s ILIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    INFO("ILIKE ALL (disk): no row starts with both A and C");
    {
        auto cur = run("SELECT id FROM db.t WHERE s ILIKE ALL (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("NOT LIKE ANY (disk conjunction_not): every row fails at least one uppercase pattern");
    {
        auto cur = run("SELECT id FROM db.t WHERE s NOT LIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }
    // NULL subject: `NULL NOT LIKE p` is NULL -> excluded (is_not_null guard); without it the disk regex
    // reads NULL as empty, negates to true, and wrongly includes the row.
    REQUIRE(run("INSERT INTO db.t (id, s) VALUES (99, NULL);")->is_success());
    INFO("NOT LIKE ANY excludes a NULL subject");
    {
        auto cur = run("SELECT id FROM db.t WHERE s NOT LIKE ANY (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }
    INFO("NOT LIKE ALL excludes a NULL subject");
    {
        auto cur = run("SELECT id FROM db.t WHERE s NOT LIKE ALL (SELECT p FROM db.pat);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }
}

TEST_CASE("integration::cpp::test_subqueries::union_null_reconcile") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/union_null_reconcile"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.one (id bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.one (id) VALUES (1);")->is_success());

    INFO("NULL on the right branch reconciles to the left (bigint) type");
    {
        auto cur = run("SELECT id FROM db.one UNION SELECT NULL FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    INFO("NULL on the left branch reconciles to the right (bigint) type");
    {
        auto cur = run("SELECT NULL FROM db.one UNION SELECT id FROM db.one;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

// A sub-query's ORDER BY is dead work when its result is compacted (IN/ANY/ALL, EXISTS, scalar),
// so the transformer strips it; it survives when it carries LIMIT/OFFSET (an observable top-N).
TEST_CASE("integration::cpp::test_subqueries::sort_elimination") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/sort_elimination"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    INFO("IN (SELECT ... ORDER BY) is order-insensitive: identical result, sub-query Sort stripped");
    {
        auto cur = run("SELECT name FROM TestDatabase.Employees "
                       "WHERE dept_id IN (SELECT id FROM TestDatabase.Departments "
                       "                  WHERE budget > 60000 ORDER BY id);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);

        auto ex = run("EXPLAIN ANALYZE SELECT name FROM TestDatabase.Employees "
                      "WHERE dept_id IN (SELECT id FROM TestDatabase.Departments "
                      "                  WHERE budget > 60000 ORDER BY id);");
        REQUIRE(ex->is_success());
        const auto t = plan_text(ex);
        REQUIRE(contains(t, "InitPlan"));
        REQUIRE_FALSE(contains(t, "Sort"));
    }

    INFO("EXISTS (SELECT ... ORDER BY) is order-insensitive: identical result, sub-query Sort stripped");
    {
        auto cur = run("SELECT name FROM TestDatabase.Employees "
                       "WHERE EXISTS (SELECT id FROM TestDatabase.Departments ORDER BY id);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);

        auto ex = run("EXPLAIN ANALYZE SELECT name FROM TestDatabase.Employees "
                      "WHERE EXISTS (SELECT id FROM TestDatabase.Departments ORDER BY id);");
        REQUIRE(ex->is_success());
        const auto t = plan_text(ex);
        REQUIRE(contains(t, "InitPlan"));
        REQUIRE_FALSE(contains(t, "Sort"));
    }

    INFO("ANY (SELECT ... ORDER BY) is order-insensitive: identical result, sub-query Sort stripped");
    {
        auto cur = run("SELECT name FROM TestDatabase.Employees "
                       "WHERE salary = ANY (SELECT salary FROM TestDatabase.Employees "
                       "                    WHERE dept_id = 1 ORDER BY salary);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        auto ex = run("EXPLAIN ANALYZE SELECT name FROM TestDatabase.Employees "
                      "WHERE salary = ANY (SELECT salary FROM TestDatabase.Employees "
                      "                    WHERE dept_id = 1 ORDER BY salary);");
        REQUIRE(ex->is_success());
        REQUIRE_FALSE(contains(plan_text(ex), "Sort"));
    }

    INFO("NEGATIVE: scalar (SELECT ... ORDER BY ... LIMIT 1) is a top-N — the Sort is OBSERVABLE and stays");
    {
        auto cur = run("SELECT name FROM TestDatabase.Employees "
                       "WHERE salary = (SELECT salary FROM TestDatabase.Employees "
                       "                ORDER BY salary DESC LIMIT 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell67 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell67.has_error());
        REQUIRE(cell67.value() == "Alice");

        auto ex = run("EXPLAIN ANALYZE SELECT name FROM TestDatabase.Employees "
                      "WHERE salary = (SELECT salary FROM TestDatabase.Employees "
                      "                ORDER BY salary DESC LIMIT 1);");
        REQUIRE(ex->is_success());
        REQUIRE(contains(plan_text(ex), "Sort"));
    }

    INFO("NEGATIVE: a TOP-LEVEL ORDER BY (the main query, not a sub-query) is never touched");
    {
        auto cur = run("SELECT name FROM TestDatabase.Employees ORDER BY salary DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        auto cell68 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell68.has_error());
        REQUIRE(cell68.value() == "Alice");

        auto ex = run("EXPLAIN SELECT name FROM TestDatabase.Employees ORDER BY salary DESC;");
        REQUIRE(ex->is_success());
        REQUIRE(contains(plan_text(ex), "Sort"));
    }
}
// IN/NOT IN over an S holding NULL is three-valued: a non-match is UNKNOWN, not the naive opposite.
// S empty: IN -> FALSE, NOT IN -> TRUE for every x.
TEST_CASE("integration::cpp::test_subqueries::in_not_in_null_semantics") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_not_in_null_semantics"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    INFO("setup: outer with x in {10,20,30}; needles with a NULL element {10,NULL,30}");
    {
        REQUIRE(run("CREATE DATABASE nulldb;")->is_success());
        REQUIRE(run("CREATE TABLE nulldb.outer (id bigint, x bigint);")->is_success());
        REQUIRE(run("INSERT INTO nulldb.outer (id, x) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
        REQUIRE(run("CREATE TABLE nulldb.needles (id bigint, v bigint);")->is_success());
        REQUIRE(run("INSERT INTO nulldb.needles (id, v) VALUES (1, 10), (2, NULL), (3, 30);")->is_success());
        REQUIRE(run("CREATE TABLE nulldb.pure (id bigint, v bigint);")->is_success());
        REQUIRE(run("INSERT INTO nulldb.pure (id, v) VALUES (1, 10), (2, 30);")->is_success());
    }

    INFO("IN with a NULL element: x=10 and x=30 match; x=20 is UNKNOWN -> dropped");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x IN (SELECT v FROM nulldb.needles);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("NOT IN with a NULL element: yields ZERO rows (no non-matched row survives the NULL)");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x NOT IN (SELECT v FROM nulldb.needles);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("IN over a no-NULL set is a plain FALSE for the non-member (regression)");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x IN (SELECT v FROM nulldb.pure);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("NOT IN over a no-NULL set keeps the non-member (regression)");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x NOT IN (SELECT v FROM nulldb.pure);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell69 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell69.has_error());
        REQUIRE(cell69.value() == 2);
    }

    INFO("IN over an EMPTY sub-query is FALSE for all -> zero rows");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x IN (SELECT v FROM nulldb.pure WHERE v > 1000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("NOT IN over an EMPTY sub-query is TRUE for all -> every row");
    {
        auto cur = run("SELECT id FROM nulldb.outer WHERE x NOT IN (SELECT v FROM nulldb.pure WHERE v > 1000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("Sub-query is ALL NULLs: IN -> zero rows; NOT IN -> zero rows");
    {
        auto in_cur = run("SELECT id FROM nulldb.outer "
                          "WHERE x IN (SELECT v FROM nulldb.needles WHERE v IS NULL);");
        REQUIRE(in_cur->is_success());
        REQUIRE(in_cur->size() == 0);

        auto notin_cur = run("SELECT id FROM nulldb.outer "
                             "WHERE x NOT IN (SELECT v FROM nulldb.needles WHERE v IS NULL);");
        REQUIRE(notin_cur->is_success());
        REQUIRE(notin_cur->size() == 0);
    }
}

// The pending internal-aggregate stash must survive a sub-query transform that runs before it flushes --
// without a save/restore around each inner transform, the outer aggregate leaks into the inner group.
TEST_CASE("integration::cpp::test_subqueries::outer_aggregate_survives_where_subquery") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/outer_agg_where_subquery"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    { setup_subquery_db(dispatcher); }

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    INFO("scalar (EXPR) sub-query as a WHERE comparison operand");
    {
        auto cur = run("SELECT SUM(salary) + 10 AS s FROM TestDatabase.Employees "
                       "WHERE salary > (SELECT AVG(budget) FROM TestDatabase.Departments);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell70 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell70.has_error());
        REQUIRE(cell70.value() == 387010);
    }

    INFO("EXISTS sub-query in WHERE (speculative semi/anti probe + flatten path)");
    {
        auto cur = run("SELECT SUM(salary) + 10 AS s FROM TestDatabase.Employees "
                       "WHERE EXISTS (SELECT id FROM TestDatabase.Departments WHERE budget > 60000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell71 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell71.has_error());
        REQUIRE(cell71.value() == 652010);
    }

    INFO("ANY sub-query in WHERE");
    {
        auto cur = run("SELECT SUM(salary) + 10 AS s FROM TestDatabase.Employees "
                       "WHERE dept_id = ANY (SELECT id FROM TestDatabase.Departments WHERE budget > 60000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell72 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell72.has_error());
        REQUIRE(cell72.value() == 452010);
    }
}

// DISTINCT ON makes a sub-query's ORDER BY observable without LIMIT; sort elimination must not strip it here.
TEST_CASE("integration::cpp::test_subqueries::distinct_on_subquery_sort_kept") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/distinct_on_subquery_sort"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.events (k bigint, ts bigint, v bigint);")->is_success());
    // Insertion order is oldest-ts first, so a wrongly-stripped sort would keep the oldest, not latest, row per k.
    REQUIRE(run("INSERT INTO db.events (k, ts, v) VALUES "
                "(1, 1, 100), (2, 1, 300), (1, 2, 200), (2, 2, 400);")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.vals (x bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.vals (x) VALUES (200), (400), (100), (300);")->is_success());

    INFO("IN (SELECT DISTINCT ON (k) v ... ORDER BY k, ts DESC) keeps the latest row per k");
    {
        auto cur = run("SELECT x FROM db.vals WHERE x IN "
                       "(SELECT DISTINCT ON (k) v FROM db.events ORDER BY k, ts DESC);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell73 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell73.has_error());
        const auto a = cell73.value();
        auto cell74 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell74.has_error());
        const auto b = cell74.value();
        const bool latest_pair = (a == 200 && b == 400) || (a == 400 && b == 200);
        REQUIRE(latest_pair);
    }

    INFO("the sub-query Sort survives in the plan (it feeds DISTINCT ON, not a LIMIT)");
    {
        auto ex = run("EXPLAIN ANALYZE SELECT x FROM db.vals WHERE x IN "
                      "(SELECT DISTINCT ON (k) v FROM db.events ORDER BY k, ts DESC);");
        REQUIRE(ex->is_success());
        const auto t = plan_text(ex);
        REQUIRE(contains(t, "InitPlan"));
        REQUIRE(contains(t, "Sort"));
    }
}

// A NULL probe value against an empty set / a NULL-only set, and a result larger than one chunk with a NULL element.
TEST_CASE("integration::cpp::test_subqueries::in_not_in_null_probe_and_null_only_set") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_not_in_null_probe"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE pdb;")->is_success());
    REQUIRE(run("CREATE TABLE pdb.probe (id bigint, x bigint);")->is_success());
    REQUIRE(run("INSERT INTO pdb.probe (id, x) VALUES (1, 10), (2, NULL), (3, 1500);")->is_success());
    REQUIRE(run("CREATE TABLE pdb.big (v bigint);")->is_success());
    {
        std::string ins = "INSERT INTO pdb.big (v) VALUES (NULL)";
        for (int i = 1; i <= 1100; ++i) {
            ins += ", (" + std::to_string(i) + ")";
        }
        REQUIRE(run(ins + ";")->is_success());
    }

    INFO("x = ANY (empty): FALSE for every x, NULL probe included -> zero rows");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x = ANY (SELECT v FROM pdb.big WHERE v > 100000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("x > ALL (empty): TRUE for every x, NULL probe included -> all three rows");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x > ALL (SELECT v FROM pdb.big WHERE v > 100000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("x IN (empty): FALSE for every x, NULL probe included -> zero rows");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x IN (SELECT v FROM pdb.big WHERE v > 100000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("x NOT IN (empty): TRUE for every x, NULL probe included -> all three rows");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x NOT IN (SELECT v FROM pdb.big WHERE v > 100000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("1101-element set with a NULL element: member found beyond the first chunk, non-member UNKNOWN");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x IN (SELECT v FROM pdb.big);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell75 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell75.has_error());
        REQUIRE(cell75.value() == 1);
    }

    INFO("NOT IN over the 1101-element set with a NULL element: zero rows");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x NOT IN (SELECT v FROM pdb.big);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// PostgreSQL resolves an untyped NULL column of a sub-select as text (resolveTargetListUnknowns), so the
// membership test is `bigint = text`, which has no operator; text and typed NULLs compare fine and are never TRUE.
TEST_CASE("integration::cpp::test_subqueries::in_not_in_select_null") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_not_in_select_null"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto error_of = [](const components::cursor::cursor_t_ptr& cur) {
        return cur->is_error() ? std::string(cur->get_error().what) : std::string("none");
    };

    REQUIRE(run("CREATE DATABASE pdb;")->is_success());
    REQUIRE(run("CREATE TABLE pdb.probe (id bigint, x bigint, s string);")->is_success());
    REQUIRE(
        run("INSERT INTO pdb.probe (id, x, s) VALUES (1, 10, 'a'), (2, NULL, NULL), (3, 1500, 'b');")->is_success());
    REQUIRE(run("CREATE TABLE pdb.empty_set (k bigint);")->is_success());

    INFO("bigint IN (SELECT NULL): the NULL column is text, so the test is `bigint = text`");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x IN (SELECT NULL);");
        REQUIRE(cur->is_error());
        INFO("error: " << error_of(cur));
        REQUIRE(error_of(cur).find("operator does not exist: bigint = text") != std::string::npos);
    }
    INFO("bigint NOT IN (SELECT NULL): the same error, spelled with the operator NOT IN is lowered to (<> ALL)");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x NOT IN (SELECT NULL);");
        REQUIRE(cur->is_error());
        INFO("error: " << error_of(cur));
        REQUIRE(error_of(cur).find("operator does not exist: bigint <> text") != std::string::npos);
    }
    INFO("bigint < ANY (SELECT NULL) names its own operator");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x < ANY (SELECT NULL);");
        REQUIRE(cur->is_error());
        INFO("error: " << error_of(cur));
        REQUIRE(error_of(cur).find("operator does not exist: bigint < text") != std::string::npos);
    }
    INFO("text IN (SELECT NULL): text = text, never TRUE");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE s IN (SELECT NULL);");
        INFO("error: " << error_of(cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("text NOT IN (SELECT NULL): never TRUE");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE s NOT IN (SELECT NULL);");
        INFO("error: " << error_of(cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("bigint IN (SELECT NULL::bigint): typed, never TRUE");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x IN (SELECT CAST(NULL AS bigint));");
        INFO("error: " << error_of(cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("bigint NOT IN (SELECT NULL::bigint): never TRUE");
    {
        auto cur = run("SELECT id FROM pdb.probe WHERE x NOT IN (SELECT CAST(NULL AS bigint));");
        INFO("error: " << error_of(cur));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("contrast, empty set: IN matches nothing, NOT IN matches every row, a NULL probe included");
    {
        auto in_cur = run("SELECT id FROM pdb.probe WHERE x IN (SELECT k FROM pdb.empty_set);");
        REQUIRE(in_cur->is_success());
        REQUIRE(in_cur->size() == 0);
        auto not_in_cur = run("SELECT id FROM pdb.probe WHERE x NOT IN (SELECT k FROM pdb.empty_set);");
        REQUIRE(not_in_cur->is_success());
        REQUIRE(not_in_cur->size() == 3);
    }
}

namespace {

    // Column 0 of every row, ascending
    std::vector<int64_t> ids_of(const components::cursor::cursor_t_ptr& cur) {
        std::vector<int64_t> ids;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell76 = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell76.has_error());
            ids.push_back(cell76.value());
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    using ids_t = std::vector<int64_t>;

} // namespace

// The probe and the set come from different integer widths: the test is done in the wider one, the probe is cast
// per chunk and the set once.
TEST_CASE("integration::cpp::test_subqueries::in_any_all_mixed_integer_widths") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_any_all_mixed_integer_widths"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.probe (id bigint, x bigint, i integer);")->is_success());
    REQUIRE(run("INSERT INTO db.probe (id, x, i) VALUES (1, 10, 10), (2, NULL, NULL), (3, 1500, 1500), (4, 30, 30);")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.small_set (k integer);")->is_success());
    REQUIRE(run("INSERT INTO db.small_set (k) VALUES (10), (30);")->is_success());
    REQUIRE(run("CREATE TABLE db.wide_set (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.wide_set (k) VALUES (10), (1500), (5000000000);")->is_success());

    INFO("bigint IN (SELECT integer)");
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.small_set);") == ids_t{1, 4});
    INFO("bigint NOT IN (SELECT integer): a NULL probe is never TRUE");
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.small_set);") == ids_t{3});
    INFO("integer IN (SELECT bigint): the set holds a value no integer can");
    CHECK(ids("SELECT id FROM db.probe WHERE i IN (SELECT k FROM db.wide_set);") == ids_t{1, 3});
    INFO("integer NOT IN (SELECT bigint)");
    CHECK(ids("SELECT id FROM db.probe WHERE i NOT IN (SELECT k FROM db.wide_set);") == ids_t{4});
    INFO("bigint = ANY (SELECT integer)");
    CHECK(ids("SELECT id FROM db.probe WHERE x = ANY (SELECT k FROM db.small_set);") == ids_t{1, 4});
    INFO("bigint < ANY (SELECT integer): below the largest");
    CHECK(ids("SELECT id FROM db.probe WHERE x < ANY (SELECT k FROM db.small_set);") == ids_t{1});
    INFO("bigint > ALL (SELECT integer): above the largest");
    CHECK(ids("SELECT id FROM db.probe WHERE x > ALL (SELECT k FROM db.small_set);") == ids_t{3});
    INFO("integer >= ALL (SELECT bigint)");
    CHECK(ids("SELECT id FROM db.probe WHERE i >= ALL (SELECT k FROM db.small_set);") == ids_t{3, 4});
    INFO("integer <= ANY (SELECT bigint)");
    CHECK(ids("SELECT id FROM db.probe WHERE i <= ANY (SELECT k FROM db.wide_set);") == ids_t{1, 3, 4});
    INFO("bigint <> ALL (SELECT integer) == NOT IN");
    CHECK(ids("SELECT id FROM db.probe WHERE x <> ALL (SELECT k FROM db.small_set);") == ids_t{3});
    INFO("bigint <> ANY (SELECT integer)");
    CHECK(ids("SELECT id FROM db.probe WHERE x <> ANY (SELECT k FROM db.small_set);") == ids_t{1, 3, 4});
    INFO("bigint = ALL (SELECT integer) over a one-valued set");
    CHECK(ids("SELECT id FROM db.probe WHERE x = ALL (SELECT k FROM db.small_set WHERE k = 30);") == ids_t{4});
}

// Three-valued logic of the membership test, row by row
TEST_CASE("integration::cpp::test_subqueries::in_any_all_null_and_empty_sets") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_any_all_null_and_empty_sets"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.probe (id bigint, x bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.probe (id, x) VALUES (1, 5), (2, 10), (3, 20), (4, NULL);")->is_success());
    // with a NULL element
    REQUIRE(run("CREATE TABLE db.with_null (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.with_null (k) VALUES (10), (NULL);")->is_success());
    // all elements NULL
    REQUIRE(run("CREATE TABLE db.only_null (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.only_null (k) VALUES (NULL), (NULL);")->is_success());
    REQUIRE(run("CREATE TABLE db.empty_set (k bigint);")->is_success());
    REQUIRE(run("CREATE TABLE db.plain (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.plain (k) VALUES (10), (30);")->is_success());

    INFO("IN over {10, NULL}: TRUE for 10, UNKNOWN for the rest");
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.with_null);") == ids_t{2});
    INFO("... which is why NOT IN over a set holding a NULL is never TRUE");
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.with_null);").empty());
    CHECK(ids("SELECT id FROM db.probe WHERE x <> ALL (SELECT k FROM db.with_null);").empty());
    CHECK(ids("SELECT id FROM db.probe WHERE NOT (x IN (SELECT k FROM db.with_null));").empty());

    INFO("< ANY over {10, NULL}: TRUE when below 10, otherwise UNKNOWN");
    CHECK(ids("SELECT id FROM db.probe WHERE x < ANY (SELECT k FROM db.with_null);") == ids_t{1});
    INFO("> ALL over {10, NULL}: FALSE when not above 10, otherwise UNKNOWN: never TRUE");
    CHECK(ids("SELECT id FROM db.probe WHERE x > ALL (SELECT k FROM db.with_null);").empty());
    INFO("... and its complement is TRUE only where a witness exists: 20 and the NULL probe are UNKNOWN");
    CHECK(ids("SELECT id FROM db.probe WHERE NOT (x > ALL (SELECT k FROM db.with_null));") == ids_t{1, 2});
    INFO(">= ALL over {10, NULL}");
    CHECK(ids("SELECT id FROM db.probe WHERE x >= ALL (SELECT k FROM db.with_null);").empty());
    INFO("<= ANY over {10, NULL}");
    CHECK(ids("SELECT id FROM db.probe WHERE x <= ANY (SELECT k FROM db.with_null);") == ids_t{1, 2});
    INFO("<> ANY over {10, NULL}: a witness needs a non-NULL element that differs");
    CHECK(ids("SELECT id FROM db.probe WHERE x <> ANY (SELECT k FROM db.with_null);") == ids_t{1, 3});
    INFO("= ALL over {10, NULL}: a counterexample decides FALSE, otherwise UNKNOWN");
    CHECK(ids("SELECT id FROM db.probe WHERE x = ALL (SELECT k FROM db.with_null);").empty());

    INFO("a set of NULLs only: nothing is TRUE, nothing is FALSE");
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.only_null);").empty());
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.only_null);").empty());

    INFO("empty set: ANY is FALSE and ALL is TRUE for every probe, a NULL one included");
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.empty_set);").empty());
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.empty_set);") == ids_t{1, 2, 3, 4});
    CHECK(ids("SELECT id FROM db.probe WHERE x < ANY (SELECT k FROM db.empty_set);").empty());
    CHECK(ids("SELECT id FROM db.probe WHERE x > ALL (SELECT k FROM db.empty_set);") == ids_t{1, 2, 3, 4});
    CHECK(ids("SELECT id FROM db.probe WHERE x = ALL (SELECT k FROM db.empty_set);") == ids_t{1, 2, 3, 4});
    CHECK(ids("SELECT id FROM db.probe WHERE x <> ANY (SELECT k FROM db.empty_set);").empty());

    INFO("a NULL probe against a non-empty set is UNKNOWN");
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.plain);") == ids_t{2});
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.plain);") == ids_t{1, 3});
}

// One membership test per value family the vector hash and cells_equal have to agree on
TEST_CASE("integration::cpp::test_subqueries::in_any_all_value_types") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_any_all_value_types"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")->is_success());
    REQUIRE(run("CREATE TABLE db.probe (id bigint, s string, d decimal(10,2), m mood, f double);")->is_success());
    REQUIRE(run("INSERT INTO db.probe (id, s, d, m, f) VALUES "
                "(1, 'apple', 1.50, 'sad', 0.5), (2, 'pear', 2.25, 'ok', 0.0), (3, 'fig', 3.00, 'happy', 2.5), "
                "(4, NULL, NULL, NULL, NULL);")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.sets (s string, d decimal(10,2), m mood, f double);")->is_success());
    REQUIRE(run("INSERT INTO db.sets (s, d, m, f) VALUES ('pear', 2.25, 'ok', -0.0), ('fig', 3.00, 'happy', 0.5);")
                ->is_success());

    INFO("string");
    CHECK(ids("SELECT id FROM db.probe WHERE s IN (SELECT s FROM db.sets);") == ids_t{2, 3});
    CHECK(ids("SELECT id FROM db.probe WHERE s NOT IN (SELECT s FROM db.sets);") == ids_t{1});
    CHECK(ids("SELECT id FROM db.probe WHERE s < ANY (SELECT s FROM db.sets);") == ids_t{1, 3});
    INFO("decimal(10,2)");
    CHECK(ids("SELECT id FROM db.probe WHERE d IN (SELECT d FROM db.sets);") == ids_t{2, 3});
    CHECK(ids("SELECT id FROM db.probe WHERE d NOT IN (SELECT d FROM db.sets);") == ids_t{1});
    CHECK(ids("SELECT id FROM db.probe WHERE d >= ALL (SELECT d FROM db.sets);") == ids_t{3});
    INFO("enum");
    CHECK(ids("SELECT id FROM db.probe WHERE m IN (SELECT m FROM db.sets);") == ids_t{2, 3});
    CHECK(ids("SELECT id FROM db.probe WHERE m NOT IN (SELECT m FROM db.sets);") == ids_t{1});
    INFO("date, timestamp, interval");
    REQUIRE(run("CREATE TABLE db.moments (id bigint, d date, ts timestamp, iv interval);")->is_success());
    REQUIRE(run("INSERT INTO db.moments (id, d, ts, iv) VALUES "
                "(1, '2024-01-01', '2024-01-01 10:00:00', INTERVAL '1 day'), "
                "(2, '2024-02-02', '2024-02-02 11:30:00', INTERVAL '2 hours'), "
                "(3, '2024-03-03', '2024-03-03 12:00:00', INTERVAL '3 months');")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.moment_set (d date, ts timestamp, iv interval);")->is_success());
    REQUIRE(run("INSERT INTO db.moment_set (d, ts, iv) VALUES "
                "('2024-02-02', '2024-03-03 12:00:00', INTERVAL '2 hours'), "
                "('2024-03-03', '2024-01-01 10:00:00', INTERVAL '1 day');")
                ->is_success());
    CHECK(ids("SELECT id FROM db.moments WHERE d IN (SELECT d FROM db.moment_set);") == ids_t{2, 3});
    CHECK(ids("SELECT id FROM db.moments WHERE ts IN (SELECT ts FROM db.moment_set);") == ids_t{1, 3});
    CHECK(ids("SELECT id FROM db.moments WHERE iv IN (SELECT iv FROM db.moment_set);") == ids_t{1, 2});
    CHECK(ids("SELECT id FROM db.moments WHERE ts < ANY (SELECT ts FROM db.moment_set);") == ids_t{1, 2});
    INFO("double: 0.0 and -0.0 are the same value");
    CHECK(ids("SELECT id FROM db.probe WHERE f IN (SELECT f FROM db.sets);") == ids_t{1, 2});
    CHECK(ids("SELECT id FROM db.probe WHERE f NOT IN (SELECT f FROM db.sets);") == ids_t{3});
}

// Sets past one vector (1024) and a lot of them
TEST_CASE("integration::cpp::test_subqueries::in_any_all_large_set") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_any_all_large_set"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.big (k bigint);")->is_success());
    for (int first = 1; first <= 100000; first += 1000) {
        std::string insert = "INSERT INTO db.big (k) VALUES ";
        for (int k = first; k < first + 1000; ++k) {
            insert += "(" + std::to_string(k * 2) + (k + 1 < first + 1000 ? ")," : ");");
        }
        REQUIRE(run(insert)->is_success());
    }
    REQUIRE(run("CREATE TABLE db.probe (id bigint, x bigint);")->is_success());
    // even numbers up to 200000 are in the set
    REQUIRE(run("INSERT INTO db.probe (id, x) VALUES (1, 2), (2, 3), (3, 99999), (4, 100000), (5, 200000), "
                "(6, 200001), (7, 1000), (8, NULL);")
                ->is_success());

    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };
    CHECK(ids("SELECT id FROM db.probe WHERE x IN (SELECT k FROM db.big);") == ids_t{1, 4, 5, 7});
    CHECK(ids("SELECT id FROM db.probe WHERE x NOT IN (SELECT k FROM db.big);") == ids_t{2, 3, 6});
    CHECK(ids("SELECT id FROM db.probe WHERE x < ANY (SELECT k FROM db.big);") == ids_t{1, 2, 3, 4, 7});
    CHECK(ids("SELECT id FROM db.probe WHERE x > ALL (SELECT k FROM db.big);") == ids_t{6});
}

// LATERAL re-binds the parameters for every outer row; the membership test has to follow the new correlation values
// while the set stays the same
TEST_CASE("integration::cpp::test_subqueries::in_any_all_lateral_reread_parameters") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/in_any_all_lateral_reread_parameters"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { setup_subquery_db(dispatcher); }

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto counts = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<std::pair<int64_t, int64_t>> rows;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell77 = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell77.has_error());
            auto cell78 = cur->get<int64_t>(1, row);
            REQUIRE_FALSE(cell78.has_error());
            rows.emplace_back(cell77.value(), cell78.value());
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    };
    using rows_t = std::vector<std::pair<int64_t, int64_t>>;

    // the set is {1, 5}: the departments of employees paid above 70000
    CHECK(counts("SELECT d.id, s.n FROM TestDatabase.Departments d, LATERAL "
                 "(SELECT count(*) AS n FROM TestDatabase.Employees e WHERE e.salary > d.budget AND e.dept_id IN "
                 "(SELECT e2.dept_id FROM TestDatabase.Employees e2 WHERE e2.salary > 70000)) s;") ==
          rows_t{{1, 0}, {2, 4}, {3, 4}, {4, 1}, {5, 4}});
    CHECK(counts("SELECT d.id, s.n FROM TestDatabase.Departments d, LATERAL "
                 "(SELECT count(*) AS n FROM TestDatabase.Employees e WHERE e.salary > d.budget AND e.dept_id NOT IN "
                 "(SELECT e2.dept_id FROM TestDatabase.Employees e2 WHERE e2.salary > 70000)) s;") ==
          rows_t{{1, 0}, {2, 4}, {3, 6}, {4, 0}, {5, 0}});
}

// A filter pushed to the disk agent builds its membership state there, once for the scan
TEST_CASE("integration::cpp::test_subqueries::in_any_all_disk_pushdown_mixed_and_null") {
    auto config =
        test_create_config(integration_fixture_path("test_subqueries/in_any_all_disk_pushdown_mixed_and_null"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (id bigint, v bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, NULL);")->is_success());
    REQUIRE(run("CREATE TABLE db.keys_int (k integer);")->is_success());
    REQUIRE(run("INSERT INTO db.keys_int (k) VALUES (20), (40);")->is_success());
    REQUIRE(run("CREATE TABLE db.keys_null (k bigint);")->is_success());
    REQUIRE(run("INSERT INTO db.keys_null (k) VALUES (20), (NULL);")->is_success());

    INFO("a narrower set is widened once");
    CHECK(ids("SELECT id FROM db.t WHERE v IN (SELECT k FROM db.keys_int);") == ids_t{2, 4});
    CHECK(ids("SELECT id FROM db.t WHERE v NOT IN (SELECT k FROM db.keys_int);") == ids_t{1, 3});
    CHECK(ids("SELECT id FROM db.t WHERE v > ALL (SELECT k FROM db.keys_int);").empty());
    CHECK(ids("SELECT id FROM db.t WHERE v < ANY (SELECT k FROM db.keys_int);") == ids_t{1, 2, 3});
    INFO("a NULL element turns every non-member into UNKNOWN");
    CHECK(ids("SELECT id FROM db.t WHERE v IN (SELECT k FROM db.keys_null);") == ids_t{2});
    CHECK(ids("SELECT id FROM db.t WHERE v NOT IN (SELECT k FROM db.keys_null);").empty());
    CHECK(ids("SELECT id FROM db.t WHERE v <> ALL (SELECT k FROM db.keys_null);").empty());
}

// Every NaN is the same value to PostgreSQL, whatever its sign and payload bits: hashing must not tell them apart
TEST_CASE("integration::cpp::test_subqueries::nan_bit_patterns_are_one_value") {
    auto config = test_create_config(integration_fixture_path("test_subqueries/nan_bit_patterns_are_one_value"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto ids = [&](const std::string& sql) {
        auto cur = run(sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return ids_of(cur);
    };

    REQUIRE(run("CREATE DATABASE db;")->is_success());
    REQUIRE(run("CREATE TABLE db.t (id bigint, f double);")->is_success());
    REQUIRE(run("INSERT INTO db.t (id, f) VALUES (1, CAST('NaN' AS double)), (2, CAST('-NaN' AS double)), "
                "(3, 1.5), (4, CAST('NaN' AS double)), (5, CAST('-NaN' AS double));")
                ->is_success());
    REQUIRE(run("CREATE TABLE db.s (f double);")->is_success());
    REQUIRE(run("INSERT INTO db.s (f) VALUES (CAST('NaN' AS double));")->is_success());
    REQUIRE(run("CREATE TABLE db.n (f double);")->is_success());
    REQUIRE(run("INSERT INTO db.n (f) VALUES (CAST('-NaN' AS double));")->is_success());

    INFO("IN: a NaN of either sign is in a set holding one NaN");
    CHECK(ids("SELECT id FROM db.t WHERE f IN (SELECT f FROM db.s);") == ids_t{1, 2, 4, 5});
    CHECK(ids("SELECT id FROM db.t WHERE f IN (SELECT f FROM db.n);") == ids_t{1, 2, 4, 5});
    CHECK(ids("SELECT id FROM db.t WHERE f NOT IN (SELECT f FROM db.s);") == ids_t{3});
    CHECK(ids("SELECT id FROM db.t WHERE f <> ANY (SELECT f FROM db.n);") == ids_t{3});
    CHECK(ids("SELECT id FROM db.t WHERE f = ALL (SELECT f FROM db.s);") == ids_t{1, 2, 4, 5});
    INFO("NaN is the greatest value");
    CHECK(ids("SELECT id FROM db.t WHERE f >= ANY (SELECT f FROM db.n);") == ids_t{1, 2, 4, 5});
    CHECK(ids("SELECT id FROM db.t WHERE f < ANY (SELECT f FROM db.n);") == ids_t{3});
    CHECK(ids("SELECT id FROM db.t WHERE f > ALL (SELECT f FROM db.s);").empty());

    INFO("GROUP BY: one group for all NaNs");
    {
        auto cur = run("SELECT f, COUNT(*) FROM db.t GROUP BY f;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    INFO("DISTINCT: one row for all NaNs");
    {
        auto cur = run("SELECT DISTINCT f FROM db.t;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    INFO("hash JOIN: NaN joins -NaN");
    {
        auto cur = run("SELECT t.id FROM db.t JOIN db.n ON t.f = n.f;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 4);
    }
}
