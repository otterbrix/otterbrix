#include "test_config.hpp"
#include <catch2/catch.hpp>

// Departments:  (id, name,          budget)
//               (1,  'Engineering', 100000)
//               (2,  'Marketing',    50000)
//               (3,  'HR',           30000)
//               (4,  'Sales',        80000)
//               (5,  'Finance',      70000)
//
// Employees:    (id,  name,      dept_id,  salary)
//               (1,   'Alice',   1,        90000)
//               (2,   'Bob',     1,        80000)
//               (3,   'Charlie', 2,        60000)
//               (4,   'Diana',   2,        55000)
//               (5,   'Eve',     3,        45000)
//               (6,   'Frank',   3,        40000)
//               (7,   'Grace',   4,        70000)
//               (8,   'Henry',   4,        65000)
//               (9,   'Iris',    5,        75000)
//               (10,  'Jack',    5,        72000)
//
// Derived constants used across tests:
//   overall avg salary   = 65200
//   overall avg budget   = 66000
//   dept1 avg salary     = 85000   (Alice 90k, Bob 80k)
//   dept2 avg salary     = 57500   (Charlie 60k, Diana 55k)
//   dept3 avg salary     = 42500   (Eve 45k, Frank 40k)
//   dept4 avg salary     = 67500   (Grace 70k, Henry 65k)
//   dept5 avg salary     = 73500   (Iris 75k, Jack 72k)
//   high-budget depts    = {1,4,5} (budget > 60000)
//   above-avg employees  = {Alice,Bob,Grace,Iris,Jack} (salary > 65200)

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
            REQUIRE(cur->size() == 5);
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
            REQUIRE(cur->size() == 10);
        }
    }

} // namespace

// ---------------------------------------------------------------------------
// Subqueries in WHERE
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::where_clause") {
    auto config = test_create_config("/tmp/test_subqueries/where_clause");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("scalar subquery in WHERE with equality") {
        // Highest-paid employee: Alice (salary 90000)
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary = (SELECT MAX(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Alice");
    }

    INFO("scalar subquery in WHERE with greater-than") {
        // Employees above overall average (65200): Alice, Bob, Grace, Iris, Jack
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("scalar subquery in WHERE with less-than against aggregated outer") {
        // Min budget of high-budget depts (budget > 60000) is Finance's 70000;
        // employees with salary < 70000: Charlie, Diana, Eve, Frank, Henry → 5
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE salary < ("
                                           "  SELECT MIN(budget) FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("IN subquery") {
        // High-budget departments (budget > 60000): Engineering(1), Sales(4), Finance(5)
        // Employees in those 3 departments: Alice, Bob, Grace, Henry, Iris, Jack → 6
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
    }

    INFO("NOT IN subquery") {
        // Low-budget departments: Marketing(2), HR(3)
        // Employees in those departments: Charlie, Diana, Eve, Frank → 4
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees "
                                           "WHERE dept_id NOT IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget > 60000"
                                           ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("EXISTS non-correlated subquery — rows found") {
        // Subquery finds employees earning > 85000 (Alice = 90000) → EXISTS is true for every outer row
        // All 5 departments are returned
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE EXISTS (SELECT 1 FROM TestDatabase.Employees WHERE salary > 85000);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("EXISTS non-correlated subquery — no rows found") {
        // Subquery finds no employee earning > 999999 → EXISTS is false for every outer row
        // 0 departments are returned
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT name FROM TestDatabase.Departments "
                                    "WHERE EXISTS (SELECT 1 FROM TestDatabase.Employees WHERE salary > 999999);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("NOT EXISTS non-correlated subquery — subquery empty") {
        // Subquery returns no rows → NOT EXISTS is true for every outer row
        // All 5 departments are returned
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
    INFO("EXISTS correlated subquery") {
        // Departments that have at least one employee earning > 85000
        // Only Engineering (Alice = 90000) → 1 row
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name FROM TestDatabase.Departments d "
            "WHERE EXISTS ("
            "  SELECT 1 FROM TestDatabase.Employees e "
            "  WHERE e.dept_id = d.id AND e.salary > 85000"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Engineering");
    }

    INFO("NOT EXISTS correlated subquery") {
        // Departments where no employee earns > 50000
        // HR: Eve(45000), Frank(40000) — neither exceeds 50000 → 1 row
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name FROM TestDatabase.Departments d "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM TestDatabase.Employees e "
            "  WHERE e.dept_id = d.id AND e.salary > 50000"
            ");");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "HR");
    }

    INFO("correlated subquery comparing to own-department average") {
        // Employees earning above their own department's average: one per department → 5
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

    INFO("ANY subquery") {
        // Departments with budget greater than at least one employee salary
        // (budget > ANY salaries) = budget > MIN(salary) = 40000
        // Engineering(100k), Marketing(50k), Sales(80k), Finance(70k) satisfy; HR(30k) does not → 4
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE budget > ANY (SELECT salary FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("ALL subquery") {
        // Departments with budget greater than ALL employee salaries
        // (budget > MAX(salary) = 90000): only Engineering (100000) → 1 row
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Departments "
                                           "WHERE budget > ALL (SELECT salary FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Engineering");
    }

    INFO("scalar subquery returning NULL (empty result)") {
        // Subquery for a non-existent dept returns no rows → NULL comparison → 0 matches
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

// ---------------------------------------------------------------------------
// Subqueries in SELECT list and in FROM (derived tables)
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::select_list_and_from") {
    auto config = test_create_config("/tmp/test_subqueries/select_list_and_from");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    // TODO: those have to be replanned into join
    /*
    INFO("scalar correlated subquery in SELECT list") {
        // For Engineering employees, show their department name via subquery
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e.name, "
            "  (SELECT d.name FROM TestDatabase.Departments d WHERE d.id = e.dept_id) AS dept_name "
            "FROM TestDatabase.Employees e "
            "WHERE e.dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<std::string_view>() == "Engineering");
        REQUIRE(cur->chunk_data().value(1, 1).value<std::string_view>() == "Engineering");
    }

    INFO("aggregate correlated subquery in SELECT list") {
        // Show each department's headcount next to the department row
        // Every department has exactly 2 employees
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT d.name, "
            "  (SELECT COUNT(*) FROM TestDatabase.Employees e WHERE e.dept_id = d.id) AS headcount "
            "FROM TestDatabase.Departments d "
            "ORDER BY d.id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t row = 0; row < 5; ++row) {
            REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == 2);
        }
    }

    INFO("subquery in SELECT list returning maximum of outer group") {
        // Each employee row shows the max salary in their department
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
            "SELECT e.name, "
            "  (SELECT MAX(e2.salary) FROM TestDatabase.Employees e2 WHERE e2.dept_id = e.dept_id) AS dept_max "
            "FROM TestDatabase.Employees e "
            "WHERE e.dept_id = 1 "
            "ORDER BY e.salary DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // dept_max for Engineering = 90000 for both rows
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 90000);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 90000);
    }
    */

    INFO("derived table in FROM (basic)") {
        // Select from a derived table that filters high earners
        // salary > 70000: Alice(90k), Bob(80k), Iris(75k), Jack(72k) → 4 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT name FROM "
            "  (SELECT name, salary FROM TestDatabase.Employees WHERE salary > 70000) AS high_earners;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("derived table in FROM with outer WHERE") {
        // Derived table produces salary > 60000 rows, outer query restricts to dept 1
        // salary > 60000 AND dept_id = 1: Alice(90k), Bob(80k) → 2 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT name, salary FROM "
            "  (SELECT name, salary, dept_id FROM TestDatabase.Employees WHERE salary > 60000) AS mid_up "
            "WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("double-nested derived tables in FROM") {
        // Inner derived table: salary > 60000 → Alice,Bob,Grace,Henry,Iris,Jack (6 rows)
        // Outer derived table: salary < 80000 → Grace(70k),Henry(65k),Iris(75k),Jack(72k) → 4 rows
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

    INFO("derived table aggregated in FROM") {
        // Join the derived per-department stats back to departments
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
        // dept1 avg=85000>70k ✓, dept4 avg=67500<70k ✗, dept5 avg=73500>70k ✓ → 2 rows
        REQUIRE(cur->size() == 2);
    }
}

// ---------------------------------------------------------------------------
// Subqueries in JOIN
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::join") {
    auto config = test_create_config("/tmp/test_subqueries/join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("subquery as right side of JOIN, filter above department average") {
        // One above-average earner per department → 5 rows
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

    INFO("subquery in JOIN ON clause — top earner per department") {
        // Each employee joins the max-salary-per-dept subquery and keeps only the match
        // Top earner per dept: Alice(dept1), Charlie(dept2), Eve(dept3), Grace(dept4), Iris(dept5) → 5
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

    INFO("subquery in JOIN producing multi-column derived table") {
        // Join employees to department stats; select employees whose salary is
        // within 5000 of the department average (both above and below)
        // dept1 avg=85000: Alice(90k, diff=5000 ✓), Bob(80k, diff=5000 ✓)
        // dept2 avg=57500: Charlie(60k, diff=2500 ✓), Diana(55k, diff=2500 ✓)
        // dept3 avg=42500: Eve(45k, diff=2500 ✓), Frank(40k, diff=2500 ✓)
        // dept4 avg=67500: Grace(70k, diff=2500 ✓), Henry(65k, diff=2500 ✓)
        // dept5 avg=73500: Iris(75k, diff=1500 ✓), Jack(72k, diff=1500 ✓)
        // All 10 employees are within 5000 of their dept average
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

// ---------------------------------------------------------------------------
// Subqueries in HAVING
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::having") {
    auto config = test_create_config("/tmp/test_subqueries/having");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("subquery in HAVING comparing to overall average") {
        // Departments whose average salary exceeds the overall average (65200)
        // dept1=85000✓, dept2=57500✗, dept3=42500✗, dept4=67500✓, dept5=73500✓ → 3
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, AVG(salary) AS avg_sal "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING AVG(salary) > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("subquery in HAVING with MIN") {
        // Departments where the minimum salary exceeds the overall average (65200)
        // dept1 min=80000>65200 ✓, dept2 min=55000 ✗, dept3 min=40000 ✗,
        // dept4 min=65000 ✗, dept5 min=72000>65200 ✓ → 2
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING MIN(salary) > (SELECT AVG(salary) FROM TestDatabase.Employees);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("subquery in HAVING comparing to specific department budget") {
        // Departments whose total payroll exceeds the budget of HR (30000)
        // dept1 total=170000>30k ✓, dept2=115000>30k ✓, dept3=85000>30k ✓,
        // dept4=135000>30k ✓, dept5=147000>30k ✓ → all 5
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id, SUM(salary) AS total_payroll "
                                           "FROM TestDatabase.Employees "
                                           "GROUP BY dept_id "
                                           "HAVING SUM(salary) > ("
                                           "  SELECT budget FROM TestDatabase.Departments WHERE name = 'HR'"
                                           ");");
        REQUIRE(cur->is_success());
        // Every dept's total payroll (≥85000) > HR budget (30000) → all 5
        REQUIRE(cur->size() == 5);
    }
}

// ---------------------------------------------------------------------------
// Deeply nested subqueries (3, 4, and 5 levels)
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::nested") {
    auto config = test_create_config("/tmp/test_subqueries/nested");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("3-level nested: scalar in scalar in IN") {
        // Find employees whose salary exceeds the average salary of employees
        // in high-budget departments (budget > 60000).
        // High-budget depts: {1,4,5}; their employees avg: (90k+80k+70k+65k+75k+72k)/6 = 75333
        // Employees with salary > 75333: Alice(90k), Bob(80k) → 2
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
    INFO("3-level nested: EXISTS inside IN inside scalar") {
        // Find the maximum salary among employees who work in a department
        // that has at least one peer earning less than the overall average.
        // Overall avg = 65200.
        // Depts with at least one employee earning < 65200:
        //   dept1: Bob(80k)? No, Bob > 65200. Alice(90k)? No. So dept1 has no one below avg. Wait:
        //   dept1: both Alice(90k) and Bob(80k) > 65200 → dept1 NOT included
        //   dept2: Charlie(60k) < 65200 ✓ → dept2 included
        //   dept3: Eve(45k) < 65200 ✓ → dept3 included
        //   dept4: Henry(65k) < 65200 ✓ → dept4 included
        //   dept5: Jack(72k) > 65200, but no... wait: Jack=72000 > 65200, Iris=75000 > 65200
        //          dept5: both above → not included
        // So depts with someone below avg: {2,3,4}
        // Employees in those depts: Charlie(60k),Diana(55k),Eve(45k),Frank(40k),Grace(70k),Henry(65k)
        // MAX of their salaries = 70000 (Grace)
        // Employees with salary = 70000: Grace → 1 row
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Grace");
    }

    INFO("4-level nested: top earner in each of the best departments") {
        // Find employees whose salary equals the maximum salary within their department,
        // and whose department has a budget above the median (average) budget.
        // Avg budget = 66000; depts above 66000: Engineering(100k), Sales(80k), Finance(70k) → {1,4,5}
        // Top earner per dept: Alice(dept1,90k), Grace(dept4,70k), Iris(dept5,75k) → 3
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

    INFO("4-level nested: IN in IN in scalar in scalar") {
        // Employees in departments whose budget exceeds the average budget
        // of departments that contain above-average earners.
        // Overall avg salary = 65200.
        // Above-avg earners: Alice(1),Bob(1),Grace(4),Iris(5),Jack(5) → dept_ids {1,4,5}
        // Avg budget of depts {1,4,5}: (100k+80k+70k)/3 = 83333
        // Depts with budget > 83333: Engineering(100k) → id=1
        // Employees in dept 1: Alice, Bob → 2
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

    INFO("5-level nested: scalar chain through both tables") {
        // Find employees who work in the department with the single highest budget
        // among departments whose budget exceeds the average budget of departments
        // that contain at least one above-average earner.
        //
        // L5: AVG(salary) = 65200
        // L4: dept_ids with salary > 65200 → {1,4,5}
        // L3: AVG budget of depts {1,4,5} = 83333
        // L2: MAX budget of depts with budget > 83333 → 100000 (Engineering)
        // L1: dept id with budget = 100000 → 1
        // Main: employees in dept 1: Alice, Bob → 2
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

    INFO("5-level nested: second-highest budget department via subquery chain") {
        // Find employees earning above the average salary in the department
        // with the second-highest budget.
        //
        // L5: DISTINCT dept_ids of all employees → {1,2,3,4,5}
        // L4: MAX(budget) of all departments → 100000
        // L3: MAX(budget) where budget < 100000 → 80000 (Sales)
        // L2: id of department with budget = 80000 → 4
        // L1: AVG(salary) of employees in dept 4: (70000+65000)/2 = 67500
        // Main: employees with salary > 67500: Alice(90k),Bob(80k),Grace(70k),Iris(75k),Jack(72k) → 5
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

// ---------------------------------------------------------------------------
// Subqueries in DML: INSERT SELECT, DELETE WHERE, UPDATE WHERE
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::dml") {
    auto config = test_create_config("/tmp/test_subqueries/dml");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("INSERT SELECT — copy high earners to new table") {
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TopEarners (name string, salary bigint);");
            REQUIRE(cur->is_success());
        }
        {
            // salary > 70000: Alice(90k), Bob(80k), Iris(75k), Jack(72k) → 4 rows
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TopEarners (name, salary) "
                                               "SELECT name, salary FROM TestDatabase.Employees WHERE salary > 70000;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT name FROM TestDatabase.TopEarners;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 4);
        }
    }

    INFO("INSERT SELECT with ORDER BY") {
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
            // Top 3: Alice(90k), Bob(80k), Iris(75k)
            REQUIRE(cur->size() == 3);
        }
    }

    INFO("DELETE WHERE IN subquery") {
        // Remove employees in the lowest-budget department (HR, budget=30000)
        // HR employees: Eve(5), Frank(6) → 2 rows deleted
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
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*) AS cnt FROM TestDatabase.Employees;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 8);
        }
    }

    INFO("UPDATE WHERE scalar subquery") {
        // Promote below-average earners in high-budget departments to the dept average
        // High-budget depts: {1,4,5}; below-avg-in-dept in those depts:
        //   dept1: Bob(80k < 85k avg) → update to 85000
        //   dept4: Henry(65k < 67.5k avg) → update to 67500
        //   dept5: Jack(72k < 73.5k avg) → update to 73500
        // But we implement a simpler variant: set salary = 70000 for employees
        // in low-budget depts (budget < 40000) → HR employees (Eve, Frank, but deleted above)
        // So after the DELETE above, no HR employees remain; let's update Marketing employees instead.
        // Marketing employees: Charlie(60k), Diana(55k); set salary to overall avg floor
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Employees SET salary = 65200 "
                                           "WHERE dept_id IN ("
                                           "  SELECT id FROM TestDatabase.Departments WHERE budget = 50000"
                                           ");");
        REQUIRE(cur->is_success());
        // Marketing dept: Charlie and Diana → 2 rows updated
        REQUIRE(cur->size() == 2);
    }

    INFO("DELETE WHERE NOT IN subquery") {
        // Keep only employees in Engineering (highest budget dept)
        // and remove the rest
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
            // After previous DELETE (HR removed) and this DELETE (non-Engineering removed):
            // dept2 (Marketing): Charlie, Diana  → 2
            // dept4 (Sales):     Grace, Henry    → 2
            // dept5 (Finance):   Iris, Jack      → 2
            // Total deleted: 6
            REQUIRE(cur->size() == 6);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(*) AS cnt FROM TestDatabase.Employees;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            // Only Alice and Bob remain (dept 1 = Engineering)
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 2);
        }
    }
}

// ---------------------------------------------------------------------------
// Common Table Expressions (WITH clause)
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::cte") {
    auto config = test_create_config("/tmp/test_subqueries/cte");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("simple CTE used in SELECT") {
        // CTE filters above-average earners, outer query counts them
        // Above overall avg (65200): Alice, Bob, Grace, Iris, Jack → 5
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Alice");
    }

    INFO("CTE joined with base table") {
        // CTE produces per-department stats; join with Departments to show names
        // All 5 departments have stats → 5 rows
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
        // Highest avg: Engineering (85000)
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Engineering");
    }

    INFO("multiple CTEs chained") {
        // CTE1: above-average earners (salary > 65200): Alice,Bob,Grace,Iris,Jack (5)
        // CTE2: high-budget department ids (budget > 60000): {1,4,5} (3 depts)
        // Final: above_avg employees whose dept is in high_budget_depts:
        //   Alice(dept1✓), Bob(dept1✓), Grace(dept4✓), Iris(dept5✓), Jack(dept5✓) → 5
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

    INFO("CTE used twice in the same query") {
        // CTE defines top-earner per dept; join it with itself to find departments
        // whose top earner salary equals the overall maximum top-earner salary.
        // top earners: Alice(90k), Charlie(60k), Eve(45k), Grace(70k), Iris(75k)
        // max of those = 90000 → only Alice's department (Engineering)
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Engineering");
    }

    INFO("CTE with subquery in its own WHERE clause") {
        // CTE selects employees earning above the budget of the cheapest department
        // MIN budget = HR = 30000; salary > 30000: all 10 employees
        // Outer query counts per department
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
        // All 5 departments represented, each with 2 employees
        REQUIRE(cur->size() == 5);
        for (size_t row = 0; row < 5; ++row) {
            REQUIRE(cur->chunk_data().value(1, row).value<int64_t>() == 2);
        }
    }
}

// ---------------------------------------------------------------------------
// UNION / UNION ALL
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::union") {
    auto config = test_create_config("/tmp/test_subqueries/union");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_subquery_db(dispatcher); }

    INFO("UNION ALL preserves duplicates") {
        // Both sides select dept_id=1; UNION ALL keeps all 4 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        for (size_t row = 0; row < 4; ++row) {
            REQUIRE(cur->chunk_data().value(0, row).value<int64_t>() == 1);
        }
    }

    INFO("UNION ALL disjoint sets") {
        // dept_id=1: Alice,Bob. dept_id=2: Charlie,Diana. No overlap → 4 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT name FROM TestDatabase.Employees WHERE dept_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("UNION distinct removes duplicates") {
        // Both sides return dept_ids of high-salary employees:
        // salary >= 80000: Alice(90k,dept1), Bob(80k,dept1) → {1,1}
        // salary >= 70000: Alice, Bob, Grace(70k,dept4), Iris(75k,dept5), Jack(72k,dept5) → {1,1,4,5,5}
        // UNION distinct: {1,4,5} → 3 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE salary >= 80000 "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE salary >= 70000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("UNION distinct same values on both sides") {
        // dept_id=1 on left, dept_id=1 on right → only one unique value
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION "
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
    }

    INFO("UNION ALL three operands") {
        // A UNION ALL B UNION ALL C: dept 1 (2 rows) + dept 2 (2 rows) + dept 3 (2 rows) = 6
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

    INFO("UNION schema mismatch rejected") {
        // Different column counts: error expected
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT dept_id FROM TestDatabase.Employees WHERE dept_id = 1 "
                                           "UNION ALL "
                                           "SELECT dept_id, salary FROM TestDatabase.Employees WHERE dept_id = 1;");
        REQUIRE_FALSE(cur->is_success());
    }
}

// ---------------------------------------------------------------------------
// UNION with structured / array columns
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_subqueries::union_complex_types") {
    auto config = test_create_config("/tmp/test_subqueries/union_complex_types");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") {
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
        // ShapeA: 3 rows
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.ShapeA (id, pt, tags) VALUES "
                                               "(1, ROW(0, 0), ARRAY[1,2,3]), "
                                               "(2, ROW(1, 1), ARRAY[4,5,6]), "
                                               "(3, ROW(2, 2), ARRAY[7,8,9]);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        // ShapeB: 2 rows — row id=1 duplicates ShapeA's first row exactly
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.ShapeB (id, pt, tags) VALUES "
                                               "(1, ROW(0, 0), ARRAY[1,2,3]), "
                                               "(4, ROW(3, 3), ARRAY[10,11,12]);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }

    INFO("UNION ALL id column") {
        // ids from A: {1,2,3}; ids from B: {1,4}; UNION ALL = 5 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("UNION ALL with UDT and array columns") {
        // All 5 rows: 3 from A + 2 from B
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id, pt, tags FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id, pt, tags FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("UNION distinct id column removes duplicates") {
        // ids from A: {1,2,3}; ids from B: {1,4}; distinct = {1,2,3,4} = 4
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION "
                                           "SELECT id FROM TestDatabase.ShapeB;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    INFO("UNION schema mismatch rejected") {
        // Different column counts — error expected
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT id FROM TestDatabase.ShapeA "
                                           "UNION ALL "
                                           "SELECT id, pt FROM TestDatabase.ShapeB;");
        REQUIRE_FALSE(cur->is_success());
    }
}

// ---------------------------------------------------------------------------
// Recursive Common Table Expressions (WITH RECURSIVE)
// ---------------------------------------------------------------------------
//
// OrgChart: (id, name,        manager_id)
//           (1,  'CEO',       0)          -- root: manager_id=0 means no manager
//           (2,  'VP Eng',    1)
//           (3,  'VP Mkt',    1)
//           (4,  'Engineer',  2)
//           (5,  'Designer',  3)
//
// Hierarchy (depth):
//   depth 0: CEO
//   depth 1: VP Eng, VP Mkt
//   depth 2: Engineer, Designer

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
            REQUIRE(cur->size() == 5);
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_subqueries::recursive_cte") {
    auto config = test_create_config("/tmp/test_subqueries/recursive_cte");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup") { setup_recursive_db(dispatcher); }

    INFO("full hierarchy traversal") {
        // Starting from root (manager_id=0), traverse all 5 nodes
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "CEO");
        REQUIRE(cur->chunk_data().value(0, 1).value<std::string_view>() == "VP Eng");
        REQUIRE(cur->chunk_data().value(0, 4).value<std::string_view>() == "Designer");
    }

    INFO("subtree rooted at VP Eng") {
        // Starting from VP Eng (id=2), traverse only her subtree: VP Eng + Engineer
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "VP Eng");
        REQUIRE(cur->chunk_data().value(0, 1).value<std::string_view>() == "Engineer");
    }

    INFO("hierarchy with depth") {
        // Carry depth through recursion: depth 0 at root, +1 each level
        // CEO=0, VP Eng=1, VP Mkt=1, Engineer=2, Designer=2
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
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 0); // CEO
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 1); // VP Eng
        REQUIRE(cur->chunk_data().value(1, 3).value<int64_t>() == 2); // Engineer
    }

    INFO("filter by depth in outer query") {
        // Only depth-2 employees (Engineer, Designer) via outer WHERE on depth
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
        REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Engineer");
        REQUIRE(cur->chunk_data().value(0, 1).value<std::string_view>() == "Designer");
    }
}