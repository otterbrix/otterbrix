#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/explain/explain_plan.hpp>
#include <services/collection/explain/explain_renderer.hpp>

#include <string>
#include <string_view>

using namespace components;
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

    bool contains(const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; }

    // A single-row cursor with `marker` in the QUERY PLAN column, standing in for a host renderer's output.
    cursor_t_ptr marker_cursor(std::pmr::memory_resource* mr, std::string_view marker) {
        std::pmr::vector<types::complex_logical_type> types(mr);
        types.emplace_back(types::logical_type::STRING_LITERAL, "QUERY PLAN");
        vector::data_chunk_t chunk(mr, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, marker);
        return make_cursor(mr, std::move(chunk));
    }

    cursor_t_ptr fake_render(std::pmr::memory_resource* mr,
                             const services::collection::explain_plan_node& /*root*/,
                             bool /*analyze*/) {
        return marker_cursor(mr, std::string_view("FAKE-RENDERER"));
    }
    // Marker is not a substring of "FAKE-RENDERER" either way, so a distinctness assertion can tell them apart.
    cursor_t_ptr fake_render_2(std::pmr::memory_resource* mr,
                               const services::collection::explain_plan_node& /*root*/,
                               bool /*analyze*/) {
        return marker_cursor(mr, std::string_view("FAKE-SPARK"));
    }
    cursor_t_ptr fake_render_analyze(std::pmr::memory_resource* mr,
                                     const services::collection::explain_plan_node& /*root*/,
                                     bool analyze) {
        return marker_cursor(mr, std::string_view(analyze ? "FAKE-ANALYZE-ON" : "FAKE-ANALYZE-OFF"));
    }

} // namespace

TEST_CASE("integration::cpp::test_explain::sql") {
    auto config = test_create_config(integration_fixture_path("test_explain/sql"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.customer(id int, name string);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.customer (id, name) VALUES (10,'a'),(20,'b');")
                    ->is_success());
    }

    INFO("EXPLAIN SELECT: single QUERY PLAN column, scans the table");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->chunks().front().data[0].type().alias() == "QUERY PLAN");
        REQUIRE(cur->size() > 0);
        REQUIRE(contains(plan_text(cur), "orders"));
    }

    INFO("EXPLAIN SELECT with JOIN renders both scanned relations");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE reports actual per-operator stats");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "actual time"));
        REQUIRE(contains(t, "rows="));
        REQUIRE(contains(t, "loops="));
    }

    INFO("plan-only EXPLAIN INSERT does NOT change the table");
    {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, "EXPLAIN INSERT INTO TestDatabase.orders (id, cust) VALUES (99, 99);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
        }
    }

    INFO("EXPLAIN ANALYZE INSERT executes and commits (PostgreSQL-compatible)");
    {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s, "EXPLAIN ANALYZE INSERT INTO TestDatabase.orders (id, cust) VALUES (99, 99);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == 4);
        }
    }

    INFO("EXPLAIN (ANALYZE false/off/0) is plan-only — the inner DML must NOT execute");
    {
        for (const char* q : {"EXPLAIN (ANALYZE false) INSERT INTO TestDatabase.orders (id, cust) VALUES (77, 77);",
                              "EXPLAIN (ANALYZE off) INSERT INTO TestDatabase.orders (id, cust) VALUES (66, 66);",
                              "EXPLAIN (ANALYZE 0) INSERT INTO TestDatabase.orders (id, cust) VALUES (55, 55);"}) {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, q)->is_success());
        }
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 4);
    }

    INFO("EXPLAIN (ANALYZE true) DOES execute the inner DML (positive control)");
    {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(
                dispatcher
                    ->execute_sql(s,
                                  "EXPLAIN (ANALYZE true) INSERT INTO TestDatabase.orders (id, cust) VALUES (88, 88);")
                    ->is_success());
        }
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5);
    }

    INFO("plan-only EXPLAIN does NOT execute an uncorrelated sub-query");
    {
        // The sub-query returns 2 rows; if plan-only EXPLAIN executed it, compacting to a scalar would error.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN SELECT * FROM TestDatabase.orders WHERE id = (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "orders"));
    }

    INFO("EXPLAIN of an unsupported (DDL) inner statement is rejected");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN CREATE TABLE TestDatabase.foo(x int);");
        REQUIRE(cur->is_error());
    }

    INFO("host customization: set_explain_renderer swaps output, SQL unchanged");
    {
        // Slot 0 is the default a plain EXPLAIN (render_id == 0) selects.
        REQUIRE_FALSE(dispatcher->set_explain_renderer(0, &fake_render).contains_error());
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
    }
}

TEST_CASE("integration::cpp::test_explain::inline_subquery_initplan") {
    auto config = test_create_config(integration_fixture_path("test_explain/inline_subquery_initplan"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.customer(id int, name string);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.customer (id, name) VALUES (10,'a'),(20,'b');")
                    ->is_success());
    }

    INFO("EXPLAIN ANALYZE: scalar WHERE sub-query renders an InitPlan with the sub-scan + stats");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust = (SELECT "
                                           "max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
        REQUIRE(contains(t, "actual time"));
    }

    INFO("EXPLAIN ANALYZE: IN (SELECT ...) renders an InitPlan");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust IN (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    // Attach-at-root is operator-independent: these three carriers (EXISTS, HAVING, JOIN-ON) motivated the rewrite.
    INFO("EXPLAIN ANALYZE: EXISTS (...) renders an InitPlan (carrier: operator_match)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE EXISTS (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE: HAVING sub-query renders an InitPlan (carrier: operator_group / aggregate)");
    {
        auto s = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s,
                                    "EXPLAIN ANALYZE SELECT cust, count(*) FROM TestDatabase.orders "
                                    "GROUP BY cust HAVING count(*) > (SELECT min(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE: JOIN-ON sub-query renders an InitPlan (carrier: operator_join)");
    {
        auto s = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s,
                                    "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c "
                                    "ON o.cust = c.id AND c.id = (SELECT max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
    }

    INFO("EXPLAIN ANALYZE: two sub-queries render two globally-numbered InitPlans");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders "
                                           "WHERE cust = (SELECT max(id) FROM TestDatabase.customer) "
                                           "AND id = (SELECT min(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "InitPlan 2 (returns $"));
    }

    INFO("EXPLAIN ANALYZE: nested sub-query — both InitPlans present (all flattened top-level)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust IN "
            "(SELECT id FROM TestDatabase.customer WHERE id = (SELECT max(id) FROM TestDatabase.customer));");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "InitPlan 2 (returns $"));
    }

    INFO("plain EXPLAIN (not ANALYZE) shows the sub-query InitPlan STRUCTURE (PostgreSQL), without stats");
    {
        // Each flattened sub-query's IR is captured but never executed -- hence no actual time/rows/loops shown.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN SELECT * FROM TestDatabase.orders WHERE cust = (SELECT max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
        REQUIRE_FALSE(contains(t, "actual time"));
    }
}

TEST_CASE("integration::cpp::test_explain::per_query_renderer") {
    auto config = test_create_config(integration_fixture_path("test_explain/per_query_renderer"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10);")
                    ->is_success());
    }

    REQUIRE_FALSE(dispatcher->set_explain_renderer(1, &fake_render).contains_error());
    REQUIRE_FALSE(dispatcher->set_explain_renderer(2, &fake_render_2).contains_error());

    INFO("per-query selection: id 1 -> fake, id 0 -> postgres default");
    {
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
            REQUIRE(cur->is_success());
            REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 0);
            REQUIRE(cur->is_success());
            const auto t = plan_text(cur);
            REQUIRE(contains(t, "orders"));
            REQUIRE_FALSE(contains(t, "FAKE-RENDERER"));
        }
    }

    INFO("per-query, NOT global: interleaved ids each pick their own renderer");
    {
        auto s1 = otterbrix::session_id_t();
        auto c1 = dispatcher->execute_sql(s1, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
        auto s0 = otterbrix::session_id_t();
        auto c0 = dispatcher->execute_sql(s0, "EXPLAIN SELECT * FROM TestDatabase.orders;", 0);
        REQUIRE(contains(plan_text(c1), "FAKE-RENDERER"));
        REQUIRE_FALSE(contains(plan_text(c0), "FAKE-RENDERER"));
    }

    INFO("multiple renderers registered simultaneously: id 1 and id 2 are distinct");
    {
        auto s1 = otterbrix::session_id_t();
        auto c1 = dispatcher->execute_sql(s1, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
        auto s2 = otterbrix::session_id_t();
        auto c2 = dispatcher->execute_sql(s2, "EXPLAIN SELECT * FROM TestDatabase.orders;", 2);
        const auto t1 = plan_text(c1);
        const auto t2 = plan_text(c2);
        REQUIRE(contains(t1, "FAKE-RENDERER"));
        REQUIRE_FALSE(contains(t1, "FAKE-SPARK"));
        REQUIRE(contains(t2, "FAKE-SPARK"));
        REQUIRE_FALSE(contains(t2, "FAKE-RENDERER"));
    }

    INFO("out-of-range id resolves to the built-in default (a default, not a fallback branch)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 999);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE_FALSE(contains(t, "FAKE-RENDERER"));
    }

    INFO("EXPLAIN ANALYZE via a custom renderer sees analyze == true");
    {
        REQUIRE_FALSE(dispatcher->set_explain_renderer(3, &fake_render_analyze).contains_error());
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders;", 3);
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "FAKE-ANALYZE-ON"));
    }

    INFO("registration fan-out reaches every pooled executor");
    {
        for (int i = 0; i < 12; ++i) {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
            REQUIRE(cur->is_success());
            REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
        }
    }
}

TEST_CASE("integration::cpp::test_explain::renderer_registration_edges") {
    auto config = test_create_config(integration_fixture_path("test_explain/renderer_registration_edges"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20);")
                    ->is_success());
    }

    INFO("out-of-range registration id is rejected, not an unbounded allocation");
    {
        // A huge id must be rejected, not grow the registry to gigabytes (which, with exceptions off, would abort).
        REQUIRE(dispatcher->set_explain_renderer(4000000000u, &fake_render).contains_error());
    }

    INFO("a null renderer is rejected (reported failure, not silent success)");
    {
        REQUIRE(dispatcher->set_explain_renderer(5, nullptr).contains_error());
    }

    INFO("out-of-range render_id resolves to slot 0 — the host's default, not the built-in");
    {
        REQUIRE_FALSE(dispatcher->set_explain_renderer(0, &fake_render_2).contains_error());
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 999);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "FAKE-SPARK"));
        REQUIRE_FALSE(contains(t, "orders"));
    }

    INFO("execute_sql_with_params honors render_id (previously dropped)");
    {
        REQUIRE_FALSE(dispatcher->set_explain_renderer(1, &fake_render).contains_error());
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql_with_params(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", {}, 1);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "FAKE-RENDERER"));
        REQUIRE_FALSE(contains(t, "FAKE-SPARK"));
    }
}

TEST_CASE("integration::cpp::test_explain::analyze_recursive_cte_rows") {
    auto config = test_create_config(integration_fixture_path("test_explain/analyze_recursive_cte_rows"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.org(id int, name string, manager_id int);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "INSERT INTO TestDatabase.org (id, name, manager_id) VALUES "
                                  "(1,'CEO',0),(2,'VP Eng',1),(3,'VP Mkt',1),(4,'Engineer',2),(5,'Designer',3);")
                    ->is_success());
    }

    INFO("EXPLAIN ANALYZE records the recursive-CTE producer's rows (regression: was rows=0)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE WITH RECURSIVE hierarchy AS ("
            "  SELECT id, name FROM TestDatabase.org WHERE manager_id = 0 "
            "  UNION ALL "
            "  SELECT e.id, e.name FROM TestDatabase.org e JOIN hierarchy h ON e.manager_id = h.id"
            ") SELECT name FROM hierarchy;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Recursive Union"));
        REQUIRE(contains(t, "CTE Scan"));
        // Isolated because before the fix this line alone read rows=0 -- record_analyze was never called on it.
        const auto pos = t.find("Recursive Union");
        const auto eol = t.find('\n', pos);
        const std::string ru_line = t.substr(pos, eol - pos);
        REQUIRE_FALSE(contains(ru_line, "rows=0"));
    }
}

TEST_CASE("integration::cpp::test_explain::analyze_per_loop_rows_round") {
    // Rounds per-loop rows like PostgreSQL's rint(); uses a monotonic arena, never std::pmr::get_default_resource().
    std::pmr::monotonic_buffer_resource pool{std::pmr::new_delete_resource()};
    auto* mr = &pool;

    INFO("5 rows / 3 loops rounds to 2 (truncation gave 1)");
    {
        services::collection::explain_plan_node node(mr);
        node.type = components::operators::operator_type::full_scan; // renders "Seq Scan"
        node.rows = 5;
        node.loops = 3;
        node.time = std::chrono::nanoseconds(3'000'000);
        auto cur = services::collection::render_postgres(mr, node, /*analyze=*/true);
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "rows=2"));
    }

    INFO("2 rows / 3 loops rounds to 1 (truncation gave 0)");
    {
        services::collection::explain_plan_node node(mr);
        node.type = components::operators::operator_type::full_scan;
        node.rows = 2;
        node.loops = 3;
        node.time = std::chrono::nanoseconds(1'000'000);
        auto cur = services::collection::render_postgres(mr, node, true);
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "rows=1"));
    }
}

// operator_limit renders "Limit" as the outermost node exactly when the LIMIT/OFFSET window is effective.
TEST_CASE("integration::cpp::test_explain::limit_node_when_effective") {
    auto config = test_create_config(integration_fixture_path("test_explain/limit_node_when_effective"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10),(4,30);")
                ->is_success());
    }

    INFO("EXPLAIN of an effectively-limited SELECT shows a Limit node ABOVE the scan");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders LIMIT 2;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Limit"));
        REQUIRE(contains(t, "orders"));
        REQUIRE(t.find("Limit") < t.find("orders"));
    }

    INFO("EXPLAIN of DISTINCT ... LIMIT shows a Limit node (above the DISTINCT/scan)");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT DISTINCT cust FROM TestDatabase.orders LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "Limit"));
    }

    INFO("EXPLAIN of GROUP BY ... LIMIT shows a Limit node (above the aggregate/scan)");
    {
        auto s = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(s, "EXPLAIN SELECT cust, count(*) FROM TestDatabase.orders GROUP BY cust LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "Limit"));
    }

    INFO("EXPLAIN with NO limit clause shows NO Limit node");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE_FALSE(contains(t, "Limit"));
    }

    INFO("EXPLAIN of LIMIT ALL (an ineffective window) shows NO Limit node");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders LIMIT ALL;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE_FALSE(contains(t, "Limit"));
    }
}

TEST_CASE("integration::cpp::test_explain::having_node_labeled") {
    auto config = test_create_config(integration_fixture_path("test_explain/having_node_labeled"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10),(4,30);")
                ->is_success());
    }

    // HAVING lowers to a dedicated operator_having_t, rendered "Having" -- distinct from a WHERE "Filter".
    INFO("EXPLAIN of GROUP BY ... HAVING shows a Having node above the Aggregate");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN SELECT cust, count(*) FROM TestDatabase.orders GROUP BY cust HAVING count(*) > 1;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Having"));
        REQUIRE(contains(t, "Aggregate"));
        REQUIRE(t.find("Having") < t.find("Aggregate"));
    }
}

// One assertion per node label; a mis-tagged operator (DISTINCT once rendered "Filter") is caught here, not shipped.
TEST_CASE("integration::cpp::test_explain::operator_labels") {
    auto config = test_create_config(integration_fixture_path("test_explain/operator_labels"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.orders(id int, cust int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.customer(id int, name string);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20),(3,10);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.customer (id, name) VALUES (10,'a'),(20,'b');")
                    ->is_success());
    }

    auto label_of = [&](const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, sql);
        REQUIRE(cur->is_success());
        return plan_text(cur);
    };

    REQUIRE(contains(label_of("EXPLAIN SELECT * FROM TestDatabase.orders;"), "Seq Scan"));
    // col-vs-col predicates push into the scan (column_column_filter_t), rendering a bare Seq Scan with no Filter.
    REQUIRE(contains(label_of("EXPLAIN SELECT * FROM TestDatabase.orders WHERE id > cust;"), "Seq Scan"));
    REQUIRE(contains(label_of("EXPLAIN SELECT * FROM TestDatabase.orders ORDER BY id;"), "Sort"));
    REQUIRE(contains(label_of("EXPLAIN SELECT id + cust FROM TestDatabase.orders;"), "Project"));
    REQUIRE(
        contains(label_of("EXPLAIN SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;"),
                 "Hash Join"));
    // Non-equi join condition can't hash → Nested Loop.
    REQUIRE(
        contains(label_of("EXPLAIN SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust > c.id;"),
                 "Nested Loop"));
    REQUIRE(contains(label_of("EXPLAIN SELECT id FROM TestDatabase.orders UNION SELECT id FROM TestDatabase.customer;"),
                     "Append"));
    REQUIRE(contains(label_of("EXPLAIN VALUES (1),(2);"), "Values Scan"));
    // Plan-only EXPLAIN of DML does not execute (proven by the EXPLAIN-INSERT-doesn't-run test above).
    REQUIRE(contains(label_of("EXPLAIN INSERT INTO TestDatabase.orders (id, cust) VALUES (9, 9);"), "Insert"));
    REQUIRE(contains(label_of("EXPLAIN UPDATE TestDatabase.orders SET cust = 1 WHERE id = 1;"), "Update"));
    REQUIRE(contains(label_of("EXPLAIN DELETE FROM TestDatabase.orders WHERE id = 1;"), "Delete"));
    REQUIRE(contains(label_of("EXPLAIN SELECT DISTINCT cust FROM TestDatabase.orders;"), "Unique"));
}

// A WHERE conjunct on a name shared by both join sides must reach the correct side's scan, not a residual Filter.
TEST_CASE("integration::cpp::test_explain::join_shared_column_name_pushdown") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_explain/join_shared_col"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    using test_helpers::exec;
    REQUIRE(exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.t1 (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.t2 (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.t1 (id, k) VALUES (5,1),(5,2),(6,1);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.t2 (id, k) VALUES (7,1),(8,2),(7,3);")->is_success());

    const std::string q =
        "SELECT a.id, a.k, b.id, b.k FROM db.t1 a JOIN db.t2 b ON a.k = b.k WHERE a.id = 5 AND b.id = 7";

    INFO("parity: the collision-named filters still return the correct join rows");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, q + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(2, 0).value<int64_t>() == 7);
        REQUIRE(cur->value(3, 0).value<int64_t>() == 1);
    }

    INFO("EXPLAIN: both per-side filters ride the Seq Scans; no residual Filter remains");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN " + q + ";");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Seq Scan on t1"));
        REQUIRE(contains(t, "Seq Scan on t2"));
        const auto join_pos = t.find("Hash Join");
        const auto first_filter = t.find("Filter");
        REQUIRE(join_pos != std::string::npos);
        REQUIRE(first_filter != std::string::npos);
        REQUIRE(join_pos < first_filter);
    }
}

// The optimizer derives t2.k=5 from t1.k=t2.k WHERE t1.k=5 and pushes it below t2's scan; suppressed on a LEFT join.
namespace {
    size_t count_occurrences(const std::string& hay, const std::string& needle) {
        size_t n = 0;
        for (size_t p = hay.find(needle); p != std::string::npos; p = hay.find(needle, p + needle.size())) {
            ++n;
        }
        return n;
    }
} // namespace

TEST_CASE("integration::cpp::test_explain::transitive_equi_predicate_propagation") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_explain/transitive_equi"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    using test_helpers::exec;
    REQUIRE(exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.t1 (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE db.t2 (id2 bigint, k bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.t1 (id, k) VALUES (1,5),(2,5),(3,9);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO db.t2 (id2, k, v) VALUES (10,5,100),(11,5,200),(12,7,300);")->is_success());

    const std::string inner =
        "SELECT t1.id, t2.id2 FROM db.t1 JOIN db.t2 ON t1.k = t2.k WHERE t1.k = 5 ORDER BY t1.id, t2.id2";

    INFO("parity: derived t2.k=5 does not change the result set");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, inner + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 11);
        REQUIRE(cur->value(0, 2).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 2).value<int64_t>() == 10);
        REQUIRE(cur->value(0, 3).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 3).value<int64_t>() == 11);
    }

    INFO("parity: writing the partner predicate explicitly yields the IDENTICAL result");
    {
        const std::string with_partner =
            "SELECT t1.id, t2.id2 FROM db.t1 JOIN db.t2 ON t1.k = t2.k WHERE t1.k = 5 AND t2.k = 5 "
            "ORDER BY t1.id, t2.id2";
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, with_partner + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 3).value<int64_t>() == 11);
    }

    INFO("EXPLAIN (inner): a Filter rides BOTH scans — the derived predicate reached t2");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN " + inner + ";");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Seq Scan on t1"));
        REQUIRE(contains(t, "Seq Scan on t2"));
        REQUIRE(count_occurrences(t, "Seq Scan") == 2);
        REQUIRE(count_occurrences(t, "Filter") == 2);
        REQUIRE(t.find("Hash Join") < t.find("Filter"));
    }

    const std::string outer =
        "SELECT t1.id, t2.id2 FROM db.t1 LEFT JOIN db.t2 ON t1.k = t2.k WHERE t1.k = 5 ORDER BY t1.id, t2.id2";
    INFO("EXPLAIN (LEFT): the derivation is suppressed on the null-padded side — only t1 filtered");
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN " + outer + ";");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "Seq Scan on t1"));
        REQUIRE(contains(t, "Seq Scan on t2"));
        REQUIRE(count_occurrences(t, "Filter") == 1);
    }
}

// A DISTINCT whose projection is a superset of the GROUP BY keys is redundant; drop_redundant_distinct clears it.
TEST_CASE("integration::cpp::test_explain::distinct_under_group_by") {
    auto config = test_create_config(integration_fixture_path("test_explain/distinct_under_group_by"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE TestDatabase;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE TestDatabase.t(a int, b int);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(s, "INSERT INTO TestDatabase.t (a, b) VALUES (1,10),(1,10),(1,20),(2,10),(2,10);")
                ->is_success());
    }

    INFO("DISTINCT a,b GROUP BY a,b: DISTINCT is redundant -> no Unique, still 3 rows");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(s, "EXPLAIN SELECT DISTINCT a, b FROM TestDatabase.t GROUP BY a, b;");
        REQUIRE(plan->is_success());
        REQUIRE_FALSE(contains(plan_text(plan), "Unique"));

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s2, "SELECT DISTINCT a, b FROM TestDatabase.t GROUP BY a, b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // The extra projected column must be an aggregate here; a bare ungrouped column would be invalid SQL.
    INFO("DISTINCT a, COUNT(*) GROUP BY a: group ⊊ projection -> no Unique, 2 rows");
    {
        auto s = otterbrix::session_id_t();
        auto plan =
            dispatcher->execute_sql(s, "EXPLAIN SELECT DISTINCT a, COUNT(*) AS c FROM TestDatabase.t GROUP BY a;");
        REQUIRE(plan->is_success());
        REQUIRE_FALSE(contains(plan_text(plan), "Unique"));

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s2, "SELECT DISTINCT a, COUNT(*) AS c FROM TestDatabase.t GROUP BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // The trap: groups (1,10) and (1,20) both project a=1, so DISTINCT genuinely removes a duplicate here.
    INFO("TRAP: DISTINCT a GROUP BY a,b: DISTINCT is NOT redundant -> Unique kept, 2 rows");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(s, "EXPLAIN SELECT DISTINCT a FROM TestDatabase.t GROUP BY a, b;");
        REQUIRE(plan->is_success());
        REQUIRE(contains(plan_text(plan), "Unique"));

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s2, "SELECT DISTINCT a FROM TestDatabase.t GROUP BY a, b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        auto s3 = otterbrix::session_id_t();
        auto cur_no_distinct = dispatcher->execute_sql(s3, "SELECT a FROM TestDatabase.t GROUP BY a, b;");
        REQUIRE(cur_no_distinct->is_success());
        REQUIRE(cur_no_distinct->size() == 3);
    }

    INFO("DISTINCT a (no GROUP BY): untouched -> Unique kept, 2 rows");
    {
        auto s = otterbrix::session_id_t();
        auto plan = dispatcher->execute_sql(s, "EXPLAIN SELECT DISTINCT a FROM TestDatabase.t;");
        REQUIRE(plan->is_success());
        REQUIRE(contains(plan_text(plan), "Unique"));

        auto s2 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s2, "SELECT DISTINCT a FROM TestDatabase.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}
