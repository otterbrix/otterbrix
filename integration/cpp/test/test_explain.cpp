#include "test_config.hpp"

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

    bool contains(const std::string& hay, const std::string& needle) {
        return hay.find(needle) != std::string::npos;
    }

    // A single-row cursor carrying `marker` in the "QUERY PLAN" column — the observable output of a
    // host-supplied renderer, so a test can assert which renderer produced a query's EXPLAIN.
    cursor_t_ptr marker_cursor(std::pmr::memory_resource* mr, std::string_view marker) {
        std::pmr::vector<types::complex_logical_type> types(mr);
        types.emplace_back(types::logical_type::STRING_LITERAL, "QUERY PLAN");
        vector::data_chunk_t chunk(mr, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, marker);
        return make_cursor(mr, std::move(chunk));
    }

    // Distinct host renderers: each emits a fixed marker so the selected slot is observable.
    cursor_t_ptr
    fake_render(std::pmr::memory_resource* mr, const services::collection::explain_plan_node& /*root*/, bool /*analyze*/) {
        return marker_cursor(mr, std::string_view("FAKE-RENDERER"));
    }
    // Marker deliberately NOT a substring of (nor containing) "FAKE-RENDERER", so a distinctness
    // assertion can tell the two fakes apart both ways.
    cursor_t_ptr fake_render_2(std::pmr::memory_resource* mr,
                               const services::collection::explain_plan_node& /*root*/,
                               bool /*analyze*/) {
        return marker_cursor(mr, std::string_view("FAKE-SPARK"));
    }
    // Reports the `analyze` flag it was called with, so a test can prove EXPLAIN ANALYZE reaches a
    // custom renderer with analyze == true.
    cursor_t_ptr fake_render_analyze(std::pmr::memory_resource* mr,
                                     const services::collection::explain_plan_node& /*root*/,
                                     bool analyze) {
        return marker_cursor(mr, std::string_view(analyze ? "FAKE-ANALYZE-ON" : "FAKE-ANALYZE-OFF"));
    }

} // namespace

TEST_CASE("integration::cpp::test_explain::sql") {
    auto config = test_create_config("/tmp/test_explain/sql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // --- setup: two tables + rows so EXPLAIN has a scan + a join to render ---
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

    INFO("EXPLAIN SELECT: single QUERY PLAN column, scans the table"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->chunks().front().data[0].type().alias() == "QUERY PLAN");
        REQUIRE(cur->size() > 0);
        REQUIRE(contains(plan_text(cur), "orders"));
    }

    INFO("EXPLAIN SELECT with JOIN renders both scanned relations"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE reports actual per-operator stats"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "actual time"));
        REQUIRE(contains(t, "rows="));
        REQUIRE(contains(t, "loops="));
    }

    INFO("plan-only EXPLAIN INSERT does NOT change the table"); {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, "EXPLAIN INSERT INTO TestDatabase.orders (id, cust) VALUES (99, 99);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == 3); // still 3 — plan-only never executed
        }
    }

    INFO("EXPLAIN ANALYZE INSERT executes and commits (PostgreSQL-compatible)"); {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s,
                                      "EXPLAIN ANALYZE INSERT INTO TestDatabase.orders (id, cust) VALUES (99, 99);")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->value(0, 0).value<int64_t>() == 4); // now 4 — ANALYZE ran the insert
        }
    }

    INFO("EXPLAIN (ANALYZE false/off/0) is plan-only — the inner DML must NOT execute"); {
        // Regression: an `ANALYZE <false-ish>` arg was misread as ANALYZE=true and ran the INSERT.
        for (const char* q :
             {"EXPLAIN (ANALYZE false) INSERT INTO TestDatabase.orders (id, cust) VALUES (77, 77);",
              "EXPLAIN (ANALYZE off) INSERT INTO TestDatabase.orders (id, cust) VALUES (66, 66);",
              "EXPLAIN (ANALYZE 0) INSERT INTO TestDatabase.orders (id, cust) VALUES (55, 55);"}) {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, q)->is_success());
        }
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 4); // still 4 — no plan-only form executed
    }

    INFO("EXPLAIN (ANALYZE true) DOES execute the inner DML (positive control)"); {
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(
                            s, "EXPLAIN (ANALYZE true) INSERT INTO TestDatabase.orders (id, cust) VALUES (88, 88);")
                        ->is_success());
        }
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "SELECT COUNT(*) FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5); // now 5 — ANALYZE true ran the insert
    }

    INFO("plan-only EXPLAIN does NOT execute an uncorrelated sub-query"); {
        // The scalar sub-query returns 2 rows (customer); if plan-only EXPLAIN ran it, compaction to a
        // single value would error. Plan-only must skip sub-query execution and still render the plan.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN SELECT * FROM TestDatabase.orders WHERE id = (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "orders"));
    }

    INFO("EXPLAIN of an unsupported (DDL) inner statement is rejected"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN CREATE TABLE TestDatabase.foo(x int);");
        REQUIRE(cur->is_error());
    }

    INFO("host customization: set_explain_renderer swaps output, SQL unchanged"); {
        // Register the fake at slot 0 — the default slot a plain EXPLAIN (render_id == 0) selects.
        REQUIRE(dispatcher->set_explain_renderer(0, &fake_render));
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
    }
}

TEST_CASE("integration::cpp::test_explain::inline_subquery_initplan") {
    auto config = test_create_config("/tmp/test_explain/inline_subquery_initplan");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    INFO("EXPLAIN ANALYZE: scalar WHERE sub-query renders an InitPlan with the sub-scan + stats"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust = (SELECT max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $")); // PostgreSQL-style InitPlan header + param slot
        REQUIRE(contains(t, "customer"));              // the sub-query's scanned relation
        REQUIRE(contains(t, "actual time"));           // ANALYZE stats present (main + sub tree)
    }

    INFO("EXPLAIN ANALYZE: IN (SELECT ...) renders an InitPlan"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust IN (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    // The attach-at-root placement is operator-INDEPENDENT: the flattened sub-query's param lands on a
    // scan / match / aggregate / join, but the InitPlan renders at the root regardless. These three cover
    // the carriers that motivated the rewrite (EXISTS->match, HAVING->aggregate, JOIN-ON->join).
    INFO("EXPLAIN ANALYZE: EXISTS (...) renders an InitPlan (carrier: operator_match)"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE EXISTS (SELECT id FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE: HAVING sub-query renders an InitPlan (carrier: operator_group / aggregate)"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "EXPLAIN ANALYZE SELECT cust, count(*) FROM TestDatabase.orders "
                                           "GROUP BY cust HAVING count(*) > (SELECT min(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE: JOIN-ON sub-query renders an InitPlan (carrier: operator_join)"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c "
            "ON o.cust = c.id AND c.id = (SELECT max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
    }

    INFO("EXPLAIN ANALYZE: two sub-queries render two globally-numbered InitPlans"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s,
                                           "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders "
                                           "WHERE cust = (SELECT max(id) FROM TestDatabase.customer) "
                                           "AND id = (SELECT min(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "InitPlan 2 (returns $")); // global numbering, second sub-query
    }

    INFO("EXPLAIN ANALYZE: nested sub-query — both InitPlans present (all flattened top-level)"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s,
            "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders WHERE cust IN "
            "(SELECT id FROM TestDatabase.customer WHERE id = (SELECT max(id) FROM TestDatabase.customer));");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "InitPlan 1 (returns $"));
        REQUIRE(contains(t, "InitPlan 2 (returns $")); // nested sub-query is a sibling InitPlan, not dropped
    }

    INFO("plain EXPLAIN (not ANALYZE) shows the sub-query InitPlan STRUCTURE (PostgreSQL), without stats"); {
        // PostgreSQL's plain EXPLAIN shows the InitPlan shape even though it does not RUN the sub-query.
        // We build each flattened sub-query's physical plan and capture its IR, but never execute it — so
        // the InitPlan section appears with no `actual time`/rows/loops.
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN SELECT * FROM TestDatabase.orders WHERE cust = (SELECT max(id) FROM TestDatabase.customer);");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE(contains(t, "InitPlan 1 (returns $")); // structure shown ...
        REQUIRE(contains(t, "customer"));
        REQUIRE_FALSE(contains(t, "actual time")); // ... but the sub-query was NOT executed (no ANALYZE stats)
    }
}

TEST_CASE("integration::cpp::test_explain::per_query_renderer") {
    auto config = test_create_config("/tmp/test_explain/per_query_renderer");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // Register two DISTINCT host renderers into slots 1 and 2; slot 0 stays the built-in postgres.
    REQUIRE(dispatcher->set_explain_renderer(1, &fake_render));
    REQUIRE(dispatcher->set_explain_renderer(2, &fake_render_2));

    INFO("per-query selection: id 1 -> fake, id 0 -> postgres default"); {
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
            REQUIRE(contains(t, "orders"));         // built-in postgres output
            REQUIRE_FALSE(contains(t, "FAKE-RENDERER"));
        }
    }

    INFO("per-query, NOT global: interleaved ids each pick their own renderer"); {
        auto s1 = otterbrix::session_id_t();
        auto c1 = dispatcher->execute_sql(s1, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
        auto s0 = otterbrix::session_id_t();
        auto c0 = dispatcher->execute_sql(s0, "EXPLAIN SELECT * FROM TestDatabase.orders;", 0);
        REQUIRE(contains(plan_text(c1), "FAKE-RENDERER"));
        REQUIRE_FALSE(contains(plan_text(c0), "FAKE-RENDERER"));
    }

    INFO("multiple renderers registered simultaneously: id 1 and id 2 are distinct"); {
        auto s1 = otterbrix::session_id_t();
        auto c1 = dispatcher->execute_sql(s1, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
        auto s2 = otterbrix::session_id_t();
        auto c2 = dispatcher->execute_sql(s2, "EXPLAIN SELECT * FROM TestDatabase.orders;", 2);
        const auto t1 = plan_text(c1);
        const auto t2 = plan_text(c2);
        // Cross negatives (markers are mutually non-substring) so this genuinely proves the two
        // slots resolve to DIFFERENT renderers, not merely that each contains its own marker.
        REQUIRE(contains(t1, "FAKE-RENDERER"));
        REQUIRE_FALSE(contains(t1, "FAKE-SPARK"));
        REQUIRE(contains(t2, "FAKE-SPARK"));
        REQUIRE_FALSE(contains(t2, "FAKE-RENDERER"));
    }

    INFO("out-of-range id resolves to the built-in default (a default, not a fallback branch)"); {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 999);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));             // postgres default
        REQUIRE_FALSE(contains(t, "FAKE-RENDERER"));
    }

    INFO("EXPLAIN ANALYZE via a custom renderer sees analyze == true"); {
        REQUIRE(dispatcher->set_explain_renderer(3, &fake_render_analyze));
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders;", 3);
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "FAKE-ANALYZE-ON"));
    }

    INFO("registration fan-out reaches every pooled executor"); {
        // The pool has 4 executors; a session hashes to one. Sweep enough sessions that a single
        // set_explain_renderer(1, ...) registration must have reached whichever executor each hits.
        for (int i = 0; i < 12; ++i) {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 1);
            REQUIRE(cur->is_success());
            REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
        }
    }
}

TEST_CASE("integration::cpp::test_explain::renderer_registration_edges") {
    auto config = test_create_config("/tmp/test_explain/renderer_registration_edges");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
            dispatcher->execute_sql(s, "INSERT INTO TestDatabase.orders (id, cust) VALUES (1,10),(2,20);")->is_success());
    }

    INFO("out-of-range registration id is rejected, not an unbounded allocation"); {
        // A bogus huge id must return false (bounded) — never resize the per-executor registry to
        // gigabytes of fill (which, with exceptions disabled, would abort the process).
        REQUIRE_FALSE(dispatcher->set_explain_renderer(4000000000u, &fake_render));
    }

    INFO("a null renderer is rejected (reported failure, not silent success)"); {
        REQUIRE_FALSE(dispatcher->set_explain_renderer(5, nullptr));
    }

    INFO("out-of-range render_id resolves to slot 0 — the host's default, not the built-in"); {
        // Host overwrites the default slot 0 with its own renderer; an out-of-range query id must
        // resolve to THAT slot-0 default (not the hardcoded built-in postgres).
        REQUIRE(dispatcher->set_explain_renderer(0, &fake_render_2)); // slot 0 = FAKE-SPARK
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", 999);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "FAKE-SPARK"));   // host's slot-0 default
        REQUIRE_FALSE(contains(t, "orders")); // NOT the built-in postgres plan
    }

    INFO("execute_sql_with_params honors render_id (previously dropped)"); {
        // slot 1 = FAKE-RENDERER; slot 0 was overwritten to FAKE-SPARK above. No bound parameters are
        // needed — the point is that this entry point stamps render_id like execute_sql does.
        REQUIRE(dispatcher->set_explain_renderer(1, &fake_render));
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql_with_params(s, "EXPLAIN SELECT * FROM TestDatabase.orders;", {}, 1);
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "FAKE-RENDERER"));  // render_id 1 honored (slot 1)
        REQUIRE_FALSE(contains(t, "FAKE-SPARK")); // not the slot-0 default
    }
}

TEST_CASE("integration::cpp::test_explain::analyze_recursive_cte_rows") {
    auto config = test_create_config("/tmp/test_explain/analyze_recursive_cte_rows");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    INFO("EXPLAIN ANALYZE records the recursive-CTE producer's rows (regression: was rows=0)"); {
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
        // Isolate the Recursive Union line: before the fix it read "(actual time=0.000ms rows=0 loops=1)"
        // because the producing bottom's record_analyze was never called.
        const auto pos = t.find("Recursive Union");
        const auto eol = t.find('\n', pos);
        const std::string ru_line = t.substr(pos, eol - pos);
        REQUIRE_FALSE(contains(ru_line, "rows=0")); // now reports the actual produced row count
    }
}

TEST_CASE("integration::cpp::test_explain::analyze_per_loop_rows_round") {
    // Round-to-nearest per-loop rows (PostgreSQL rint), computed by render_postgres directly. Uses a
    // monotonic arena over new_delete_resource — never std::pmr::get_default_resource() (Rule 14).
    std::pmr::monotonic_buffer_resource pool{std::pmr::new_delete_resource()};
    auto* mr = &pool;

    INFO("5 rows / 3 loops rounds to 2 (truncation gave 1)"); {
        services::collection::explain_plan_node node(mr);
        node.type = components::operators::operator_type::full_scan; // renders "Seq Scan"
        node.rows = 5;
        node.loops = 3;
        node.time = std::chrono::nanoseconds(3'000'000);
        auto cur = services::collection::render_postgres(mr, node, /*analyze=*/true);
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "rows=2"));
    }

    INFO("2 rows / 3 loops rounds to 1 (truncation gave 0)"); {
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
