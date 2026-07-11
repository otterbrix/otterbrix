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
