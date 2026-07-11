#include "test_config.hpp"

#include <catch2/catch.hpp>

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

    // A host-supplied renderer: emits a fixed marker so the customization point is observable.
    cursor_t_ptr
    fake_render(std::pmr::memory_resource* mr, const services::collection::explain_plan_node& /*root*/, bool /*analyze*/) {
        std::pmr::vector<types::complex_logical_type> types(mr);
        types.emplace_back(types::logical_type::STRING_LITERAL, "QUERY PLAN");
        vector::data_chunk_t chunk(mr, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, std::string_view("FAKE-RENDERER"));
        return make_cursor(mr, std::move(chunk));
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

    INFO("EXPLAIN SELECT: single QUERY PLAN column, scans the table") {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->chunks().front().data[0].type().alias() == "QUERY PLAN");
        REQUIRE(cur->size() > 0);
        REQUIRE(contains(plan_text(cur), "orders"));
    }

    INFO("EXPLAIN SELECT with JOIN renders both scanned relations") {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "orders"));
        REQUIRE(contains(t, "customer"));
    }

    INFO("EXPLAIN ANALYZE reports actual per-operator stats") {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "EXPLAIN ANALYZE SELECT * FROM TestDatabase.orders o JOIN TestDatabase.customer c ON o.cust = c.id;");
        REQUIRE(cur->is_success());
        const auto t = plan_text(cur);
        REQUIRE(contains(t, "actual time"));
        REQUIRE(contains(t, "rows="));
        REQUIRE(contains(t, "loops="));
    }

    INFO("plan-only EXPLAIN INSERT does NOT change the table") {
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

    INFO("EXPLAIN ANALYZE INSERT executes and commits (PostgreSQL-compatible)") {
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

    INFO("EXPLAIN of an unsupported (DDL) inner statement is rejected") {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN CREATE TABLE TestDatabase.foo(x int);");
        REQUIRE(cur->is_error());
    }

    INFO("host customization: set_explain_renderer swaps output, SQL unchanged") {
        REQUIRE(dispatcher->set_explain_renderer(&fake_render));
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(s, "EXPLAIN SELECT * FROM TestDatabase.orders;");
        REQUIRE(cur->is_success());
        REQUIRE(contains(plan_text(cur), "FAKE-RENDERER"));
    }
}
