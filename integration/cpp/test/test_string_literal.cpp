#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/vector/vector_builders.hpp>

#include <string>
#include <utility>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return cur;
    }

    configuration::config config_for(const char* name) {
        auto config = test_create_config(integration_fixture_path(std::string{"string_literal/"} + name));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    void create_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_ok(dispatcher, "CREATE DATABASE d;");
        run_ok(dispatcher, "CREATE TABLE d.t (i integer, day date, s text);");
        run_ok(dispatcher, "INSERT INTO d.t (i, day, s) VALUES ('5', '2020-01-15', '7');");
    }

} // namespace

TEST_CASE("integration::cpp::string_literal") {
    test_spaces space(config_for("statements"));
    auto* d = space.dispatcher();
    create_table(d);

    SECTION("insert") {
        SECTION("values") {
            auto stored = run_ok(d, "SELECT i FROM d.t;");
            REQUIRE(stored->size() == 1);
            auto cell = stored->get<int32_t>(0, 0);
            REQUIRE_FALSE(cell.has_error());
            CHECK(cell.value() == 5);
        }

        SECTION("typed text") {
            // '7' is a number as written, but s holds typed text: neither reaches an integer column implicitly.
            CHECK(exec(d, "INSERT INTO d.t (i) VALUES ('7'::text);")->is_error());
            CHECK(exec(d, "INSERT INTO d.t (i) SELECT s FROM d.t;")->is_error());
            run_ok(d, "INSERT INTO d.t (i) SELECT s::integer FROM d.t;");
        }

        SECTION("select") {
            // The SELECT an INSERT reads leaves its literals to the target columns.
            run_ok(d, "INSERT INTO d.t (i, day) SELECT '30', '2022-03-04';");
            auto selected = run_ok(d, "SELECT i FROM d.t WHERE day = '2022-03-04';");
            REQUIRE(selected->size() == 1);
            auto cell = selected->get<int32_t>(0, 0);
            REQUIRE_FALSE(cell.has_error());
            CHECK(cell.value() == 30);
        }

        SECTION("nested select") {
            // One SELECT deeper, the literal is text already.
            CHECK(exec(d, "INSERT INTO d.t (i) SELECT x FROM (SELECT '31' AS x) s;")->is_error());
            run_ok(d, "INSERT INTO d.t (s) SELECT x FROM (SELECT '31' AS x) s;");
        }

        SECTION("bound parameter") {
            // s first holds the literal 'a', then the number 3: 'a' waits for the column's type while $1 is bound.
            std::vector<std::pair<size_t, vector::vector_t>> params;
            params.emplace_back(1, vector::make_scalar(d->resource(), int64_t{41}));
            auto inserted = d->execute_sql_with_params(otterbrix::session_id_t(),
                                                       "INSERT INTO d.t (i, s) VALUES ($1, 'a'), (42, 3);",
                                                       params);
            INFO("insert error: " << (inserted->is_error() ? inserted->get_error().what : "none"));
            REQUIRE(inserted->is_success());
            auto texts = run_ok(d, "SELECT s FROM d.t WHERE i > 40 ORDER BY i;");
            REQUIRE(texts->size() == 2);
            auto first = texts->get<std::string_view>(0, 0);
            REQUIRE_FALSE(first.has_error());
            CHECK(first.value() == "a");
            auto second = texts->get<std::string_view>(0, 1);
            REQUIRE_FALSE(second.has_error());
            CHECK(second.value() == "3");
        }
    }

    SECTION("update") {
        SECTION("literal") {
            run_ok(d, "UPDATE d.t SET i = '9', day = '2021-02-03';");
            auto stored = run_ok(d, "SELECT i FROM d.t WHERE day = '2021-02-03';");
            REQUIRE(stored->size() == 1);
            auto cell = stored->get<int32_t>(0, 0);
            REQUIRE_FALSE(cell.has_error());
            CHECK(cell.value() == 9);
        }

        SECTION("typed value") {
            // bigint reaches an integer column only by an assignment cast.
            run_ok(d, "UPDATE d.t SET i = 4::bigint;");
            CHECK(exec(d, "UPDATE d.t SET i = s;")->is_error());
        }
    }

    SECTION("values read as rows") {
        SECTION("only literals") {
            for (const char* sql : {"VALUES ('a'), ('b');", "SELECT * FROM (VALUES ('a'), ('b')) v;"}) {
                auto rows = run_ok(d, sql);
                INFO("statement: " << sql);
                REQUIRE(rows->size() == 2);
                CHECK(rows->type_data().front().type() == types::logical_type::TEXT);
                auto text = rows->get<std::string_view>(0, 1);
                REQUIRE_FALSE(text.has_error());
                CHECK(text.value() == "b");
            }
        }

        SECTION("literal and number") {
            // A literal takes the type of the other values in its column, in either order.
            for (const char* sql : {"VALUES ('1'), (2);", "SELECT * FROM (VALUES (2), ('1')) v;"}) {
                auto rows = run_ok(d, sql);
                INFO("statement: " << sql);
                REQUIRE(rows->size() == 2);
                CHECK(rows->type_data().front().type() == types::logical_type::BIGINT);
                int64_t total = 0;
                for (uint64_t row = 0; row < 2; ++row) {
                    auto cell = rows->get<int64_t>(0, row);
                    REQUIRE_FALSE(cell.has_error());
                    total += cell.value();
                }
                CHECK(total == 3);
            }
            CHECK(exec(d, "VALUES ('a'), (1);")->is_error());
        }

        SECTION("typed text") {
            CHECK(exec(d, "VALUES (1), ('x'::text);")->is_error());
            run_ok(d, "VALUES ('x'::text), ('y');");
        }
    }

    SECTION("comparison") {
        CHECK(run_ok(d, "SELECT i FROM d.t WHERE i = '5';")->size() == 1);
        CHECK(run_ok(d, "SELECT i FROM d.t WHERE day = '2020-01-15';")->size() == 1);
        CHECK(run_ok(d, "SELECT i FROM d.t WHERE s = '7';")->size() == 1);
        CHECK(exec(d, "SELECT i FROM d.t WHERE i = s;")->is_error());

        std::vector<std::pair<size_t, vector::vector_t>> params;
        params.emplace_back(1, vector::make_scalar(d->resource(), std::string("2020-01-15")));
        auto cur = d->execute_sql_with_params(otterbrix::session_id_t(), "SELECT i FROM d.t WHERE day = $1;", params);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }

    SECTION("untyped output") {
        auto plain = run_ok(d, "SELECT 'abc' AS v;");
        REQUIRE(plain->size() == 1);
        CHECK(plain->type_data().front().type() == types::logical_type::TEXT);
        auto text = plain->get<std::string_view>(0, 0);
        REQUIRE_FALSE(text.has_error());
        CHECK(text.value() == "abc");

        auto grouped = run_ok(d, "SELECT 'k' AS v, count(*) AS n FROM d.t;");
        REQUIRE(grouped->size() == 1);
        CHECK(grouped->type_data().front().type() == types::logical_type::TEXT);

        auto blended = run_ok(d, "SELECT CASE WHEN i = 5 THEN 'yes' ELSE 'no' END AS v FROM d.t;");
        REQUIRE(blended->size() == 1);
        CHECK(blended->type_data().front().type() == types::logical_type::TEXT);
    }
}

TEST_CASE("integration::cpp::string_literal::restart") {
    auto config = config_for("restart");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        create_table(d);
        run_ok(d, "CREATE MATERIALIZED VIEW d.mv AS SELECT i, 'x' AS tag FROM d.t WITH NO DATA;");
        run_ok(d, "REFRESH MATERIALIZED VIEW d.mv;");
        run_ok(d, "CHECKPOINT;");
    }
    // A literal stored through a view comes back as text.
    auto host = otterbrix::base_otterbrix_t::open(config, {});
    INFO("restart: " << (host.has_error() ? host.error().what : "ok"));
    REQUIRE_FALSE(host.has_error());
    otterbrix::otterbrix_ptr engine{new otterbrix::otterbrix_t(std::move(host.value()))};
    auto again = run_ok(engine->dispatcher(), "SELECT tag FROM d.mv;");
    REQUIRE(again->size() == 1);
    CHECK(again->type_data().front().type() == types::logical_type::TEXT);
    auto tag = again->get<std::string_view>(0, 0);
    REQUIRE_FALSE(tag.has_error());
    CHECK(tag.value() == "x");
}
