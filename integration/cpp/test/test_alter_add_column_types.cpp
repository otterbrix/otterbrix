// ALTER TABLE ... ADD COLUMN types its column the way CREATE TABLE types one: a built-in written by its catalog name
// (int8, float8, ...) is that built-in, a user type is looked up on the table database's search path, and a DEFAULT
// is cast to the column's type.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>

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
        auto config = test_create_config(integration_fixture_path(std::string{"alter_add_column_types/"} + name));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

} // namespace

TEST_CASE("integration::cpp::alter_add_column_types::builtin_by_catalog_name") {
    test_spaces space(config_for("builtin"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.created (c int8);");
    run_ok(d, "INSERT INTO d.created (c) VALUES (20);");
    auto created = run_ok(d, "SELECT c FROM d.created;");
    REQUIRE(created->size() == 1);
    REQUIRE(created->type_data()[0].type() == types::logical_type::BIGINT);
    auto cell8 = created->is_null(0, 0);
    REQUIRE_FALSE(cell8.has_error());
    REQUIRE_FALSE(cell8.value());

    run_ok(d, "CREATE TABLE d.t (a bigint);");
    run_ok(d, "ALTER TABLE d.t ADD COLUMN c int8;");
    run_ok(d, "INSERT INTO d.t (a, c) VALUES (2, 20);");
    auto added = run_ok(d, "SELECT c FROM d.t;");
    REQUIRE(added->size() == 1);
    CHECK(added->type_data()[0].type() == types::logical_type::BIGINT);
    auto cell0 = added->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    CHECK(cell0.value() == 20);
}

TEST_CASE("integration::cpp::alter_add_column_types::user_type_on_the_search_path") {
    test_spaces space(config_for("user_type"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE TYPE pair_t AS (x bigint, y bigint);");
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.created (p pair_t);");
    run_ok(d, "CREATE TABLE d.t (a bigint);");
    run_ok(d, "ALTER TABLE d.t ADD COLUMN p pair_t;");
    auto added = run_ok(d, "SELECT * FROM d.t;");
    CHECK(added->column_count() == 2);
}

TEST_CASE("integration::cpp::alter_add_column_types::default_is_cast_to_the_column_type") {
    test_spaces space(config_for("default"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.t (a bigint);");
    run_ok(d, "ALTER TABLE d.t ADD COLUMN x double DEFAULT 1;");
    run_ok(d, "INSERT INTO d.t (a) VALUES (1);");
    auto read = run_ok(d, "SELECT x FROM d.t;");
    REQUIRE(read->size() == 1);
    CHECK(read->type_data()[0].type() == types::logical_type::DOUBLE);
    auto cell1 = read->get<double>(0, 0);
    REQUIRE_FALSE(cell1.has_error());
    CHECK(cell1.value() == Catch::Approx(1.0));
}

// A NULL default carries no type of its own: the column type is resolved after the transformer (user ENUM, composite,
// a built-in written by its catalog name), so the NULL must take that resolved type rather than fail a cast.
TEST_CASE("integration::cpp::alter_add_column_types::null_default_on_a_resolved_type") {
    test_spaces space(config_for("null_default"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
    run_ok(d, "CREATE TYPE pair_t AS (x bigint, y bigint);");
    run_ok(d, "CREATE DATABASE d;");

    SECTION("CREATE TABLE") {
        run_ok(d,
               "CREATE TABLE d.t (a bigint, m mood DEFAULT NULL, p pair_t DEFAULT NULL, c public.int8 DEFAULT NULL);");
        run_ok(d, "INSERT INTO d.t (a) VALUES (1);");
        auto read = run_ok(d, "SELECT m, p, c FROM d.t;");
        REQUIRE(read->size() == 1);
        auto cell2 = read->is_null(0, 0);
        REQUIRE_FALSE(cell2.has_error());
        CHECK(cell2.value());
        auto cell3 = read->is_null(1, 0);
        REQUIRE_FALSE(cell3.has_error());
        CHECK(cell3.value());
        auto cell4 = read->is_null(2, 0);
        REQUIRE_FALSE(cell4.has_error());
        CHECK(cell4.value());
    }
    SECTION("ALTER TABLE ADD COLUMN") {
        run_ok(d, "CREATE TABLE d.t (a bigint);");
        run_ok(d, "ALTER TABLE d.t ADD COLUMN m mood DEFAULT NULL;");
        run_ok(d, "ALTER TABLE d.t ADD COLUMN p pair_t DEFAULT NULL;");
        run_ok(d, "ALTER TABLE d.t ADD COLUMN c public.int8 DEFAULT NULL;");
        run_ok(d, "INSERT INTO d.t (a) VALUES (1);");
        auto read = run_ok(d, "SELECT m, p, c FROM d.t;");
        REQUIRE(read->size() == 1);
        auto cell5 = read->is_null(0, 0);
        REQUIRE_FALSE(cell5.has_error());
        CHECK(cell5.value());
        auto cell6 = read->is_null(1, 0);
        REQUIRE_FALSE(cell6.has_error());
        CHECK(cell6.value());
        auto cell7 = read->is_null(2, 0);
        REQUIRE_FALSE(cell7.has_error());
        CHECK(cell7.value());
    }
}
