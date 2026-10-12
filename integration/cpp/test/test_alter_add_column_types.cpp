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
    REQUIRE(created->value(0, 0).type().type() == types::logical_type::BIGINT);

    run_ok(d, "CREATE TABLE d.t (a bigint);");
    run_ok(d, "ALTER TABLE d.t ADD COLUMN c int8;");
    run_ok(d, "INSERT INTO d.t (a, c) VALUES (2, 20);");
    auto added = run_ok(d, "SELECT c FROM d.t;");
    REQUIRE(added->size() == 1);
    CHECK(added->value(0, 0).type().type() == types::logical_type::BIGINT);
    CHECK(added->value(0, 0).value<int64_t>() == 20);
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
    CHECK(read->value(0, 0).type().type() == types::logical_type::DOUBLE);
    CHECK(read->value(0, 0).value<double>() == Catch::Approx(1.0));
}
