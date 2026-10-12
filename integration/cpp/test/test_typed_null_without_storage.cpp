#include "integration_fixture_path.hpp"
#include "test_config.hpp"

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

    cursor_t_ptr run_refused(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        REQUIRE(cur->is_error());
        return cur;
    }

    configuration::config config_for(const char* name) {
        auto config = test_create_config(integration_fixture_path(std::string{"typed_null_without_storage/"} + name));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    void backfills_null(const char* name, const std::string& column, const std::string& later_value) {
        test_spaces space(config_for(name));
        auto* d = space.dispatcher();
        run_ok(d, "CREATE DATABASE d;");
        run_ok(d, "CREATE TABLE d.t (id bigint);");
        run_ok(d, "INSERT INTO d.t (id) VALUES (1);");
        run_ok(d, "ALTER TABLE d.t ADD COLUMN x " + column + ";");
        auto old_row = run_ok(d, "SELECT id, x FROM d.t;");
        REQUIRE(old_row->size() == 1);
        CHECK(old_row->value(1, 0).is_null());
        if (later_value.empty()) {
            return;
        }
        run_ok(d, "INSERT INTO d.t (id, x) VALUES (2, " + later_value + ");");
        auto both = run_ok(d, "SELECT id, x FROM d.t WHERE id = 2;");
        REQUIRE(both->size() == 1);
        CHECK_FALSE(both->value(1, 0).is_null());
    }

    void is_refused(const char* name, const std::string& column) {
        test_spaces space(config_for(name));
        auto* d = space.dispatcher();
        run_ok(d, "CREATE DATABASE d;");
        run_ok(d, "CREATE TABLE d.t (id bigint);");
        run_ok(d, "INSERT INTO d.t (id) VALUES (1);");
        run_refused(d, "ALTER TABLE d.t ADD COLUMN x " + column + ";");
        auto rows = run_ok(d, "SELECT * FROM d.t;");
        REQUIRE(rows->size() == 1);
        CHECK(rows->column_count() == 1);
    }

} // namespace

TEST_CASE("integration::cpp::typed_null_without_storage::add_fixed_array_column_backfills_null") {
    backfills_null("array1", "bigint[1]", "array[7]");
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_nested_array_column_backfills_null") {
    backfills_null("array2x2", "int[2][2]", "");
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_interval_column_backfills_null") {
    backfills_null("interval", "interval", "interval '1 day'");
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_timetz_column_backfills_null") {
    backfills_null("timetz", "timetz", "'12:00:00+01'");
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_fixed_array_column_default_null_backfills_null") {
    backfills_null("array1_default_null", "bigint[1] DEFAULT NULL", "array[7]");
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_struct_column_backfills_null") {
    test_spaces space(config_for("struct"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE TYPE pair_t AS (x bigint, y bigint);");
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.t (id bigint);");
    run_ok(d, "INSERT INTO d.t (id) VALUES (1);");
    run_ok(d, "ALTER TABLE d.t ADD COLUMN p pair_t;");
    auto old_row = run_ok(d, "SELECT id, p FROM d.t;");
    REQUIRE(old_row->size() == 1);
    CHECK(old_row->value(1, 0).is_null());
}

TEST_CASE("integration::cpp::typed_null_without_storage::add_blob_column_is_refused") { is_refused("blob", "blob"); }

TEST_CASE("integration::cpp::typed_null_without_storage::add_bit_column_is_refused") { is_refused("bit", "bit"); }

TEST_CASE("integration::cpp::typed_null_without_storage::add_pointer_column_is_refused") {
    is_refused("pointer", "pointer");
}

TEST_CASE("integration::cpp::typed_null_without_storage::cast_null_to_blob_is_refused") {
    test_spaces space(config_for("cast_blob"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_refused(d, "SELECT CAST(NULL AS blob) AS x;");
    run_refused(d, "SELECT CAST(NULL AS bit) AS x;");
    run_refused(d, "SELECT CAST(NULL AS pointer) AS x;");
    run_refused(d, "SELECT CAST(NULL AS blob[]) AS x;");
    auto text = run_ok(d, "SELECT CAST(NULL AS text) AS x;");
    REQUIRE(text->size() == 1);
    CHECK(text->value(0, 0).is_null());
}

TEST_CASE("integration::cpp::typed_null_without_storage::matview_over_cast_null_to_blob_never_crashes") {
    test_spaces space(config_for("matview_blob"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.src (a bigint);");
    run_ok(d, "INSERT INTO d.src (a) VALUES (1), (2);");
    auto created =
        exec(d, "CREATE MATERIALIZED VIEW d.mv AS SELECT a, CAST(NULL AS blob) AS x FROM d.src WITH NO DATA;");
    if (created->is_success()) {
        run_refused(d, "REFRESH MATERIALIZED VIEW d.mv;");
    }
    auto src = run_ok(d, "SELECT a FROM d.src;");
    CHECK(src->size() == 2);
}
