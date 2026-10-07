#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

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

    std::string run_refused(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        REQUIRE(cur->is_error());
        return std::string{cur->get_error().what};
    }

    configuration::config config_for(const char* name) {
        auto config = test_create_config(integration_fixture_path(std::string{"matview_null_column/"} + name));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

} // namespace

TEST_CASE("integration::cpp::matview_null_column::a_NULL_only_column_is_text_and_holds_rows") {
    auto config = config_for("matview");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        run_ok(d, "CREATE DATABASE d;");
        run_ok(d, "CREATE TABLE d.src (a bigint);");
        run_ok(d, "INSERT INTO d.src (a) VALUES (1), (2);");
        run_ok(d, "CREATE MATERIALIZED VIEW d.mv AS SELECT a, NULL AS n FROM d.src WITH NO DATA;");

        auto empty = run_ok(d, "SELECT * FROM d.mv;");
        REQUIRE(empty->chunks().size() == 1);
        REQUIRE(empty->chunks().front().types().size() == 2);
        CHECK(empty->chunks().front().types()[1].type() == types::logical_type::STRING_LITERAL);

        run_ok(d, "REFRESH MATERIALIZED VIEW d.mv;");
        auto filled = run_ok(d, "SELECT a, n FROM d.mv;");
        REQUIRE(filled->size() == 2);
        CHECK(filled->value(1, 0).is_null());
        CHECK(filled->value(1, 1).is_null());
        run_ok(d, "CHECKPOINT;");
    }
    {
        auto host = otterbrix::base_otterbrix_t::open(config, {});
        INFO("restart: " << (host.has_error() ? host.error().what : "ok"));
        REQUIRE_FALSE(host.has_error());
        otterbrix::otterbrix_ptr engine{new otterbrix::otterbrix_t(std::move(host.value()))};
        auto again = run_ok(engine->dispatcher(), "SELECT a, n FROM d.mv;");
        REQUIRE(again->size() == 2);
        CHECK(again->value(1, 0).is_null());
    }
}

TEST_CASE("integration::cpp::matview_null_column::a_view_over_SELECT_NULL_records_text_and_still_expands") {
    test_spaces space(config_for("view"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE VIEW d.v AS SELECT NULL AS x;");
    auto read = run_ok(d, "SELECT * FROM d.v;");
    REQUIRE(read->size() == 1);
    CHECK(read->value(0, 0).is_null());
}

TEST_CASE("integration::cpp::matview_null_column::a_matview_over_an_array_of_NULL_is_refused") {
    test_spaces space(config_for("array_of_null"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.src (a bigint);");
    run_ok(d, "INSERT INTO d.src (a) VALUES (1), (2);");
    const auto why =
        run_refused(d, "CREATE MATERIALIZED VIEW d.mv AS SELECT a, array[NULL] AS x FROM d.src WITH NO DATA;");
    INFO("refusal: " << why);
    CHECK(why.find("'x'") != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM d.src;")->size() == 2);
}

TEST_CASE("integration::cpp::matview_null_column::a_matview_over_an_empty_array_is_refused") {
    test_spaces space(config_for("empty_array"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    run_ok(d, "CREATE TABLE d.src (a bigint);");
    run_ok(d, "INSERT INTO d.src (a) VALUES (1), (2);");
    const auto why = run_refused(d, "CREATE MATERIALIZED VIEW d.mv AS SELECT a, array[] AS x FROM d.src WITH NO DATA;");
    INFO("refusal: " << why);
    CHECK(why.find("'x'") != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM d.src;")->size() == 2);
}

TEST_CASE("integration::cpp::matview_null_column::a_blob_column_is_refused_by_CREATE_TABLE") {
    test_spaces space(config_for("blob_table"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE d;");
    const auto why = run_refused(d, "CREATE TABLE d.t (id bigint, b blob);");
    INFO("refusal: " << why);
    CHECK(why.find("'b'") != std::string::npos);
    CHECK(why.find("bytea") != std::string::npos);
    run_ok(d, "CREATE TABLE d.t (id bigint);");
    run_ok(d, "INSERT INTO d.t (id) VALUES (1);");
    CHECK(run_ok(d, "SELECT * FROM d.t;")->size() == 1);
}
