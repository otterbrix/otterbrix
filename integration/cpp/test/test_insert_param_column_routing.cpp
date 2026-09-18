#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/types/logical_value.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// A parameterized INSERT builds its working chunk in DISCOVERY order: literals create
// columns as fill_row meets them, parameters used to materialise their columns only at
// bind time — appended after every literal column. Everything downstream pairs the
// chunk's columns with the written column list positionally, so the appended column
// landed its value under another column's name: INSERT (id, d) VALUES ($1, 7) stored
// id=7, d=1. These cases pin the routing by reading the numbers back.

using namespace test_helpers;

namespace {

    using param_t = std::pair<size_t, components::types::logical_value_t>;
    using params_t = std::vector<param_t>;

    components::types::logical_value_t i64(std::pmr::memory_resource* resource, int64_t v) {
        return components::types::logical_value_t{resource, v};
    }

} // namespace

TEST_CASE("integration::cpp::insert_param_column_routing::leading_parameter_before_a_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/leading_param"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 1)}};
    auto insert =
        d->execute_sql_with_params(otterbrix::session_id_t(), "INSERT INTO w.t (id, d) VALUES ($1, 7);", params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).value<int64_t>() == 7);
}

TEST_CASE("integration::cpp::insert_param_column_routing::trailing_parameter_after_a_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/trailing_param"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 1)}};
    auto insert =
        d->execute_sql_with_params(otterbrix::session_id_t(), "INSERT INTO w.t (id, d) VALUES (7, $1);", params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 7);
    CHECK(cur->value(1, 0).value<int64_t>() == 1);
}

TEST_CASE("integration::cpp::insert_param_column_routing::middle_parameter_among_three_columns") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/middle_param"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (a BIGINT, b BIGINT, c BIGINT);")->is_success());

    params_t params{{1, i64(resource, 20)}};
    auto insert =
        d->execute_sql_with_params(otterbrix::session_id_t(), "INSERT INTO w.t (a, b, c) VALUES (10, $1, 30);", params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT a, b, c FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
    CHECK(cur->value(1, 0).value<int64_t>() == 20);
    CHECK(cur->value(2, 0).value<int64_t>() == 30);
}

TEST_CASE("integration::cpp::insert_param_column_routing::placeholders_written_in_reverse_order") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/reverse_placeholders"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 5)}, {2, i64(resource, 6)}};
    auto insert =
        d->execute_sql_with_params(otterbrix::session_id_t(), "INSERT INTO w.t (id, d) VALUES ($2, $1);", params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 6);
    CHECK(cur->value(1, 0).value<int64_t>() == 5);
}

TEST_CASE("integration::cpp::insert_param_column_routing::two_rows_swap_the_parameter_column") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/two_rows"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 1)}, {2, i64(resource, 2)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES ($1, 7), (8, $2);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).value<int64_t>() == 7);
    CHECK(cur->value(0, 1).value<int64_t>() == 8);
    CHECK(cur->value(1, 1).value<int64_t>() == 2);
}

TEST_CASE("integration::cpp::insert_param_column_routing::literals_only_stay_in_place") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_column_routing/literals_only"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    REQUIRE(exec(d, "INSERT INTO w.t (id, d) VALUES (7, 8);")->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 7);
    CHECK(cur->value(1, 0).value<int64_t>() == 8);
}
