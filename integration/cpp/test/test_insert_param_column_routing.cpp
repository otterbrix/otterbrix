#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/vector/vector_builders.hpp>

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

    using param_t = std::pair<size_t, components::vector::vector_t>;
    using params_t = std::vector<param_t>;

    components::vector::vector_t i64(std::pmr::memory_resource* resource, int64_t v) {
        return components::vector::make_scalar(resource, v);
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
    auto cell0 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    CHECK(cell0.value() == 1);
    auto cell1 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell1.has_error());
    CHECK(cell1.value() == 7);
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
    auto cell2 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == 7);
    auto cell3 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell3.has_error());
    CHECK(cell3.value() == 1);
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
    auto cell4 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell4.has_error());
    CHECK(cell4.value() == 10);
    auto cell5 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell5.has_error());
    CHECK(cell5.value() == 20);
    auto cell6 = cur->get<int64_t>(2, 0);
    REQUIRE_FALSE(cell6.has_error());
    CHECK(cell6.value() == 30);
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
    auto cell7 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell7.has_error());
    CHECK(cell7.value() == 6);
    auto cell8 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell8.has_error());
    CHECK(cell8.value() == 5);
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
    auto cell9 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell9.has_error());
    CHECK(cell9.value() == 1);
    auto cell10 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell10.has_error());
    CHECK(cell10.value() == 7);
    auto cell11 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell11.has_error());
    CHECK(cell11.value() == 8);
    auto cell12 = cur->get<int64_t>(1, 1);
    REQUIRE_FALSE(cell12.has_error());
    CHECK(cell12.value() == 2);
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
    auto cell13 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell13.has_error());
    CHECK(cell13.value() == 7);
    auto cell14 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell14.has_error());
    CHECK(cell14.value() == 8);
}
