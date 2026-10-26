#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/vector/vector_builders.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// When a parameterized INSERT binds a value whose type differs from the working
// chunk's column type, bind() used to recreate that column from scratch in every
// chunk — silently erasing literal cells (and NULL literals) already written to
// other rows of the same column. These cases insert and read back, pinning by
// value that a retype keeps what was already stored.

using namespace test_helpers;

namespace {

    using param_t = std::pair<size_t, components::vector::vector_t>;
    using params_t = std::vector<param_t>;

    components::vector::vector_t i64(std::pmr::memory_resource* resource, int64_t v) {
        return components::vector::make_scalar(resource, v);
    }

    components::vector::vector_t f64(std::pmr::memory_resource* resource, double v) {
        return components::vector::make_scalar(resource, v);
    }

    components::vector::vector_t null_value(std::pmr::memory_resource* resource) {
        return components::vector::make_null(
            resource,
            components::types::complex_logical_type{components::types::logical_type::NA});
    }

} // namespace

TEST_CASE("integration::cpp::insert_param_retype::null_literal_before_a_parameter") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/null_literal_first"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 42)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, NULL), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell0 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    CHECK(cell0.value() == 1);
    auto cell1 = cur->is_null(1, 0);
    REQUIRE_FALSE(cell1.has_error());
    CHECK(cell1.value());
    auto cell2 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == 2);
    auto cell3 = cur->is_null(1, 1);
    REQUIRE_FALSE(cell3.has_error());
    REQUIRE_FALSE(cell3.value());
    auto cell4 = cur->get<int64_t>(1, 1);
    REQUIRE_FALSE(cell4.has_error());
    CHECK(cell4.value() == 42);
}

TEST_CASE("integration::cpp::insert_param_retype::literal_before_a_wider_typed_parameter") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/literal_first_wider"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d DOUBLE);")->is_success());

    params_t params{{1, f64(resource, 3.5)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell5 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell5.has_error());
    CHECK(cell5.value() == 1);
    auto cell6 = cur->get<double>(1, 0);
    REQUIRE_FALSE(cell6.has_error());
    CHECK(cell6.value() == Catch::Approx(7.0));
    auto cell7 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell7.has_error());
    CHECK(cell7.value() == 2);
    auto cell8 = cur->get<double>(1, 1);
    REQUIRE_FALSE(cell8.has_error());
    CHECK(cell8.value() == Catch::Approx(3.5));
}

TEST_CASE("integration::cpp::insert_param_retype::narrower_parameter_keeps_a_wider_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/narrow_param_wide_literal"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d DOUBLE);")->is_success());

    params_t params{{1, i64(resource, 3)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7.5), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell9 = cur->get<double>(1, 0);
    REQUIRE_FALSE(cell9.has_error());
    CHECK(cell9.value() == Catch::Approx(7.5));
    auto cell10 = cur->get<double>(1, 1);
    REQUIRE_FALSE(cell10.has_error());
    CHECK(cell10.value() == Catch::Approx(3.0));
}

TEST_CASE("integration::cpp::insert_param_retype::several_rows_and_columns_at_once") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/rows_and_columns"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, a BIGINT, b DOUBLE);")->is_success());

    params_t params{{1, i64(resource, 2)}, {2, i64(resource, 3)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, a, b) VALUES "
                                             "(1, 10, 1.5), (2, NULL, $1), (3, $2, NULL);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, a, b FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    // row id=1: literals survive both later binds
    auto cell11 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell11.has_error());
    CHECK(cell11.value() == 10);
    auto cell12 = cur->get<double>(2, 0);
    REQUIRE_FALSE(cell12.has_error());
    CHECK(cell12.value() == Catch::Approx(1.5));
    // row id=2: NULL literal in a, bound 2 -> 2.0 in b
    auto cell13 = cur->is_null(1, 1);
    REQUIRE_FALSE(cell13.has_error());
    CHECK(cell13.value());
    auto cell14 = cur->get<double>(2, 1);
    REQUIRE_FALSE(cell14.has_error());
    CHECK(cell14.value() == Catch::Approx(2.0));
    // row id=3: bound 3 in a, NULL literal in b
    auto cell15 = cur->get<int64_t>(1, 2);
    REQUIRE_FALSE(cell15.has_error());
    CHECK(cell15.value() == 3);
    auto cell16 = cur->is_null(2, 2);
    REQUIRE_FALSE(cell16.has_error());
    CHECK(cell16.value());
}

TEST_CASE("integration::cpp::insert_param_retype::null_parameter_keeps_a_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/null_param"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, null_value(resource)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell17 = cur->is_null(1, 0);
    REQUIRE_FALSE(cell17.has_error());
    REQUIRE_FALSE(cell17.value());
    auto cell18 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell18.has_error());
    CHECK(cell18.value() == 7);
    auto cell19 = cur->is_null(1, 1);
    REQUIRE_FALSE(cell19.has_error());
    CHECK(cell19.value());
}

TEST_CASE("integration::cpp::insert_param_retype::matching_types_take_no_retype") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/no_retype"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 1)}, {2, i64(resource, 2)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (7, $1), (8, $2);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell20 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell20.has_error());
    CHECK(cell20.value() == 7);
    auto cell21 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell21.has_error());
    CHECK(cell21.value() == 1);
    auto cell22 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell22.has_error());
    CHECK(cell22.value() == 8);
    auto cell23 = cur->get<int64_t>(1, 1);
    REQUIRE_FALSE(cell23.has_error());
    CHECK(cell23.value() == 2);
}

TEST_CASE("integration::cpp::insert_param_retype::parameter_between_text_and_integer_rows_into_text") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/param_between_text_rows"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, s TEXT);")->is_success());

    params_t params{{1, i64(resource, 42)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, s) VALUES (1, 'a'), (2, $1), (3, 7);",
                                             params);
    INFO("error: " << (insert->is_error() ? insert->get_error().what : "none"));
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, s FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell24 = cur->get<std::string_view>(1, 0);
    REQUIRE_FALSE(cell24.has_error());
    CHECK(cell24.value() == "a");
    auto cell25 = cur->get<std::string_view>(1, 1);
    REQUIRE_FALSE(cell25.has_error());
    CHECK(cell25.value() == "42");
    auto cell26 = cur->get<std::string_view>(1, 2);
    REQUIRE_FALSE(cell26.has_error());
    CHECK(cell26.value() == "7");
}

TEST_CASE("integration::cpp::insert_param_retype::parameter_between_quoted_and_integer_rows_into_integer") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/param_between_quoted_rows"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, n INTEGER);")->is_success());

    params_t params{{1, i64(resource, 11)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, n) VALUES (6, '10'), (7, $1), (8, 12);",
                                             params);
    INFO("error: " << (insert->is_error() ? insert->get_error().what : "none"));
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, n FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell27 = cur->get<int32_t>(1, 0);
    REQUIRE_FALSE(cell27.has_error());
    CHECK(cell27.value() == 10);
    auto cell28 = cur->get<int32_t>(1, 1);
    REQUIRE_FALSE(cell28.has_error());
    CHECK(cell28.value() == 11);
    auto cell29 = cur->get<int32_t>(1, 2);
    REQUIRE_FALSE(cell29.has_error());
    CHECK(cell29.value() == 12);
}
