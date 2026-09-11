#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/types/logical_value.hpp>

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

    using param_t = std::pair<size_t, components::types::logical_value_t>;
    using params_t = std::vector<param_t>;

    components::types::logical_value_t i64(std::pmr::memory_resource* resource, int64_t v) {
        return components::types::logical_value_t{resource, v};
    }

    components::types::logical_value_t f64(std::pmr::memory_resource* resource, double v) {
        return components::types::logical_value_t{resource, v};
    }

    components::types::logical_value_t null_value(std::pmr::memory_resource* resource) {
        return components::types::logical_value_t{resource, components::types::logical_type::NA};
    }

} // namespace

TEST_CASE("integration::cpp::insert_param_retype::null_literal_before_a_parameter") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/null_literal_first"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 42)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, NULL), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).is_null());
    CHECK(cur->value(0, 1).value<int64_t>() == 2);
    REQUIRE_FALSE(cur->value(1, 1).is_null());
    CHECK(cur->value(1, 1).value<int64_t>() == 42);
}

TEST_CASE("integration::cpp::insert_param_retype::literal_before_a_wider_typed_parameter") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/literal_first_wider"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d DOUBLE);")->is_success());

    params_t params{{1, f64(resource, 3.5)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).value<double>() == Catch::Approx(7.0));
    CHECK(cur->value(0, 1).value<int64_t>() == 2);
    CHECK(cur->value(1, 1).value<double>() == Catch::Approx(3.5));
}

TEST_CASE("integration::cpp::insert_param_retype::narrower_parameter_keeps_a_wider_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/narrow_param_wide_literal"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d DOUBLE);")->is_success());

    params_t params{{1, i64(resource, 3)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7.5), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(1, 0).value<double>() == Catch::Approx(7.5));
    CHECK(cur->value(1, 1).value<double>() == Catch::Approx(3.0));
}

TEST_CASE("integration::cpp::insert_param_retype::several_rows_and_columns_at_once") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/rows_and_columns"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
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
    CHECK(cur->value(1, 0).value<int64_t>() == 10);
    CHECK(cur->value(2, 0).value<double>() == Catch::Approx(1.5));
    // row id=2: NULL literal in a, bound 2 -> 2.0 in b
    CHECK(cur->value(1, 1).is_null());
    CHECK(cur->value(2, 1).value<double>() == Catch::Approx(2.0));
    // row id=3: bound 3 in a, NULL literal in b
    CHECK(cur->value(1, 2).value<int64_t>() == 3);
    CHECK(cur->value(2, 2).is_null());
}

TEST_CASE("integration::cpp::insert_param_retype::null_parameter_keeps_a_literal") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/null_param"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, null_value(resource)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (1, 7), (2, $1);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE_FALSE(cur->value(1, 0).is_null());
    CHECK(cur->value(1, 0).value<int64_t>() == 7);
    CHECK(cur->value(1, 1).is_null());
}

TEST_CASE("integration::cpp::insert_param_retype::matching_types_take_no_retype") {
    auto config = make_test_config(integration_fixture_path("test_insert_param_retype/no_retype"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d BIGINT);")->is_success());

    params_t params{{1, i64(resource, 1)}, {2, i64(resource, 2)}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES (7, $1), (8, $2);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 7);
    CHECK(cur->value(1, 0).value<int64_t>() == 1);
    CHECK(cur->value(0, 1).value<int64_t>() == 8);
    CHECK(cur->value(1, 1).value<int64_t>() == 2);
}
