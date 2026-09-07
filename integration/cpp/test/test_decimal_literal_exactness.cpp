#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// A bare fractional literal fell through to string_to_double: `0.1` entered the plan as
// 0.1000000000000000055511151231257827, and the write-path cast scaled that into NUMERIC(38,20)
// as 10000000000000000555 instead of 10000000000000000000.

using namespace test_helpers;

namespace {

    components::types::int128_t pow10(int n) {
        components::types::int128_t v{1};
        for (int i = 0; i < n; ++i) {
            v *= 10;
        }
        return v;
    }

    // 0.12345678901234567890 at scale 20 -- 20 significant digits, past a double's 17.
    components::types::int128_t twenty_digit_fraction() {
        components::types::int128_t v{1234567890123456789LL};
        v *= 10;
        return v;
    }

}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_bare_fractional_literal_reaches_a_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/numeric128"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,20));")->is_success());

    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, d) VALUES (1, 0.1), "
                 "(2, 0.12345678901234567890), "
                 "(3, 8.75);")
                ->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    // NUMERIC(38,20) is INT128-backed; the cursor exposes the scaled payload (value * 10^20).
    CHECK(cur->value(0, 0).value<components::types::int128_t>() == pow10(19));
    CHECK(cur->value(0, 1).value<components::types::int128_t>() == twenty_digit_fraction());
    CHECK(cur->value(0, 2).value<components::types::int128_t>() == components::types::int128_t{875} * pow10(18));
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::an_int64_backed_column_keeps_its_cents") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/numeric64"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(18,6));")->is_success());

    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, d) VALUES (1, 0.000001), "
                 "(2, 123456789012.345678), "
                 "(3, -0.1);")
                ->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(0, 1).value<int64_t>() == 123456789012345678LL);
    CHECK(cur->value(0, 2).value<int64_t>() == -100000LL);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_literal_too_wide_for_the_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/range"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(5,2));")->is_success());

    // PostgreSQL 18.6 ("Numeric Types"): refused only when digits left of the point exceed
    // precision minus scale, so 1234.5 is refused by NUMERIC(5,2) while 1.005 rounds to 1.01.
    CHECK(exec(d, "INSERT INTO w.t (id, d) VALUES (1, 1234.5);")->is_error());
    REQUIRE(exec(d, "INSERT INTO w.t (id, d) VALUES (2, 1.005), (3, 999.99);")->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 101);
    CHECK(cur->value(0, 1).value<int64_t>() == 99999);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::nulls_and_other_targets_are_left_alone") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/edges"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,20), f DOUBLE PRECISION);")->is_success());

    // The DOUBLE column takes the same literal and must stay a plain double.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, d, f) VALUES (1, 0.12345678901234567890, 0.5), "
                 "(2, NULL, 0.25);")
                ->is_success());

    auto cur = exec(d, "SELECT d, f FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<components::types::int128_t>() == twenty_digit_fraction());
    CHECK(cur->value(0, 1).is_null());
    // WithinULP(x, 0) is exact equality without a float `==`, which -Wfloat-equal refuses:
    // this case is about the DOUBLE column staying exact, so Approx would defeat it.
    CHECK_THAT(cur->value(1, 0).value<double>(), Catch::Matchers::WithinULP(0.5, 0));
    CHECK_THAT(cur->value(1, 1).value<double>(), Catch::Matchers::WithinULP(0.25, 0));
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::an_integer_literal_in_the_same_column_still_stores") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/mixed"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(18,6));")->is_success());

    REQUIRE(exec(d, "INSERT INTO w.t (id, d) VALUES (1, 0.5), (2, 7);")->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int64_t>() == 500000);
    CHECK(cur->value(0, 1).value<int64_t>() == 7000000);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_literal_beside_a_bound_parameter_stays_exact") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/params"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,20));")->is_success());

    // A parameterised INSERT rebuilds its chunks at bind time; the parameter leads on purpose so
    // the literal is not the chunk's first column.
    std::vector<std::pair<size_t, components::types::logical_value_t>> params{
        {1, components::types::logical_value_t{resource, static_cast<int64_t>(1)}}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES ($1, 0.12345678901234567890);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 1);
    CHECK(cur->value(1, 0).value<components::types::int128_t>() == twenty_digit_fraction());
}
