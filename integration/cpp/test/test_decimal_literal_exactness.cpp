#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector_builders.hpp>

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

} // namespace

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_bare_fractional_literal_reaches_a_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/numeric128"));
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
    auto cell0 = cur->get<components::types::int128_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    CHECK(cell0.value() == pow10(19));
    auto cell1 = cur->get<components::types::int128_t>(0, 1);
    REQUIRE_FALSE(cell1.has_error());
    CHECK(cell1.value() == twenty_digit_fraction());
    auto cell2 = cur->get<components::types::int128_t>(0, 2);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == components::types::int128_t{875} * pow10(18));
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::an_int64_backed_column_keeps_its_cents") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/numeric64"));
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
    auto cell3 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell3.has_error());
    CHECK(cell3.value() == 1);
    auto cell4 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell4.has_error());
    CHECK(cell4.value() == 123456789012345678LL);
    auto cell5 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell5.has_error());
    CHECK(cell5.value() == -100000LL);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_literal_too_wide_for_the_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/range"));
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
    auto cell6 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell6.has_error());
    CHECK(cell6.value() == 101);
    auto cell7 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell7.has_error());
    CHECK(cell7.value() == 99999);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::nulls_and_other_targets_are_left_alone") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/edges"));
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
    auto cell8 = cur->get<components::types::int128_t>(0, 0);
    REQUIRE_FALSE(cell8.has_error());
    CHECK(cell8.value() == twenty_digit_fraction());
    auto cell9 = cur->is_null(0, 1);
    REQUIRE_FALSE(cell9.has_error());
    CHECK(cell9.value());
    // WithinULP(x, 0) is exact equality without a float `==`, which -Wfloat-equal refuses:
    // this case is about the DOUBLE column staying exact, so Approx would defeat it.
    auto cell10 = cur->get<double>(1, 0);
    REQUIRE_FALSE(cell10.has_error());
    CHECK_THAT(cell10.value(), Catch::Matchers::WithinULP(0.5, 0));
    auto cell11 = cur->get<double>(1, 1);
    REQUIRE_FALSE(cell11.has_error());
    CHECK_THAT(cell11.value(), Catch::Matchers::WithinULP(0.25, 0));
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::an_integer_literal_in_the_same_column_still_stores") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/mixed"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(18,6));")->is_success());

    REQUIRE(exec(d, "INSERT INTO w.t (id, d) VALUES (1, 0.5), (2, 7);")->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell12 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell12.has_error());
    CHECK(cell12.value() == 500000);
    auto cell13 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell13.has_error());
    CHECK(cell13.value() == 7000000);
}

TEST_CASE("integration::cpp::test_decimal_literal_exactness::a_literal_beside_a_bound_parameter_stays_exact") {
    auto config = make_test_config(integration_fixture_path("test_decimal_literal_exactness/params"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto* resource = d->resource();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,20));")->is_success());

    // A parameterised INSERT rebuilds its chunks at bind time; the parameter leads on purpose so
    // the literal is not the chunk's first column.
    std::vector<std::pair<size_t, components::vector::vector_t>> params{
        {1, components::vector::make_scalar(resource, static_cast<int64_t>(1))}};
    auto insert = d->execute_sql_with_params(otterbrix::session_id_t(),
                                             "INSERT INTO w.t (id, d) VALUES ($1, 0.12345678901234567890);",
                                             params);
    REQUIRE(insert->is_success());

    auto cur = exec(d, "SELECT id, d FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell14 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell14.has_error());
    CHECK(cell14.value() == 1);
    auto cell15 = cur->get<components::types::int128_t>(1, 0);
    REQUIRE_FALSE(cell15.has_error());
    CHECK(cell15.value() == twenty_digit_fraction());
}
