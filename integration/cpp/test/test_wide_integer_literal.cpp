#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>

#include <cstdint>
#include <limits>
#include <string>

// core_YYSTYPE::ival is a 32-bit int; process_integer_literal (scan.l) left the int32 guard
// behind an undefined `#ifdef HAVE_LONG_INT_64`, so strtol()'s 64-bit result was silently
// truncated in the lexer. Rejected fix: enabling the guard routes overflow through atof(), which
// rounds instead of keeping exact digits; FCONST carries them, so tests assert equality past a
// double's 53-bit mantissa.

using namespace test_helpers;

namespace {

    constexpr int64_t INT64_MAX_V = std::numeric_limits<int64_t>::max();
    constexpr int64_t INT64_MIN_V = std::numeric_limits<int64_t>::min();

    // 12345678901234567890123456789 -- 29 digits, past int64, inside NUMERIC(38,0).
    components::types::int128_t past_int64() {
        components::types::int128_t v{1234567890123456789LL};
        v *= 10000000000LL;
        v += 123456789LL;
        return v;
    }

} // namespace

TEST_CASE("integration::cpp::test_wide_integer_literal::int64_literals_reach_a_bigint_column_intact") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/bigint"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, a BIGINT);")->is_success());

    // Row 3 (int64 floor) is reached as unary minus over one-past-ceiling, exercising the negation path.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, a) VALUES "
                 "(1, 9223372036854775807), "
                 "(2, 2147483648), "
                 "(3, -9223372036854775808), "
                 "(4, 123456789012345678);")
                ->is_success());

    auto cur = exec(d, "SELECT a FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 4);
    auto cell0 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    CHECK(cell0.value() == INT64_MAX_V);
    auto cell1 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell1.has_error());
    CHECK(cell1.value() == 2147483648LL);
    auto cell2 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell2.has_error());
    CHECK(cell2.value() == INT64_MIN_V);
    auto cell3 = cur->get<int64_t>(0, 3);
    REQUIRE_FALSE(cell3.has_error());
    CHECK(cell3.value() == 123456789012345678LL);
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_wide_literal_in_a_predicate_matches_the_stored_row") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/predicate"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, a BIGINT);")->is_success());

    // 9223372036854775807 and 4294967295 share the same low 32 bits, so a truncating lexer would
    // store and compare both as -1, and a single-row assertion wouldn't have caught that.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, a) VALUES (1, 9223372036854775807), (2, 4294967295), "
                 "(3, 123456789012345678);")
                ->is_success());

    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a = 9223372036854775807;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell4 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        CHECK(cell4.value() == 1);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a = 4294967295;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell5 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell5.has_error());
        CHECK(cell5.value() == 2);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a = 123456789012345678;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell6 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell6.has_error());
        CHECK(cell6.value() == 3);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a > 1000000000000 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cell7 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        CHECK(cell7.value() == 1);
        auto cell8 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell8.has_error());
        CHECK(cell8.value() == 3);
    }
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_past_int64_reaches_a_wide_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/numeric128"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,0));")->is_success());

    // NUMERIC(38,0) is 128-bit scaled, so this fits exactly only if the lexer keeps it an exact integer.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, d) VALUES (1, 12345678901234567890123456789), "
                 "(2, -12345678901234567890123456789);")
                ->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell9 = cur->get<components::types::int128_t>(0, 0);
    REQUIRE_FALSE(cell9.has_error());
    CHECK(cell9.value() == past_int64());
    auto cell10 = cur->get<components::types::int128_t>(0, 1);
    REQUIRE_FALSE(cell10.has_error());
    CHECK(cell10.value() == -past_int64());
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_wider_than_its_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/column_range"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, small INTEGER);")->is_success());

    // The narrowing-cast refusal is independent of the lexer fix; it only fires once literals arrive intact.
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (1, 9223372036854775807);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (2, 2147483648);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (3, -2147483649);")->is_error());

    REQUIRE(exec(d, "INSERT INTO w.t (id, small) VALUES (4, 2147483647), (5, -2147483648);")->is_success());
    auto cur = exec(d, "SELECT small FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell11 = cur->get<int32_t>(0, 0);
    REQUIRE_FALSE(cell11.has_error());
    CHECK(cell11.value() == 2147483647);
    auto cell12 = cur->get<int32_t>(0, 1);
    REQUIRE_FALSE(cell12.has_error());
    CHECK(cell12.value() == -2147483648);
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_no_type_can_hold_is_refused_loudly") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/overflow"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE SCHEMA w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, a BIGINT);")->is_success());

    // 40 digits is past int128, the widest exact integer this engine has, so it must fail, not round or wrap.
    CHECK(exec(d, "INSERT INTO w.t (id, a) VALUES (1, 1234567890123456789012345678901234567890);")->is_error());

    REQUIRE(exec(d, "INSERT INTO w.t (id, a) VALUES (2, 9223372036854775807);")->is_success());
    auto cur = exec(d, "SELECT a FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell13 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell13.has_error());
    CHECK(cell13.value() == INT64_MAX_V);
}
