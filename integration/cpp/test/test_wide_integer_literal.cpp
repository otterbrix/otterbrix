#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/bigint"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
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
    CHECK(cur->value(0, 0).value<int64_t>() == INT64_MAX_V);
    CHECK(cur->value(0, 1).value<int64_t>() == 2147483648LL);
    CHECK(cur->value(0, 2).value<int64_t>() == INT64_MIN_V);
    CHECK(cur->value(0, 3).value<int64_t>() == 123456789012345678LL);
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_wide_literal_in_a_predicate_matches_the_stored_row") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/predicate"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
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
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a = 4294967295;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 2);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a = 123456789012345678;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 3);
    }
    {
        auto cur = exec(d, "SELECT id FROM w.t WHERE a > 1000000000000 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
        CHECK(cur->value(0, 1).value<int64_t>() == 3);
    }
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_past_int64_reaches_a_wide_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/numeric128"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, d NUMERIC(38,0));")->is_success());

    // NUMERIC(38,0) is 128-bit scaled, so this fits exactly only if the lexer keeps it an exact integer.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, d) VALUES (1, 12345678901234567890123456789), "
                 "(2, -12345678901234567890123456789);")
                ->is_success());

    auto cur = exec(d, "SELECT d FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<components::types::int128_t>() == past_int64());
    CHECK(cur->value(0, 1).value<components::types::int128_t>() == -past_int64());
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_wider_than_its_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/column_range"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, small INTEGER);")->is_success());

    // The narrowing-cast refusal is independent of the lexer fix; it only fires once literals arrive intact.
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (1, 9223372036854775807);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (2, 2147483648);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (3, -2147483649);")->is_error());

    REQUIRE(exec(d, "INSERT INTO w.t (id, small) VALUES (4, 2147483647), (5, -2147483648);")->is_success());
    auto cur = exec(d, "SELECT small FROM w.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(0, 0).value<int32_t>() == 2147483647);
    CHECK(cur->value(0, 1).value<int32_t>() == -2147483648);
}

TEST_CASE("integration::cpp::test_wide_integer_literal::a_literal_no_type_can_hold_is_refused_loudly") {
    auto config = make_test_config(integration_fixture_path("test_wide_integer_literal/overflow"), true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, a BIGINT);")->is_success());

    // 40 digits is past int128, the widest exact integer this engine has, so it must fail, not round or wrap.
    CHECK(exec(d, "INSERT INTO w.t (id, a) VALUES (1, 1234567890123456789012345678901234567890);")->is_error());

    REQUIRE(exec(d, "INSERT INTO w.t (id, a) VALUES (2, 9223372036854775807);")->is_success());
    auto cur = exec(d, "SELECT a FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == INT64_MAX_V);
}
