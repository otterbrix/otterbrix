#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>

#include <cstdint>
#include <limits>
#include <string>

// AN INTEGER LITERAL MUST ARRIVE AS THE NUMBER THAT WAS WRITTEN.
//
// The core scanner hands the parser an `int` (core_YYSTYPE::ival in
// components/sql/parser/scanner.h, and the bison %union built on top of it), so every
// integer literal has to be checked against int32 BEFORE it is stored there. That check
// lived behind `#ifdef HAVE_LONG_INT_64` in process_integer_literal
// (components/sql/parser/scan.l) and nothing in this project defines that macro, so the
// 64-bit result of strtol() was assigned straight into the 32-bit field:
//
//   INSERT INTO t (a) VALUES (9223372036854775807);   -- stored -1
//   INSERT INTO t (a) VALUES (123456789012345678);    -- stored -1506741426
//
// No statement failed. There is no column type that escapes it, because the damage is done
// in the LEXER, before any type is known. That is a silently wrong answer, which is why
// every case here checks the VALUE and not merely that the statement succeeded.
//
// The other half of the same defect is what the repair must NOT do. Simply enabling the
// guard sends an oversize literal down the FCONST path, and FCONST is read with atof() —
// so 9223372036854775807 would come back as 9223372036854775808 (the nearest double) and
// the wrong answer would merely change shape. So these cases pin EXACT equality on values
// that no double can represent: every one of them has more significant digits than the 53
// bits of a double's mantissa.

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

    // Row 1 is int64's ceiling, row 2 the first value past int32 (the exact boundary the
    // scanner's guard is about), row 3 int64's floor -- which the grammar reaches as unary
    // minus over a literal that is itself one past int64's ceiling, so it exercises the
    // negation path too. Row 4 is the 18-digit value from the original report.
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

    // These two literals are DIFFERENT numbers that truncate to the SAME int32.
    // 9223372036854775807 is 0x7FFF'FFFF'FFFF'FFFF and 4294967295 is 0xFFFF'FFFF; both keep
    // 0xFFFFFFFF in their low word, so a truncating lexer stores AND compares both as -1, and
    // a predicate for either one matches both rows -- the wrong answer that a test asserting
    // only "one row came back for the value I asked for" would still miss, because the two
    // literals collapse to the same thing consistently.
    REQUIRE(exec(d,
                 "INSERT INTO w.t (id, a) VALUES (1, 9223372036854775807), (2, 4294967295), "
                 "(3, 123456789012345678);")
                ->is_success());

    // A literal on the right of a comparison travels the same lexer as the one that was
    // written. If it is truncated there, the predicate compares against a DIFFERENT number.
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
    // Ordering was a lie too: two of these are far above the bound and one is far below,
    // but as truncated int32s they were -1, -1 and -1506741426 -- all below it.
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

    // NUMERIC(38,0) stores a 128-bit scaled integer, so a 29-digit literal is a value the
    // column can hold exactly. Carrying it there requires the literal itself to survive as
    // an exact integer past int64 -- neither an int32 nor a double gets it here.
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

    // The narrowing cast the insert binding resolves (INTEGER target, wider source) DOES
    // refuse an out-of-range value -- it always did. It simply never saw one, because the
    // lexer had already cut the literal down to something that fit: 9223372036854775807
    // arrived as -1 and was stored without complaint. With the literal intact the refusal
    // finally fires, which is the difference between a wrong row and no row.
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (1, 9223372036854775807);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (2, 2147483648);")->is_error());
    CHECK(exec(d, "INSERT INTO w.t (id, small) VALUES (3, -2147483649);")->is_error());

    // The edges of INTEGER itself still go in, so the gate is a range check and not a ban
    // on wide literals.
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

    // 40 digits: past int128, which is the widest exact integer this engine has. There is
    // no representation left, so the statement must FAIL rather than store a rounded
    // double or a wrapped remainder.
    CHECK(exec(d, "INSERT INTO w.t (id, a) VALUES (1, 1234567890123456789012345678901234567890);")->is_error());

    // ...and the refusal costs exactly one statement: the session still answers.
    REQUIRE(exec(d, "INSERT INTO w.t (id, a) VALUES (2, 9223372036854775807);")->is_success());
    auto cur = exec(d, "SELECT a FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == INT64_MAX_V);
}
