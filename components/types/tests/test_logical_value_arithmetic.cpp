#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <core/arithmetic_op.hpp>
#include <memory_resource>
#include <stdexcept>

using namespace components::types;
using namespace core::date;
using components::vector::arithmetic_op;

// ---------------------------------------------------------------------------
// Characterization of boxed scalar arithmetic.
//
// These cases pin the answer of every arm that the five public entry points
// (sum/subtract/mult/divide/modulus) reached, so collapsing them into one
// `arithmetic()` entry was provably behaviour-preserving. Two properties matter
// most and are asserted throughout:
//   * a NULL operand yields NA -- three-valued logic, NOT an error;
//   * an operand pair no arm handles is a real failure -- it used to throw.
// ---------------------------------------------------------------------------

namespace {
    logical_value_t na(std::pmr::memory_resource* r) {
        return logical_value_t{r, complex_logical_type{logical_type::NA}};
    }

    core::result_wrapper_t<logical_value_t>
    arith(std::pmr::memory_resource* r, arithmetic_op op, const logical_value_t& lhs, const logical_value_t& rhs) {
        return logical_value_t::arithmetic(r, op, lhs, rhs);
    }
} // namespace

TEST_CASE("components::types::logical_value::arithmetic_null_operand_is_na") {
    std::pmr::monotonic_buffer_resource resource;
    logical_value_t one(&resource, int32_t{1});

    constexpr arithmetic_op ops[] = {arithmetic_op::add,
                                     arithmetic_op::subtract,
                                     arithmetic_op::multiply,
                                     arithmetic_op::divide,
                                     arithmetic_op::mod};
    for (auto op : ops) {
        auto lhs_null = arith(&resource, op, na(&resource), one);
        REQUIRE_FALSE(lhs_null.has_error());
        CHECK(lhs_null.value().is_null());

        auto rhs_null = arith(&resource, op, one, na(&resource));
        REQUIRE_FALSE(rhs_null.has_error());
        CHECK(rhs_null.value().is_null());

        auto both_null = arith(&resource, op, na(&resource), na(&resource));
        REQUIRE_FALSE(both_null.has_error());
        CHECK(both_null.value().is_null());
    }
}

TEST_CASE("components::types::logical_value::arithmetic_numeric_arms") {
    std::pmr::monotonic_buffer_resource resource;

    SECTION("same-type integers") {
        logical_value_t six(&resource, int32_t{6});
        logical_value_t three(&resource, int32_t{3});

        auto add = arith(&resource, arithmetic_op::add, six, three);
        REQUIRE_FALSE(add.has_error());
        CHECK(add.value().type().type() == logical_type::INTEGER);
        CHECK(add.value().value<int32_t>() == 9);

        auto sub = arith(&resource, arithmetic_op::subtract, six, three);
        REQUIRE_FALSE(sub.has_error());
        CHECK(sub.value().value<int32_t>() == 3);

        auto mul = arith(&resource, arithmetic_op::multiply, six, three);
        REQUIRE_FALSE(mul.has_error());
        CHECK(mul.value().value<int32_t>() == 18);

        auto div = arith(&resource, arithmetic_op::divide, six, three);
        REQUIRE_FALSE(div.has_error());
        CHECK(div.value().value<int32_t>() == 2);

        auto mod = arith(&resource, arithmetic_op::mod, six, three);
        REQUIRE_FALSE(mod.has_error());
        CHECK(mod.value().value<int32_t>() == 0);
    }

    SECTION("mixed numeric operands are promoted to the wider type") {
        logical_value_t small(&resource, int32_t{7});
        logical_value_t big(&resource, int64_t{2});
        auto add = arith(&resource, arithmetic_op::add, small, big);
        REQUIRE_FALSE(add.has_error());
        CHECK(add.value().type().type() == logical_type::BIGINT);
        CHECK(add.value().value<int64_t>() == 9);
    }

    SECTION("floating point") {
        logical_value_t a(&resource, double{7.5});
        logical_value_t b(&resource, double{2.5});
        auto div = arith(&resource, arithmetic_op::divide, a, b);
        REQUIRE_FALSE(div.has_error());
        CHECK(div.value().type().type() == logical_type::DOUBLE);
        CHECK(div.value().value<double>() == Catch::Approx(3.0));
    }

    SECTION("128-bit") {
        logical_value_t a(&resource, int128_t{100});
        logical_value_t b(&resource, int128_t{7});
        auto mod = arith(&resource, arithmetic_op::mod, a, b);
        REQUIRE_FALSE(mod.has_error());
        CHECK(mod.value().type().type() == logical_type::HUGEINT);
        CHECK(mod.value().value<int128_t>() == int128_t{2});
    }
}

TEST_CASE("components::types::logical_value::arithmetic_string_concatenation") {
    // `+` over STRING_LITERAL is string concatenation -- an arm only `add` has.
    std::pmr::monotonic_buffer_resource resource;
    logical_value_t a(&resource, std::string{"ab"});
    logical_value_t b(&resource, std::string{"cd"});

    auto cat = arith(&resource, arithmetic_op::add, a, b);
    REQUIRE_FALSE(cat.has_error());
    CHECK(cat.value().type().type() == logical_type::STRING_LITERAL);
    CHECK(cat.value().value<std::string_view>() == "abcd");

    // Every other operator over strings has no arm at all.
    CHECK(arith(&resource, arithmetic_op::subtract, a, b).has_error());
    CHECK(arith(&resource, arithmetic_op::multiply, a, b).has_error());
}

TEST_CASE("components::types::logical_value::arithmetic_temporal_arms") {
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t day(&resource, date_t{days{100}});
    logical_value_t one_day(&resource, interval_t{microseconds{0}, days{1}, months{0}});

    SECTION("DATE +/- INTERVAL stays a DATE, either operand order") {
        auto plus = arith(&resource, arithmetic_op::add, day, one_day);
        REQUIRE_FALSE(plus.has_error());
        CHECK(plus.value().type().type() == logical_type::DATE);
        CHECK(plus.value().value<date_t>().value.count() == 101);

        auto commuted = arith(&resource, arithmetic_op::add, one_day, day);
        REQUIRE_FALSE(commuted.has_error());
        CHECK(commuted.value().value<date_t>().value.count() == 101);

        auto minus = arith(&resource, arithmetic_op::subtract, day, one_day);
        REQUIRE_FALSE(minus.has_error());
        CHECK(minus.value().value<date_t>().value.count() == 99);
    }

    SECTION("DATE - DATE is an INTERVAL of days") {
        logical_value_t other(&resource, date_t{days{90}});
        auto diff = arith(&resource, arithmetic_op::subtract, day, other);
        REQUIRE_FALSE(diff.has_error());
        REQUIRE(diff.value().type().type() == logical_type::INTERVAL);
        CHECK(diff.value().value<interval_t>().day.count() == 10);
    }

    SECTION("INTERVAL +/- INTERVAL") {
        auto sum = arith(&resource, arithmetic_op::add, one_day, one_day);
        REQUIRE_FALSE(sum.has_error());
        CHECK(sum.value().value<interval_t>().day.count() == 2);

        auto diff = arith(&resource, arithmetic_op::subtract, one_day, one_day);
        REQUIRE_FALSE(diff.has_error());
        CHECK(diff.value().value<interval_t>().day.count() == 0);
    }

    SECTION("INTERVAL scaled by a number, either operand order") {
        logical_value_t three(&resource, int32_t{3});
        auto scaled = arith(&resource, arithmetic_op::multiply, one_day, three);
        REQUIRE_FALSE(scaled.has_error());
        REQUIRE(scaled.value().type().type() == logical_type::INTERVAL);
        CHECK(scaled.value().value<interval_t>().day.count() == 3);

        auto divided = arith(&resource, arithmetic_op::divide, scaled.value(), three);
        REQUIRE_FALSE(divided.has_error());
        CHECK(divided.value().value<interval_t>().day.count() == 1);
    }

    SECTION("TIME +/- INTERVAL wraps around the day") {
        logical_value_t noon(&resource, core::date::time_t{microseconds{12} * 3600 * 1000000});
        logical_value_t half_day(&resource, interval_t{microseconds{13} * 3600 * 1000000, days{0}, months{0}});
        auto wrapped = arith(&resource, arithmetic_op::add, noon, half_day);
        REQUIRE_FALSE(wrapped.has_error());
        REQUIRE(wrapped.value().type().type() == logical_type::TIME);
        CHECK(wrapped.value().value<core::date::time_t>().value.count() == 3600LL * 1000000);
    }

    SECTION("TIMESTAMP +/- INTERVAL, and TIMESTAMP - TIMESTAMP") {
        logical_value_t ts(&resource, timestamp_t{microseconds{1000}});
        logical_value_t iv(&resource, interval_t{microseconds{500}, days{0}, months{0}});
        auto later = arith(&resource, arithmetic_op::add, ts, iv);
        REQUIRE_FALSE(later.has_error());
        REQUIRE(later.value().type().type() == logical_type::TIMESTAMP);
        CHECK(later.value().value<timestamp_t>().value.count() == 1500);

        auto delta = arith(&resource, arithmetic_op::subtract, later.value(), ts);
        REQUIRE_FALSE(delta.has_error());
        REQUIRE(delta.value().type().type() == logical_type::INTERVAL);
        CHECK(delta.value().value<interval_t>().time.count() == 500);
    }
}

TEST_CASE("components::types::logical_value::arithmetic_numeric_times_interval_is_an_interval") {
    // Regression: `3 * INTERVAL` answered a garbage INTEGER. The per-type switch dispatches on
    // the LEFT operand and then reads BOTH operands through that type, so an INTEGER left
    // operand read the INTERVAL's heap-vector POINTER as an int32 -- and the commutative
    // `numeric * INTERVAL` arm below the switch was unreachable. Reading the result back as
    // an interval then dereferenced that truncated pointer (SIGSEGV). The planner's
    // arithmetic_result_type(INTEGER, INTERVAL, multiply) says INTERVAL, so the scalar path
    // also contradicted the plan. The switch is sound only for two operands of the SAME
    // type -- which, after numeric promotion, is exactly when it may run.
    std::pmr::monotonic_buffer_resource resource;
    logical_value_t three(&resource, int32_t{3});
    logical_value_t one_day(&resource, interval_t{microseconds{0}, days{1}, months{0}});

    auto commuted = arith(&resource, arithmetic_op::multiply, three, one_day);
    REQUIRE_FALSE(commuted.has_error());
    REQUIRE(commuted.value().type().type() == logical_type::INTERVAL);
    CHECK(commuted.value().value<interval_t>().day.count() == 3);

    // A numeric left operand mixed with a non-numeric right one has no other arm: it is an
    // error, not a misread. arithmetic_result_type agrees (NA for numeric / INTERVAL).
    CHECK(arith(&resource, arithmetic_op::divide, three, one_day).has_error());
    CHECK(arith(&resource, arithmetic_op::add, three, one_day).has_error());
}

TEST_CASE("components::types::logical_value::arithmetic_by_a_zero_divisor_yields_a_zero_of_the_type") {
    // Pinned as-is, NOT endorsed: SQL says division by zero is an error, this answers 0.
    //
    // Regression for `%`: the zero-divisor guard covered `/` only, so `6 % 0` reached `std::modulus<>`
    // and executed an integer remainder by zero -- undefined behaviour, which is SIGFPE on x86
    // and answered 6 on the AArch64 this was found on. `%` needs the same guard `/` has: the two
    // share one divisor, so they share one answer for a zero one.
    std::pmr::monotonic_buffer_resource resource;
    logical_value_t six(&resource, int32_t{6});
    logical_value_t zero(&resource, int32_t{0});

    auto div = arith(&resource, arithmetic_op::divide, six, zero);
    REQUIRE_FALSE(div.has_error());
    REQUIRE(div.value().type().type() == logical_type::INTEGER);
    CHECK_FALSE(div.value().is_null());
    CHECK(div.value().value<int32_t>() == 0);

    auto mod = arith(&resource, arithmetic_op::mod, six, zero);
    REQUIRE_FALSE(mod.has_error());
    REQUIRE(mod.value().type().type() == logical_type::INTEGER);
    CHECK(mod.value().value<int32_t>() == 0);
}

TEST_CASE("components::types::logical_value::arithmetic_unhandled_operands_are_an_error") {
    // Every one of these reached a `throw std::runtime_error` before M7. Under the
    // executor's coroutines a throw becomes an empty unhandled_exception() -> SIGABRT,
    // so they have to arrive as a value the caller can propagate.
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t day(&resource, date_t{days{1}});
    logical_value_t one(&resource, int32_t{1});
    logical_value_t iv(&resource, interval_t{microseconds{1}, days{0}, months{0}});
    logical_value_t str(&resource, std::string{"x"});

    auto check = [](const core::result_wrapper_t<logical_value_t>& r) {
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::arithmetics_failure);
    };

    check(arith(&resource, arithmetic_op::add, day, one));       // DATE + INTEGER
    check(arith(&resource, arithmetic_op::subtract, day, one));  // DATE - INTEGER
    check(arith(&resource, arithmetic_op::multiply, day, one));  // DATE * INTEGER
    check(arith(&resource, arithmetic_op::divide, day, one));    // DATE / INTEGER
    check(arith(&resource, arithmetic_op::mod, day, one));       // DATE % INTEGER
    check(arith(&resource, arithmetic_op::mod, iv, one));        // INTERVAL % INTEGER
    check(arith(&resource, arithmetic_op::mod, str, str));       // STRING % STRING

    // No floating-point arm for `%` -- the gap components/vector's
    // test_arithmetic_result_type.cpp documents. It is a real error, not NA.
    logical_value_t d1(&resource, double{7.5});
    logical_value_t d2(&resource, double{2.0});
    check(arith(&resource, arithmetic_op::mod, d1, d2));
}
