#include <catch2/catch_test_macros.hpp>

#include <components/operators/resolve_operator.hpp>
#include <core/pmr.hpp>

using namespace components::operators;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {
    // The ONE arena this file builds DECIMALs on. create_decimal allocates only on its refusal
    // path, and that message belongs to the caller, so the caller has to name an arena it owns
    // rather than reach for the process-global one (rule 14).
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // create_decimal reports an out-of-window (width, scale) through core::error_t rather
    // than an assert that vanishes under NDEBUG. Every literal these tests use is inside
    // the window, so the helper checks the result and hands back the type.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

TEST_CASE("operators::resolve_operator: matching operand types") {
    complex_logical_type integer{logical_type::INTEGER};
    complex_logical_type bigint{logical_type::BIGINT};

    auto same = resolve_operator(operator_code::add, integer, integer);
    REQUIRE(same.has_value());
    REQUIRE(same->result.type() == logical_type::INTEGER);

    // Mixed widths are not an operator -- the caller has to unify them first.
    REQUIRE_FALSE(resolve_operator(operator_code::add, integer, bigint).has_value());
}

TEST_CASE("operators::resolve_operator: temporal rules give a result unrelated to the operands") {
    complex_logical_type date{logical_type::DATE};
    complex_logical_type timestamp{logical_type::TIMESTAMP};
    complex_logical_type interval{logical_type::INTERVAL};

    auto difference = resolve_operator(operator_code::subtract, date, date);
    REQUIRE(difference.has_value());
    REQUIRE(difference->result.type() == logical_type::INTERVAL);

    // ... while addition of an interval stays in the temporal type, either way round.
    auto shifted = resolve_operator(operator_code::add, timestamp, interval);
    REQUIRE(shifted.has_value());
    REQUIRE(shifted->result.type() == logical_type::TIMESTAMP);

    auto reversed = resolve_operator(operator_code::add, interval, timestamp);
    REQUIRE(reversed.has_value());
    REQUIRE(reversed->result.type() == logical_type::TIMESTAMP);

    // Subtraction is not commutative: an interval minus a date is nothing.
    REQUIRE_FALSE(resolve_operator(operator_code::subtract, interval, date).has_value());
    // Two different temporal types are not an operator either; they unify first.
    REQUIRE_FALSE(resolve_operator(operator_code::subtract, date, timestamp).has_value());
}

TEST_CASE("operators::resolve_operator: decimals match only on equal parameters") {
    auto ten_two = make_decimal(10, 2);
    auto ten_four = make_decimal(10, 4);

    REQUIRE(resolve_operator(operator_code::add, ten_two, ten_two).has_value());
    REQUIRE_FALSE(resolve_operator(operator_code::add, ten_two, ten_four).has_value());
}

TEST_CASE("operators::resolve_operator: unary") {
    complex_logical_type integer{logical_type::INTEGER};
    complex_logical_type date{logical_type::DATE};

    auto negated = resolve_operator(operator_code::negate, integer);
    REQUIRE(negated.has_value());
    REQUIRE(negated->result.type() == logical_type::INTEGER);

    REQUIRE_FALSE(resolve_operator(operator_code::negate, date).has_value());
    // A binary code never resolves through the unary lookup.
    REQUIRE_FALSE(resolve_operator(operator_code::add, integer).has_value());
    REQUIRE(arity_of(operator_code::negate) == operator_arity::unary);
    REQUIRE(arity_of(operator_code::add) == operator_arity::binary);
}

TEST_CASE("operators::resolve_operator: bitwise operators are integer-only") {
    complex_logical_type integer{logical_type::INTEGER};
    complex_logical_type real{logical_type::FLOAT};
    complex_logical_type boolean{logical_type::BOOLEAN};

    auto masked = resolve_operator(operator_code::bit_and, integer, integer);
    REQUIRE(masked.has_value());
    REQUIRE(masked->result.type() == logical_type::INTEGER);

    // The "same numeric type" rule that arithmetic uses must not reach these.
    REQUIRE_FALSE(resolve_operator(operator_code::bit_and, real, real).has_value());
    REQUIRE_FALSE(resolve_operator(operator_code::bit_or, boolean, boolean).has_value());
    REQUIRE_FALSE(resolve_operator(operator_code::bit_xor,
                                   make_decimal(10, 2),
                                   make_decimal(10, 2))
                      .has_value());

    auto inverted = resolve_operator(operator_code::bit_not, integer);
    REQUIRE(inverted.has_value());
    REQUIRE(inverted->result.type() == logical_type::INTEGER);
    REQUIRE_FALSE(resolve_operator(operator_code::bit_not, real).has_value());
    REQUIRE(arity_of(operator_code::bit_not) == operator_arity::unary);
    REQUIRE(arity_of(operator_code::shift_left) == operator_arity::binary);
}

TEST_CASE("operators::resolve_operator: comparisons return bool") {
    complex_logical_type integer{logical_type::INTEGER};
    complex_logical_type bigint{logical_type::BIGINT};
    complex_logical_type text{logical_type::STRING_LITERAL};

    for (auto code : {operator_code::equal,
                      operator_code::not_equal,
                      operator_code::less,
                      operator_code::less_equal,
                      operator_code::greater,
                      operator_code::greater_equal}) {
        auto compared = resolve_operator(code, integer, integer);
        REQUIRE(compared.has_value());
        REQUIRE(compared->result.type() == logical_type::BOOLEAN);
        // Like arithmetic, mixed operands unify before reaching the operator.
        REQUIRE_FALSE(resolve_operator(code, integer, bigint).has_value());
    }

    auto strings = resolve_operator(operator_code::less, text, text);
    REQUIRE(strings.has_value());
    REQUIRE(strings->result.type() == logical_type::BOOLEAN);

    // Decimals compare only on equal parameters, same as arithmetic.
    REQUIRE(resolve_operator(operator_code::equal,
                             make_decimal(10, 2),
                             make_decimal(10, 2))
                .has_value());
    REQUIRE_FALSE(resolve_operator(operator_code::equal,
                                   make_decimal(10, 2),
                                   make_decimal(10, 4))
                      .has_value());
}

TEST_CASE("operators::resolve_operator: logical operators take bool only") {
    complex_logical_type boolean{logical_type::BOOLEAN};
    complex_logical_type integer{logical_type::INTEGER};

    auto conjunction = resolve_operator(operator_code::logical_and, boolean, boolean);
    REQUIRE(conjunction.has_value());
    REQUIRE(conjunction->result.type() == logical_type::BOOLEAN);

    auto disjunction = resolve_operator(operator_code::logical_or, boolean, boolean);
    REQUIRE(disjunction.has_value());
    REQUIRE(disjunction->result.type() == logical_type::BOOLEAN);

    // An integer is not a truth value here -- that is what bit_or is for.
    REQUIRE_FALSE(resolve_operator(operator_code::logical_or, integer, integer).has_value());

    auto negated = resolve_operator(operator_code::logical_not, boolean);
    REQUIRE(negated.has_value());
    REQUIRE(negated->result.type() == logical_type::BOOLEAN);
    REQUIRE_FALSE(resolve_operator(operator_code::logical_not, integer).has_value());
}

// IS NULL reads the validity mask, so it accepts any value type -- and IS NOT NULL is
// not an operator at all, it is logical_not(is_null(x)).
TEST_CASE("operators::resolve_operator: is_null accepts any type") {
    for (auto type : {complex_logical_type{logical_type::INTEGER},
                      complex_logical_type{logical_type::STRING_LITERAL},
                      complex_logical_type{logical_type::TIMESTAMP},
                      make_decimal(10, 2)}) {
        auto tested = resolve_operator(operator_code::is_null, type);
        REQUIRE(tested.has_value());
        REQUIRE(tested->result.type() == logical_type::BOOLEAN);
    }

    REQUIRE_FALSE(resolve_operator(operator_code::is_null, complex_logical_type{logical_type::INVALID}).has_value());
    // It is unary: the binary lookup must not resolve it.
    REQUIRE_FALSE(resolve_operator(operator_code::is_null,
                                   complex_logical_type{logical_type::INTEGER},
                                   complex_logical_type{logical_type::INTEGER})
                      .has_value());
    REQUIRE(arity_of(operator_code::is_null) == operator_arity::unary);
}

TEST_CASE("operators::resolve_operator: invalid never resolves") {
    complex_logical_type integer{logical_type::INTEGER};

    REQUIRE_FALSE(resolve_operator(operator_code::invalid, integer).has_value());
    // Must not fall through to the "same numeric type" rule.
    REQUIRE_FALSE(resolve_operator(operator_code::invalid, integer, integer).has_value());
}