#include <catch2/catch_test_macros.hpp>

#include <components/vector/operations/apply_operator.hpp>

using namespace components;
using components::operators::operator_code;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::vector_t;

namespace {

    constexpr uint64_t capacity = 8;

    std::pmr::memory_resource* resource() { return std::pmr::get_default_resource(); }

    template<typename T>
    vector_t column(logical_type type, std::initializer_list<std::optional<T>> values) {
        vector_t vec(resource(), complex_logical_type(type), capacity);
        uint64_t row = 0;
        for (const auto& value : values) {
            if (value.has_value()) {
                vec.set_null(row, false);
                vec.set_value(row, T{*value});
            } else {
                vec.set_null(row, true);
            }
            row++;
        }
        return vec;
    }

    vector_t slot(logical_type type) { return vector_t(resource(), complex_logical_type(type), capacity); }

    graph_execution_context context() { return graph_execution_context{}; }

} // namespace

TEST_CASE("vector::operations::arithmetic writes into the caller's output") {
    auto left = column<int64_t>(logical_type::BIGINT, {1, 2, 3});
    auto right = column<int64_t>(logical_type::BIGINT, {10, 20, 30});
    auto output = slot(logical_type::BIGINT);

    REQUIRE_FALSE(
        vector::operations::apply_binary(operator_code::add, left, right, &output, context(), 3).contains_error());

    REQUIRE(output.get_value<int64_t>(0) == 11);
    REQUIRE(output.get_value<int64_t>(1) == 22);
    REQUIRE(output.get_value<int64_t>(2) == 33);
}

TEST_CASE("vector::operations::a null operand makes the arithmetic result null") {
    auto left = column<int64_t>(logical_type::BIGINT, {1, std::nullopt, 3});
    auto right = column<int64_t>(logical_type::BIGINT, {10, 20, std::nullopt});
    auto output = slot(logical_type::BIGINT);

    REQUIRE_FALSE(
        vector::operations::apply_binary(operator_code::multiply, left, right, &output, context(), 3).contains_error());

    REQUIRE_FALSE(output.is_null(0));
    REQUIRE(output.get_value<int64_t>(0) == 10);
    REQUIRE(output.is_null(1));
    REQUIRE(output.is_null(2));
}

TEST_CASE("vector::operations::comparing against null is unknown, not false") {
    auto left = column<int64_t>(logical_type::BIGINT, {1, 5, std::nullopt});
    auto right = column<int64_t>(logical_type::BIGINT, {5, 5, 5});
    auto output = slot(logical_type::BOOLEAN);

    REQUIRE_FALSE(
        vector::operations::apply_binary(operator_code::less, left, right, &output, context(), 3).contains_error());

    REQUIRE(output.get_value<bool>(0));
    REQUIRE_FALSE(output.get_value<bool>(1));
    // The row that must NOT come back as false.
    REQUIRE(output.is_null(2));
}

TEST_CASE("vector::operations::AND is false against unknown, OR is true against unknown") {
    // rows: (false, unknown) (true, unknown) (unknown, unknown)
    auto left = column<bool>(logical_type::BOOLEAN, {false, true, std::nullopt});
    auto right = column<bool>(logical_type::BOOLEAN, {std::nullopt, std::nullopt, std::nullopt});

    auto conjunction = slot(logical_type::BOOLEAN);
    REQUIRE_FALSE(vector::operations::apply_binary(operator_code::logical_and, left, right, &conjunction, context(), 3)
                      .contains_error());
    // FALSE AND UNKNOWN is FALSE -- the case a plain null-propagation gets wrong.
    REQUIRE_FALSE(conjunction.is_null(0));
    REQUIRE_FALSE(conjunction.get_value<bool>(0));
    REQUIRE(conjunction.is_null(1));
    REQUIRE(conjunction.is_null(2));

    auto disjunction = slot(logical_type::BOOLEAN);
    REQUIRE_FALSE(vector::operations::apply_binary(operator_code::logical_or, left, right, &disjunction, context(), 3)
                      .contains_error());
    REQUIRE(disjunction.is_null(0));
    // TRUE OR UNKNOWN is TRUE.
    REQUIRE_FALSE(disjunction.is_null(1));
    REQUIRE(disjunction.get_value<bool>(1));
    REQUIRE(disjunction.is_null(2));
}

TEST_CASE("vector::operations::NOT unknown stays unknown but IS NULL never does") {
    auto operand = column<bool>(logical_type::BOOLEAN, {true, false, std::nullopt});

    auto negated = slot(logical_type::BOOLEAN);
    REQUIRE_FALSE(
        vector::operations::apply_unary(operator_code::logical_not, operand, &negated, context(), 3).contains_error());
    REQUIRE_FALSE(negated.get_value<bool>(0));
    REQUIRE(negated.get_value<bool>(1));
    REQUIRE(negated.is_null(2));

    auto tested = slot(logical_type::BOOLEAN);
    REQUIRE_FALSE(
        vector::operations::apply_unary(operator_code::is_null, operand, &tested, context(), 3).contains_error());
    REQUIRE_FALSE(tested.is_null(2));
    REQUIRE(tested.get_value<bool>(2));
    REQUIRE_FALSE(tested.get_value<bool>(0));
}

TEST_CASE("vector::operations::a reused slot does not leak the previous chunk's nulls") {
    auto output = slot(logical_type::BIGINT);
    auto first_left = column<int64_t>(logical_type::BIGINT, {std::nullopt, std::nullopt});
    auto first_right = column<int64_t>(logical_type::BIGINT, {1, 1});
    REQUIRE_FALSE(vector::operations::apply_binary(operator_code::add, first_left, first_right, &output, context(), 2)
                      .contains_error());
    REQUIRE(output.is_null(0));

    // Same slot, a chunk with no nulls at all.
    auto second_left = column<int64_t>(logical_type::BIGINT, {7, 8});
    auto second_right = column<int64_t>(logical_type::BIGINT, {1, 1});
    REQUIRE_FALSE(vector::operations::apply_binary(operator_code::add, second_left, second_right, &output, context(), 2)
                      .contains_error());
    REQUIRE_FALSE(output.is_null(0));
    REQUIRE(output.get_value<int64_t>(0) == 8);
}

TEST_CASE("vector::operations::integer division by zero is reported, not trapped") {
    auto left = column<int64_t>(logical_type::BIGINT, {1});
    auto right = column<int64_t>(logical_type::BIGINT, {0});
    auto output = slot(logical_type::BIGINT);

    REQUIRE(
        vector::operations::apply_binary(operator_code::divide, left, right, &output, context(), 1).contains_error());
}

// A container compares by its ELEMENTS: lexicographically over the shared prefix, then by length.
// It cannot go through the per-machine-word physical dispatch, because one row of it is a RUN of
// words rather than one.
TEST_CASE("vector::operations::comparison walks ARRAY elements, length-aware") {
    constexpr size_t length = 3;
    const auto array_type = complex_logical_type::create_array(logical_type::INTEGER, length);

    auto arrays = [&](std::initializer_list<std::vector<int32_t>> rows) {
        vector_t vec(resource(), array_type, capacity);
        uint64_t row = 0;
        for (const auto& value : rows) {
            vec.set_value(row++, value);
        }
        return vec;
    };

    SECTION("equal only when every element matches") {
        auto left = arrays({{7, 8, 9}, {1, 2, 3}, {1, 2, 3}});
        auto right = arrays({{7, 8, 9}, {1, 2, 4}, {1, 2, 3}});
        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::equal, left, right, &out, context(), 3).contains_error());
        REQUIRE(out.get_value<bool>(0));
        REQUIRE_FALSE(out.get_value<bool>(1)); // differs at the last element
        REQUIRE(out.get_value<bool>(2));
    }

    SECTION("ordering is lexicographic, decided by the first differing element") {
        auto left = arrays({{1, 2, 3}, {1, 9, 0}});
        auto right = arrays({{1, 2, 4}, {1, 2, 9}});
        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::less, left, right, &out, context(), 2).contains_error());
        REQUIRE(out.get_value<bool>(0));       // 3 < 4
        REQUIRE_FALSE(out.get_value<bool>(1)); // 9 > 2 decides, later elements do not matter
    }

    SECTION("a NULL row makes the comparison UNKNOWN, not false") {
        auto left = arrays({{1, 2, 3}, {1, 2, 3}});
        auto right = arrays({{1, 2, 3}, {1, 2, 3}});
        left.set_null(1, true);
        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::equal, left, right, &out, context(), 2).contains_error());
        REQUIRE(out.get_value<bool>(0));
        REQUIRE(out.is_null(1));
    }

    // A NULL ELEMENT is not a NULL row. Comparing whole containers is a TOTAL order — a NULL
    // element sorts after every value and two NULLs are equivalent — so the row still answers
    // true or false. PostgreSQL agrees: `ARRAY[1,NULL] = ARRAY[1,NULL]` is TRUE and
    // `ARRAY[1,NULL] < ARRAY[1,2]` is FALSE, neither of them NULL. Only a NULL ROW is UNKNOWN.
    SECTION("a NULL ELEMENT does not make the row UNKNOWN: it orders after every value") {
        auto left = arrays({{1, 2, 3}, {1, 2, 3}});
        auto right = arrays({{1, 2, 3}, {1, 2, 3}});
        left.set_null({0, 1}, true); // row 0: NULL where the right side holds 2 -> unequal
        left.set_null({1, 1}, true); // row 1: both sides NULL in the same place -> equal
        right.set_null({1, 1}, true);

        auto equality = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(vector::operations::apply_binary(operator_code::equal, left, right, &equality, context(), 2)
                          .contains_error());
        REQUIRE_FALSE(equality.is_null(0));
        REQUIRE_FALSE(equality.get_value<bool>(0));
        REQUIRE_FALSE(equality.is_null(1));
        REQUIRE(equality.get_value<bool>(1));

        auto ordering = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(vector::operations::apply_binary(operator_code::less, left, right, &ordering, context(), 2)
                          .contains_error());
        REQUIRE_FALSE(ordering.is_null(0));
        REQUIRE_FALSE(ordering.get_value<bool>(0)); // {1,NULL,3} sorts AFTER {1,2,3}
        REQUIRE_FALSE(ordering.get_value<bool>(1)); // two equal rows: neither is less
    }

    SECTION("a CONSTANT operand is read at its single row, not at the loop index") {
        // The literal side of `col = ARRAY[...]` is placed ONCE. An ARRAY addresses its elements
        // as row * stride, so reading it at the loop index would walk clean off the one stored
        // value — this is the shape that regressed before.
        auto left = arrays({{7, 8, 9}, {1, 2, 3}, {7, 8, 9}});
        vector_t literal(resource(), array_type, capacity);
        literal.set_value(0, std::vector<int32_t>{7, 8, 9});
        literal.set_vector_type(vector::vector_type::CONSTANT);

        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::equal, left, literal, &out, context(), 3).contains_error());
        REQUIRE(out.get_value<bool>(0));
        REQUIRE_FALSE(out.get_value<bool>(1));
        REQUIRE(out.get_value<bool>(2));
    }
}

// A LIST row is a (offset, length) run, so two of them can differ in LENGTH — which an ARRAY of
// fixed width never can. Different lengths are simply unequal: never truncated to a common prefix,
// never padded.
TEST_CASE("vector::operations::comparison is length-aware for LIST") {
    const auto list_type = complex_logical_type::create_list(logical_type::INTEGER);

    auto lists = [&](std::initializer_list<std::vector<int32_t>> rows) {
        vector_t vec(resource(), list_type, capacity);
        uint64_t row = 0;
        for (const auto& value : rows) {
            vec.set_value(row++, value);
        }
        return vec;
    };

    auto left = lists({{1, 2, 3}, {1, 2}, {1, 2, 3}});
    auto right = lists({{1, 2}, {1, 2, 3}, {1, 2, 3}});

    SECTION("a shared prefix does not make different lengths equal") {
        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::equal, left, right, &out, context(), 3).contains_error());
        REQUIRE_FALSE(out.get_value<bool>(0));
        REQUIRE_FALSE(out.get_value<bool>(1));
        REQUIRE(out.get_value<bool>(2));
    }

    SECTION("length breaks the tie for ordering: the shorter one is less") {
        auto out = slot(logical_type::BOOLEAN);
        REQUIRE_FALSE(
            vector::operations::apply_binary(operator_code::less, left, right, &out, context(), 3).contains_error());
        REQUIRE_FALSE(out.get_value<bool>(0)); // [1,2,3] > [1,2]
        REQUIRE(out.get_value<bool>(1));       // [1,2]   < [1,2,3]
        REQUIRE_FALSE(out.get_value<bool>(2)); // equal
    }
}
