#include <catch2/catch_test_macros.hpp>

#include <components/vector/arithmetic.hpp>
#include <components/vector/vector.hpp>

#include <core/operations_helper.hpp>

#include <string>

using namespace components;
using types::logical_type;

namespace {

    // Every numeric logical_type except BOOLEAN. BOOLEAN is is_numeric() but
    // promote_type() underflows on it (BOOLEAN's enum value is below the
    // signed/unsigned offset), so a BOOLEAN mixed with any signed type resolves to a
    // garbage logical_type. That is a components/types defect, out of scope here.
    constexpr logical_type numeric_types[] = {logical_type::TINYINT,
                                              logical_type::SMALLINT,
                                              logical_type::INTEGER,
                                              logical_type::BIGINT,
                                              logical_type::HUGEINT,
                                              logical_type::UTINYINT,
                                              logical_type::USMALLINT,
                                              logical_type::UINTEGER,
                                              logical_type::UBIGINT,
                                              logical_type::UHUGEINT,
                                              logical_type::FLOAT,
                                              logical_type::DOUBLE};

    constexpr vector::arithmetic_op arithmetic_ops[] = {vector::arithmetic_op::add,
                                                        vector::arithmetic_op::subtract,
                                                        vector::arithmetic_op::multiply,
                                                        vector::arithmetic_op::divide,
                                                        vector::arithmetic_op::mod};

    std::string name_of(logical_type type) {
        switch (type) {
            case logical_type::TINYINT:
                return "TINYINT";
            case logical_type::SMALLINT:
                return "SMALLINT";
            case logical_type::INTEGER:
                return "INTEGER";
            case logical_type::BIGINT:
                return "BIGINT";
            case logical_type::HUGEINT:
                return "HUGEINT";
            case logical_type::UTINYINT:
                return "UTINYINT";
            case logical_type::USMALLINT:
                return "USMALLINT";
            case logical_type::UINTEGER:
                return "UINTEGER";
            case logical_type::UBIGINT:
                return "UBIGINT";
            case logical_type::UHUGEINT:
                return "UHUGEINT";
            case logical_type::FLOAT:
                return "FLOAT";
            case logical_type::DOUBLE:
                return "DOUBLE";
            default:
                return "logical_type(" + std::to_string(static_cast<int>(type)) + ")";
        }
    }

    std::string name_of(vector::arithmetic_op op) {
        switch (op) {
            case vector::arithmetic_op::add:
                return "add";
            case vector::arithmetic_op::subtract:
                return "subtract";
            case vector::arithmetic_op::multiply:
                return "multiply";
            case vector::arithmetic_op::divide:
                return "divide";
            case vector::arithmetic_op::mod:
                return "mod";
        }
        return "?";
    }

    types::logical_value_t make_value(std::pmr::memory_resource* resource, logical_type type, int v) {
        switch (type) {
            case logical_type::TINYINT:
                return {resource, static_cast<int8_t>(v)};
            case logical_type::SMALLINT:
                return {resource, static_cast<int16_t>(v)};
            case logical_type::INTEGER:
                return {resource, static_cast<int32_t>(v)};
            case logical_type::BIGINT:
                return {resource, static_cast<int64_t>(v)};
            case logical_type::HUGEINT:
                return {resource, types::int128_t(v)};
            case logical_type::UTINYINT:
                return {resource, static_cast<uint8_t>(v)};
            case logical_type::USMALLINT:
                return {resource, static_cast<uint16_t>(v)};
            case logical_type::UINTEGER:
                return {resource, static_cast<uint32_t>(v)};
            case logical_type::UBIGINT:
                return {resource, static_cast<uint64_t>(v)};
            case logical_type::UHUGEINT:
                return {resource, types::uint128_t(static_cast<uint64_t>(v))};
            case logical_type::FLOAT:
                return {resource, static_cast<float>(v)};
            case logical_type::DOUBLE:
                return {resource, static_cast<double>(v)};
            default:
                return {resource, types::complex_logical_type(logical_type::NA)};
        }
    }

    vector::vector_t make_flat(std::pmr::memory_resource* resource, const types::logical_value_t& value) {
        vector::vector_t vec(resource, value, 1);
        vec.flatten(1);
        return vec;
    }

    core::result_wrapper_t<types::logical_value_t> scalar_arithmetic(std::pmr::memory_resource* resource,
                                                                     vector::arithmetic_op op,
                                                                     const types::logical_value_t& l,
                                                                     const types::logical_value_t& r) {
        return types::logical_value_t::arithmetic(resource, op, l, r);
    }

} // namespace

// The result type of `a <op> b` has three independent producers, and they have to agree:
//   * types::arithmetic_result_type      — what the planner types the projection column as
//     (services/dispatcher/validate_logical_plan.cpp)
//   * compute_binary_arithmetic          — what the execution kernel actually builds
//   * logical_value_t::arithmetic        — the scalar path (post-aggregate arithmetic,
//     constant expressions in the SQL transformer)
// They diverged for FLOAT: the kernel demoted FLOAT to DOUBLE right after asking
// arithmetic_result_type, so `float_col * 2` was planned FLOAT and produced DOUBLE.
TEST_CASE("arithmetic result type: plan, kernel and scalar agree over numeric x numeric") {
    auto resource = core::pmr::otterbrix_resource();

    for (auto op : arithmetic_ops) {
        for (auto lhs : numeric_types) {
            for (auto rhs : numeric_types) {
                INFO(name_of(lhs) << " " << name_of(op) << " " << name_of(rhs));

                const auto planned = types::arithmetic_result_type(lhs, rhs, op);
                REQUIRE(planned != logical_type::NA);

                // 6 and 3 keep divide/mod defined and exactly representable everywhere.
                auto left_value = make_value(&resource, lhs, 6);
                auto right_value = make_value(&resource, rhs, 3);

                auto left_vec = make_flat(&resource, left_value);
                auto right_vec = make_flat(&resource, right_value);
                auto kernel = vector::compute_binary_arithmetic(&resource, op, left_vec, right_vec, 1);
                REQUIRE_FALSE(kernel.has_error());
                REQUIRE(kernel.value().type().type() == planned);

                // Two known gaps in the scalar producer, both in components/types and both
                // outside this component's scope -- skipped rather than pinned here:
                //  * logical_value_t::arithmetic has no floating-point arm for `%` at all
                //    (std::modulus<> is `%`), so it answers an error there.
                //  * its apply<> helper returns whatever C++ promotion produced, so a result
                //    narrower than int (TINYINT/SMALLINT and their unsigned twins) comes back
                //    as INTEGER instead of the promoted operand type.
                const bool scalar_mod_gap = op == vector::arithmetic_op::mod &&
                                            (planned == logical_type::FLOAT || planned == logical_type::DOUBLE);
                const bool scalar_sub_int_gap =
                    planned == logical_type::TINYINT || planned == logical_type::SMALLINT ||
                    planned == logical_type::UTINYINT || planned == logical_type::USMALLINT;
                if (!scalar_mod_gap && !scalar_sub_int_gap) {
                    auto scalar = scalar_arithmetic(&resource, op, left_value, right_value);
                    REQUIRE_FALSE(scalar.has_error());
                    REQUIRE(scalar.value().type().type() == planned);
                }
            }
        }
    }
}

// A FLOAT column stays FLOAT through the kernel, and the values are the ones float
// arithmetic produces (not a double result truncated into a 4-byte slot).
TEST_CASE("arithmetic on FLOAT operands produces FLOAT") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr uint64_t count = 8;

    vector::vector_t left(&resource, types::complex_logical_type(logical_type::FLOAT), count);
    vector::vector_t right(&resource, types::complex_logical_type(logical_type::FLOAT), count);
    for (uint64_t i = 0; i < count; i++) {
        left.data<float>()[i] = static_cast<float>(i + 1);
        right.data<float>()[i] = 2.0f;
    }

    auto result = vector::compute_binary_arithmetic(&resource, vector::arithmetic_op::multiply, left, right, count);
    REQUIRE_FALSE(result.has_error());
    const auto& out = result.value();
    REQUIRE(out.type().type() == logical_type::FLOAT);
    REQUIRE(out.type().size() == sizeof(float));
    for (uint64_t i = 0; i < count; i++) {
        REQUIRE(core::is_equals(out.get_value<float>(i), static_cast<float>(i + 1) * 2.0f));
    }

    // FLOAT mixed with an integer stays FLOAT too (PostgreSQL / DuckDB float4 semantics).
    vector::vector_t ints(&resource, types::complex_logical_type(logical_type::INTEGER), count);
    for (uint64_t i = 0; i < count; i++) {
        ints.data<int32_t>()[i] = 3;
    }
    auto mixed = vector::compute_binary_arithmetic(&resource, vector::arithmetic_op::add, left, ints, count);
    REQUIRE_FALSE(mixed.has_error());
    REQUIRE(mixed.value().type().type() == logical_type::FLOAT);
    for (uint64_t i = 0; i < count; i++) {
        REQUIRE(core::is_equals(mixed.value().get_value<float>(i), static_cast<float>(i + 1) + 3.0f));
    }
}

// The kernel stores results through the OUTPUT vector's physical width. It used to store
// through the C++ promotion type of op(L,R): int8+int8 promotes to `int`, so a TINYINT
// output (1 byte per row) was written 4 bytes per row — a heap overflow that also made
// every other result read back as 0.
TEST_CASE("arithmetic writes results at the output type's width") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr uint64_t count = 8;

    SECTION("TINYINT") {
        vector::vector_t left(&resource, types::complex_logical_type(logical_type::TINYINT), count);
        vector::vector_t right(&resource, types::complex_logical_type(logical_type::TINYINT), count);
        for (uint64_t i = 0; i < count; i++) {
            left.data<int8_t>()[i] = static_cast<int8_t>(i + 1);
            right.data<int8_t>()[i] = 10;
        }
        auto out = vector::compute_binary_arithmetic(&resource, vector::arithmetic_op::add, left, right, count);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::TINYINT);
        for (uint64_t i = 0; i < count; i++) {
            REQUIRE(out.value().get_value<int8_t>(i) == static_cast<int8_t>(i + 11));
        }
    }

    SECTION("SMALLINT") {
        vector::vector_t left(&resource, types::complex_logical_type(logical_type::SMALLINT), count);
        vector::vector_t right(&resource, types::complex_logical_type(logical_type::SMALLINT), count);
        for (uint64_t i = 0; i < count; i++) {
            left.data<int16_t>()[i] = static_cast<int16_t>(i + 1);
            right.data<int16_t>()[i] = 100;
        }
        auto out = vector::compute_binary_arithmetic(&resource, vector::arithmetic_op::add, left, right, count);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::SMALLINT);
        for (uint64_t i = 0; i < count; i++) {
            REQUIRE(out.value().get_value<int16_t>(i) == static_cast<int16_t>(i + 101));
        }
    }
}

// A 0-row chunk is real input, not a drain sentinel, and the column it produces is still
// typed. The kernels used to hard-code DOUBLE for count == 0 regardless of the operands,
// and nothing downstream repaired it: the arithmetic arm of operator_select is the one
// column kind that does not fall back to the plan-resolved col.result_type.
TEST_CASE("zero-row arithmetic keeps the operand-derived result type") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("vector op vector") {
        vector::vector_t left(&resource, types::complex_logical_type(logical_type::INTEGER), 0);
        vector::vector_t right(&resource, types::complex_logical_type(logical_type::BIGINT), 0);
        auto out = vector::compute_binary_arithmetic(&resource, vector::arithmetic_op::add, left, right, 0);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::BIGINT);
    }

    SECTION("vector op scalar") {
        vector::vector_t vec(&resource, types::complex_logical_type(logical_type::FLOAT), 0);
        auto scalar = types::logical_value_t(&resource, static_cast<int32_t>(2));
        auto out = vector::compute_vector_scalar_arithmetic(&resource, vector::arithmetic_op::multiply, vec, scalar, 0);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::FLOAT);
    }

    SECTION("scalar op vector") {
        auto scalar = types::logical_value_t(&resource, static_cast<int64_t>(2));
        vector::vector_t vec(&resource, types::complex_logical_type(logical_type::INTEGER), 0);
        auto out = vector::compute_scalar_vector_arithmetic(&resource, vector::arithmetic_op::subtract, scalar, vec, 0);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::BIGINT);
    }

    SECTION("unary minus") {
        vector::vector_t vec(&resource, types::complex_logical_type(logical_type::SMALLINT), 0);
        auto out = vector::compute_unary_neg(&resource, vec, 0);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value().type().type() == logical_type::SMALLINT);
    }
}
