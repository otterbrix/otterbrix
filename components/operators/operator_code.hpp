#pragma once

#include <cstdint>

namespace components::operators {

    enum class operator_arity : uint8_t
    {
        unary,
        binary
    };

    enum class operator_code : uint8_t
    {
        invalid,
        // arithmetics
        add,
        subtract,
        multiply,
        divide,
        mod,
        negate,
        bit_and,
        bit_or,
        bit_xor,
        bit_not,
        shift_left,
        shift_right,
        // comparisons
        equal,
        not_equal,
        // Never yields UNKNOWN
        strict_equal,
        less,
        less_equal,
        greater,
        greater_equal,
        logical_and,
        logical_or,
        logical_not,
        is_null,
        is_not_null
    };

    [[nodiscard]] operator_arity arity_of(operator_code code) noexcept;

} // namespace components::operators