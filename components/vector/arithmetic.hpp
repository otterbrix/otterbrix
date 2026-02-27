#pragma once

#include "data_chunk.hpp"
#include <components/types/operations_helper.hpp>
#include <cmath>

namespace components::vector {

    namespace detail {
        template<typename R>
        constexpr bool is_zero(R b) {
            if constexpr (std::is_floating_point_v<R>) {
                return !(b > R{0} || b < R{0});
            } else {
                return b == R{0};
            }
        }

        // Common result type that avoids sign-promotion issues with int128
        template<typename L, typename R>
        using safe_result_t = decltype(static_cast<L>(0) + static_cast<R>(0));
    } // namespace detail

    // Safe divides that returns 0 on division by zero
    template<typename T = void>
    struct safe_divides;
    template<>
    struct safe_divides<void> {
        template<typename L, typename R>
        constexpr auto operator()(L a, R b) const {
            using result_t = detail::safe_result_t<L, R>;
            if (detail::is_zero(b)) {
                return static_cast<result_t>(0);
            }
            return static_cast<result_t>(static_cast<result_t>(a) / static_cast<result_t>(b));
        }
    };

    // Safe modulus that handles floating-point via fmod and int128
    template<typename T = void>
    struct safe_modulus;
    template<>
    struct safe_modulus<void> {
        template<typename L, typename R>
        constexpr auto operator()(L a, R b) const {
            using result_t = detail::safe_result_t<L, R>;
            if (detail::is_zero(b)) {
                return static_cast<result_t>(0);
            }
            if constexpr (std::is_floating_point_v<result_t>) {
                return static_cast<result_t>(std::fmod(static_cast<result_t>(a), static_cast<result_t>(b)));
            } else {
                return static_cast<result_t>(static_cast<result_t>(a) % static_cast<result_t>(b));
            }
        }
    };

    // op_kind: 0=add, 1=sub, 2=mul, 3=div, 4=mod
    enum class arithmetic_op : uint8_t { add = 0, subtract, multiply, divide, mod };

    // Compute binary arithmetic on two vectors (element-wise)
    vector_t compute_binary_arithmetic(std::pmr::memory_resource* resource,
                                       arithmetic_op op,
                                       const vector_t& left,
                                       const vector_t& right,
                                       uint64_t count);

    // Compute arithmetic: vector op scalar
    vector_t compute_vector_scalar_arithmetic(std::pmr::memory_resource* resource,
                                              arithmetic_op op,
                                              const vector_t& vec,
                                              const types::logical_value_t& scalar,
                                              uint64_t count);

    // Compute arithmetic: scalar op vector
    vector_t compute_scalar_vector_arithmetic(std::pmr::memory_resource* resource,
                                              arithmetic_op op,
                                              const types::logical_value_t& scalar,
                                              const vector_t& vec,
                                              uint64_t count);

    // Compute unary negation
    vector_t compute_unary_neg(std::pmr::memory_resource* resource,
                               const vector_t& vec,
                               uint64_t count);

} // namespace components::vector
