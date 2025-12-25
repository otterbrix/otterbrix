#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

namespace core {

    template<typename, typename = void>
    constexpr bool is_buffer_like = false;
    template<typename T>
    constexpr bool
        is_buffer_like<T, std::void_t<decltype(std::declval<T>().data()), decltype(std::declval<T>().size())>> = true;

    template<typename T>
    bool constexpr is_equals(T x, T y) {
        if constexpr (std::is_floating_point_v<T>) {
            return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
        } else {
            return x == y;
        }
    }

    template<typename T, typename U, std::enable_if_t<std::is_same_v<T, U>, bool> = true>
    bool constexpr is_equals(T x, U y) {
        return is_equals<T>(x, y);
    }

    template<typename T,
             typename U,
             std::enable_if_t<!std::is_same_v<T, U> && std::is_floating_point_v<T> && std::is_floating_point_v<U>,
                              bool> = true>
    bool constexpr is_equals(T x, U y) {
        if constexpr (sizeof(T) >= sizeof(U)) {
            return is_equals<T>(x, static_cast<T>(y));
        } else {
            return is_equals<U>(static_cast<U>(x), y);
        }
    }

    template<typename T,
             typename U,
             std::enable_if_t<!std::is_same_v<T, U> && std::is_floating_point_v<T> && !std::is_floating_point_v<U> &&
                                  std::is_integral_v<U>,
                              bool> = true>
    bool constexpr is_equals(T x, U y) {
        return is_equals<T>(x, static_cast<T>(y));
    }

    template<typename T,
             typename U,
             std::enable_if_t<!std::is_same_v<T, U> && !std::is_floating_point_v<T> && std::is_integral_v<T> &&
                                  std::is_floating_point_v<U>,
                              bool> = true>
    bool constexpr is_equals(T x, U y) {
        return is_equals<U>(static_cast<U>(x), y);
    }

    template<typename T,
             typename U,
             std::enable_if_t<!std::is_same_v<T, U> && std::is_integral_v<T> && std::is_integral_v<U>, bool> = true>
    bool constexpr is_equals(T x, U y) {
        if constexpr (sizeof(T) < sizeof(U)) {
            if constexpr (std::is_signed_v<U>) {
                return is_equals<U>(static_cast<std::make_signed_t<U>>(x), y);
            } else {
                return is_equals<U>(static_cast<std::make_unsigned_t<U>>(x), y);
            }
        } else if constexpr (sizeof(T) > sizeof(U)) {
            if constexpr (std::is_signed_v<T>) {
                return is_equals<T>(x, static_cast<std::make_signed_t<T>>(y));
            } else {
                return is_equals<T>(x, static_cast<std::make_unsigned_t<T>>(y));
            }
        } else {
            if constexpr (std::is_signed_v<T>) {
                return is_equals<T>(x, static_cast<T>(y));
            } else {
                return is_equals<U>(static_cast<U>(x), y);
            }
        }
    }

    template<typename T,
             typename U,
             std::enable_if_t<!std::is_same_v<T, U> && is_buffer_like<T> && is_buffer_like<U>, bool> = true>
    bool constexpr is_equals(T x, U y) {
        return is_equals<std::string_view>(std::string_view(x.data(), x.size()), std::string_view(y.data(), y.size()));
    }

    template<typename, typename, typename = void>
    struct has_equality_operator : std::false_type {};

    template<typename T, typename U>
    struct has_equality_operator<T,
                                 U,
                                 std::void_t<decltype(core::is_equals<T, U>(std::declval<T>(), std::declval<U>()))>>
        : std::true_type {};

    template<typename T>
    struct has_equality_operator<T, T, std::void_t<decltype(core::is_equals<T>(std::declval<T>(), std::declval<T>()))>>
        : std::true_type {};

    template<typename T, typename U>
    constexpr bool has_equality_operator_v = std::is_same_v<typename has_equality_operator<T, U>::type, std::true_type>;

    // TODO: use std::bit_cast after switch to c++20
    template<class To, class From>
    std::enable_if_t<std::is_trivially_copyable_v<From> && std::is_trivially_copyable_v<To>, To>
    bit_cast(const From& src) noexcept {
        To dst;
        std::memcpy(&dst, &src, sizeof(To));
        return dst;
    }

    // some compile time tests
    static_assert(has_equality_operator_v<int, int>);
    static_assert(has_equality_operator_v<float, float>);
    static_assert(has_equality_operator_v<std::string_view, std::string_view>);

    static_assert(has_equality_operator_v<int32_t, int64_t>);
    static_assert(has_equality_operator_v<uint32_t, int64_t>);
    static_assert(has_equality_operator_v<int32_t, uint64_t>);

    static_assert(has_equality_operator_v<float, double>);
    static_assert(has_equality_operator_v<float, int>);
    static_assert(has_equality_operator_v<int, float>);
    static_assert(has_equality_operator_v<float, bool>);

    static_assert(has_equality_operator_v<std::string_view, std::string>);

    static_assert(!has_equality_operator_v<float, std::string_view>);
    static_assert(!has_equality_operator_v<float, int*>);
    static_assert(!has_equality_operator_v<float*, int*>);

} // namespace core