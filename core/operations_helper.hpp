#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

namespace core {

    template<typename T>
    concept IsBufferLike = requires(T t) {
        { t.data() }
        ->std::convertible_to<const char*>;
        { t.size() }
        ->std::convertible_to<size_t>;
    };

    template<typename T>
    bool constexpr is_equals(T x, T y) {
        if constexpr (std::is_floating_point_v<T>) {
            return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
        } else {
            return x == y;
        }
    }

    template<typename T, typename U>
    bool constexpr is_equals(T x, U y) requires std::is_same_v<T, U> {
        return is_equals<T>(x, y);
    }

    template<typename T, typename U>
    bool constexpr is_equals(T x, U y) requires(!std::is_same_v<T, U> && std::is_floating_point_v<T> &&
                                                std::is_floating_point_v<U>) {
        if constexpr (sizeof(T) >= sizeof(U)) {
            return is_equals<T>(x, static_cast<T>(y));
        } else {
            return is_equals<U>(static_cast<U>(x), y);
        }
    }

    template<typename T, typename U>
    bool constexpr is_equals(T x, U y) requires(!std::is_same_v<T, U> && std::is_floating_point_v<T> &&
                                                !std::is_floating_point_v<U> && std::is_integral_v<U>) {
        return is_equals<T>(x, static_cast<T>(y));
    }

    template<typename T, typename U>
    bool constexpr is_equals(T x, U y) requires(!std::is_same_v<T, U> && !std::is_floating_point_v<T> &&
                                                std::is_integral_v<T> && std::is_floating_point_v<U>) {
        return is_equals<U>(static_cast<U>(x), y);
    }

    template<typename T, typename U>
    bool constexpr is_equals(T x,
                             U y) requires(!std::is_same_v<T, U> && std::is_integral_v<T> && std::is_integral_v<U>) {
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

    template<IsBufferLike T, IsBufferLike U>
    bool constexpr is_equals(T x, U y) {
        return is_equals<std::string_view>(std::string_view(x.data(), x.size()), std::string_view(y.data(), y.size()));
    }

    template<typename T, typename U>
    concept CanCompare = requires(T t, U u) {
        is_equals(t, u);
    };

    template<class To, class From>
    requires(std::is_trivially_copyable_v<From>, std::is_trivially_copyable_v<To>) To
        bit_cast(const From& src) noexcept {
        To dst;
        std::memcpy(&dst, &src, sizeof(To));
        return dst;
    }

    // some compile time tests
    static_assert(CanCompare<int, int>);
    static_assert(CanCompare<float, float>);
    static_assert(CanCompare<std::string_view, std::string_view>);

    static_assert(CanCompare<int32_t, int64_t>);
    static_assert(CanCompare<uint32_t, int64_t>);
    static_assert(CanCompare<int32_t, uint64_t>);

    static_assert(CanCompare<float, double>);
    static_assert(CanCompare<float, int>);
    static_assert(CanCompare<int, float>);
    static_assert(CanCompare<float, bool>);

    static_assert(CanCompare<std::string_view, std::string>);

    static_assert(!CanCompare<float, std::string_view>);
    static_assert(!CanCompare<float, int*>);
    static_assert(!CanCompare<float*, int*>);

} // namespace core