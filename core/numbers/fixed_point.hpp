#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <string>

#include <core/assert/assert.hpp>

#include "temporary.hpp"
#include <dataframe/types.hpp>

namespace core::numbers {

    enum scale_type : int32_t
    {
    };

    enum class Radix : int32_t
    {
        BASE_2 = 2,
        BASE_10 = 10
    };

    template<typename T>
    constexpr inline auto is_supported_representation_type() {
        return std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, absl::int128>;
    }

    template<typename T>
    constexpr inline auto is_supported_construction_value_type() {
        return std::is_integral<T>() || std::is_floating_point_v<T>;
    }

    namespace detail {

        template<typename Rep,
                 Radix Base,
                 typename T,
                 typename std::enable_if_t<(std::is_same_v<int32_t, T> && is_supported_representation_type<Rep>())>* =
                     nullptr>
        inline Rep ipow(T exponent) {
            assertion_exception_msg(exponent >= 0, "integer exponentiation with negative exponent is not possible.");
            if (exponent == 0) {
                return static_cast<Rep>(1);
            }

            auto extra = static_cast<Rep>(1);
            auto square = static_cast<Rep>(Base);
            while (exponent > 1) {
                if (exponent & 1 /* odd */) {
                    extra *= square;
                    exponent -= 1;
                }
                exponent /= 2;
                square *= square;
            }
            return square * extra;
        }

        template<typename Rep, Radix Rad, typename T>
        inline constexpr T right_shift(T const& val, scale_type const& scale) {
            return val / ipow<Rep, Rad>(static_cast<int32_t>(scale));
        }

        template<typename Rep, Radix Rad, typename T>
        inline constexpr T left_shift(T const& val, scale_type const& scale) {
            return val * ipow<Rep, Rad>(static_cast<int32_t>(-scale));
        }

        template<typename Rep, Radix Rad, typename T>
        inline constexpr T shift(T const& val, scale_type const& scale) {
            if (scale == 0) {
                return val;
            }
            if (scale > 0) {
                return right_shift<Rep, Rad>(val, scale);
            }
            return left_shift<Rep, Rad>(val, scale);
        }

    } // namespace detail

    template<typename Rep, typename std::enable_if_t<is_supported_representation_type<Rep>()>* = nullptr>
    struct scaled_integer {
        Rep value;
        scale_type scale;
        inline explicit scaled_integer(Rep v, scale_type s)
            : value{v}
            , scale{s} {}
    };

    template<typename Rep, Radix Rad>
    class fixed_point {
        Rep _value{};
        scale_type _scale;

    public:
        using rep = Rep;

        template<typename T,
                 typename std::enable_if_t<std::is_floating_point<T>() && is_supported_representation_type<Rep>()>* =
                     nullptr>
        inline explicit fixed_point(T const& value, scale_type const& scale)
            : _value{static_cast<Rep>(detail::shift<Rep, Rad>(value, scale))}
            , _scale{scale} {}

        template<typename T,
                 typename std::enable_if_t<std::is_integral<T>() && is_supported_representation_type<Rep>()>* = nullptr>
        inline explicit fixed_point(T const& value, scale_type const& scale)
            : _value{detail::shift<Rep, Rad>(static_cast<Rep>(value), scale)}
            , _scale{scale} {}

        inline explicit fixed_point(scaled_integer<Rep> s)
            : _value{s.value}
            , _scale{s.scale} {}

        template<typename T, typename std::enable_if_t<is_supported_construction_value_type<T>()>* = nullptr>
        inline fixed_point(T const& value)
            : _value{static_cast<Rep>(value)}
            , _scale{scale_type{0}} {}

        inline fixed_point()
            : _scale{scale_type{0}} {}

        template<typename U, typename std::enable_if_t<std::is_floating_point_v<U>>* = nullptr>
        explicit constexpr operator U() const {
            return detail::shift<Rep, Rad>(static_cast<U>(_value), scale_type{-_scale});
        }

        template<typename U, typename std::enable_if_t<std::is_integral_v<U>>* = nullptr>
        explicit constexpr operator U() const {
            auto const value = std::common_type_t<U, Rep>(_value);
            return static_cast<U>(detail::shift<Rep, Rad>(value, scale_type{-_scale}));
        }

        inline operator scaled_integer<Rep>() const { return scaled_integer<Rep>{_value, _scale}; }

        inline rep value() const { return _value; }

        inline scale_type scale() const { return _scale; }
        inline explicit constexpr operator bool() const { return static_cast<bool>(_value); }

        template<typename Rep1, Radix Rad1>
        inline fixed_point<Rep1, Rad1>& operator+=(fixed_point<Rep1, Rad1> const& rhs) {
            *this = *this + rhs;
            return *this;
        }

        template<typename Rep1, Radix Rad1>
        inline fixed_point<Rep1, Rad1>& operator*=(fixed_point<Rep1, Rad1> const& rhs) {
            *this = *this * rhs;
            return *this;
        }

        template<typename Rep1, Radix Rad1>
        inline fixed_point<Rep1, Rad1>& operator-=(fixed_point<Rep1, Rad1> const& rhs) {
            *this = *this - rhs;
            return *this;
        }

        template<typename Rep1, Radix Rad1>
        inline fixed_point<Rep1, Rad1>& operator/=(fixed_point<Rep1, Rad1> const& rhs) {
            *this = *this / rhs;
            return *this;
        }

        inline fixed_point<Rep, Rad>& operator++() {
            *this = *this + fixed_point<Rep, Rad>{1, scale_type{_scale}};
            return *this;
        }

        template<typename Rep1, Radix Rad1>
        inline friend fixed_point<Rep1, Rad1> operator+(fixed_point<Rep1, Rad1> const& lhs,
                                                        fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend fixed_point<Rep1, Rad1> operator-(fixed_point<Rep1, Rad1> const& lhs,
                                                        fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend fixed_point<Rep1, Rad1> operator*(fixed_point<Rep1, Rad1> const& lhs,
                                                        fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend fixed_point<Rep1, Rad1> operator/(fixed_point<Rep1, Rad1> const& lhs,
                                                        fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend fixed_point<Rep1, Rad1> operator%(fixed_point<Rep1, Rad1> const& lhs,
                                                        fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator==(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator!=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator<=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator>=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator<(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        template<typename Rep1, Radix Rad1>
        inline friend bool operator>(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs);

        inline fixed_point<Rep, Rad> rescaled(scale_type scale) const {
            if (scale == _scale) {
                return *this;
            }
            Rep const value = detail::shift<Rep, Rad>(_value, scale_type{scale - _scale});
            return fixed_point<Rep, Rad>{scaled_integer<Rep>{value, scale}};
        }

        explicit operator std::string() const {
            if (_scale < 0) {
                auto const av = abs(_value);
                Rep const n = exp10<Rep>(-_scale);
                Rep const f = av % n;
                auto const num_zeros = std::max(0, (-_scale - static_cast<int32_t>(to_string(f).size())));
                auto const zeros = std::string(num_zeros, '0');
                auto const sign = _value < 0 ? std::string("-") : std::string();
                return sign + to_string(av / n) + std::string(".") + zeros + to_string(av % n);
            }
            auto const zeros = std::string(_scale, '0');
            return to_string(_value) + zeros;
        }
    };

    template<typename Rep, typename T>
    inline auto addition_overflow(T lhs, T rhs) {
        return rhs > 0 ? lhs > std::numeric_limits<Rep>::max() - rhs : lhs < std::numeric_limits<Rep>::min() - rhs;
    }

    template<typename Rep, typename T>
    inline auto subtraction_overflow(T lhs, T rhs) {
        return rhs > 0 ? lhs < std::numeric_limits<Rep>::min() + rhs : lhs > std::numeric_limits<Rep>::max() + rhs;
    }

    template<typename Rep, typename T>
    inline auto division_overflow(T lhs, T rhs) {
        return lhs == std::numeric_limits<Rep>::min() && rhs == -1;
    }

    template<typename Rep, typename T>
    inline auto multiplication_overflow(T lhs, T rhs) {
        auto const min = std::numeric_limits<Rep>::min();
        auto const max = std::numeric_limits<Rep>::max();
        if (rhs > 0) {
            return lhs > max / rhs || lhs < min / rhs;
        }
        if (rhs < -1) {
            return lhs > min / rhs || lhs < max / rhs;
        }
        return rhs == -1 && lhs == min;
    }

    template<typename Rep1, Radix Rad1>
    inline fixed_point<Rep1, Rad1> operator+(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        auto const sum = lhs.rescaled(scale)._value + rhs.rescaled(scale)._value;

        assertion_exception_msg(!addition_overflow<Rep1>(lhs.rescaled(scale)._value, rhs.rescaled(scale)._value),
                                "fixed_point overflow");
        return fixed_point<Rep1, Rad1>{scaled_integer<Rep1>{sum, scale}};
    }

    template<typename Rep1, Radix Rad1>
    inline fixed_point<Rep1, Rad1> operator-(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        auto const diff = lhs.rescaled(scale)._value - rhs.rescaled(scale)._value;

        assertion_exception_msg(!subtraction_overflow<Rep1>(lhs.rescaled(scale)._value, rhs.rescaled(scale)._value),
                                "fixed_point overflow");
        return fixed_point<Rep1, Rad1>{scaled_integer<Rep1>{diff, scale}};
    }

    template<typename Rep1, Radix Rad1>
    inline fixed_point<Rep1, Rad1> operator*(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        assertion_exception_msg(!multiplication_overflow<Rep1>(lhs._value, rhs._value), "fixed_point overflow");
        return fixed_point<Rep1, Rad1>{
            scaled_integer<Rep1>(lhs._value * rhs._value, scale_type{lhs._scale + rhs._scale})};
    }

    template<typename Rep1, Radix Rad1>
    inline fixed_point<Rep1, Rad1> operator/(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        assertion_exception_msg(!division_overflow<Rep1>(lhs._value, rhs._value), "fixed_point overflow");
        return fixed_point<Rep1, Rad1>{
            scaled_integer<Rep1>(lhs._value / rhs._value, scale_type{lhs._scale - rhs._scale})};
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator==(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value == rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator!=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value != rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator<=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value <= rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator>=(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value >= rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator<(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value < rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline bool operator>(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        return lhs.rescaled(scale)._value > rhs.rescaled(scale)._value;
    }

    template<typename Rep1, Radix Rad1>
    inline fixed_point<Rep1, Rad1> operator%(fixed_point<Rep1, Rad1> const& lhs, fixed_point<Rep1, Rad1> const& rhs) {
        auto const scale = std::min(lhs._scale, rhs._scale);
        auto const remainder = lhs.rescaled(scale)._value % rhs.rescaled(scale)._value;
        return fixed_point<Rep1, Rad1>{scaled_integer<Rep1>{remainder, scale}};
    }

    using decimal32 = fixed_point<int32_t, Radix::BASE_10>;
    using decimal64 = fixed_point<int64_t, Radix::BASE_10>;
    using decimal128 = fixed_point<absl::int128, Radix::BASE_10>;

} // namespace core::numbers
