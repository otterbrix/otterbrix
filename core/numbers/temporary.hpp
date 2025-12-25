#pragma once

#include <dataframe/types.hpp>

#include <absl/numeric/int128.h>
#include <algorithm>
#include <limits>
#include <string>
#include <type_traits>

namespace core::numbers {

    template<typename T>
    auto to_string(T value) -> std::string {
        if constexpr (std::is_same_v<T, absl::int128>) {
            auto s = std::string{};
            auto const sign = value < 0;
            if (sign) {
                value += 1; // avoid overflowing if value == _int128_t lowest
                value *= -1;
                if (value == std::numeric_limits<absl::int128>::max())
                    return "-170141183460469231731687303715884105728";
                value += 1;
            }
            while (value) {
                s.push_back("0123456789"[value % 10]);
                value /= 10;
            }
            if (sign)
                s.push_back('-');
            std::reverse(s.begin(), s.end());
            return s;
        } else {
            return std::to_string(value);
        }
        return std::string{};
    }

    template<typename T>
    constexpr auto abs(T value) {
        return value >= 0 ? value : -value;
    }

    template<typename T>
    inline auto min(T lhs, T rhs) {
        return lhs < rhs ? lhs : rhs;
    }

    template<typename T>
    inline auto max(T lhs, T rhs) {
        return lhs > rhs ? lhs : rhs;
    }

    template<typename BaseType>
    constexpr auto exp10(int32_t exponent) {
        BaseType value = 1;
        while (exponent > 0) value *= 10, --exponent;
        return value;
    }

} // namespace core::numbers
