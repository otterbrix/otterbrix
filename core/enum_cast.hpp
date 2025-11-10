#pragma once
#include <cassert>
#include <type_traits>

namespace core::enums {

    template<typename T>
    constexpr typename std::underlying_type<T>::type to_underlying_type(T enum_value) {
        static_assert(std::is_enum<T>::value);
        return static_cast<std::underlying_type_t<T>>(enum_value);
    }

    template<typename T>
    constexpr T from_underlying_type(typename std::underlying_type<T>::type value) {
        static_assert(std::is_enum<T>::value);
        return static_cast<T>(value);
    }

} // namespace core::enums