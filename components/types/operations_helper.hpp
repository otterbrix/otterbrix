#pragma once

#include "types.hpp"
#include <boost/math/special_functions/factorials.hpp>

namespace components::types {

    template<typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
    static inline bool is_equals(T x, T y) {
        return std::fabs(x - y) < std::numeric_limits<T>::epsilon();
    }

    // This could be useful in other places, but for now it is here
    // Default only accepts int as amount
    constexpr int128_t operator<<(int128_t lhs, int128_t amount) { return lhs << static_cast<int>(amount); }
    constexpr int128_t operator>>(int128_t lhs, int128_t amount) { return lhs >> static_cast<int>(amount); }

    // there is no std::shift_left operator
    template<typename T = void>
    struct shift_left;
    template<typename T = void>
    struct shift_right;
    template<typename T = void>
    struct pow;
    template<typename T = void>
    struct sqrt;
    template<typename T = void>
    struct cbrt;
    template<typename T = void>
    struct fact;
    template<typename T = void>
    struct abs;

    template<>
    struct shift_left<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            return std::forward<T>(t) << std::forward<U>(u);
        }
    };

    template<>
    struct shift_right<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            return std::forward<T>(t) >> std::forward<U>(u);
        }
    };

    template<>
    struct pow<void> {
        template<typename T, typename U>
        constexpr auto operator()(T&& t, U&& u) const {
            if constexpr (std::is_same<T, int128_t>::value) {
                return t ^ u;
            } else {
                return std::pow(std::forward<T>(t), std::forward<U>(u));
            }
        }
    };

    template<>
    struct sqrt<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return std::sqrt(std::forward<T>(x));
        }
    };

    template<>
    struct cbrt<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return std::cbrt(std::forward<T>(x));
        }
    };

    template<>
    struct fact<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            return boost::math::factorial<double>(std::forward<T>(x));
        }
    };

    template<>
    struct abs<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            if constexpr (std::is_same<T, int128_t>::value) {
                return x < 0 ? -x : x;
            } else {
                return std::abs<T>(std::forward<T>(x));
            }
        }
    };

    template<template<typename...> class Callback, typename... Args>
    auto simple_physical_type_switch(physical_type type, Args&&... args) {
        Callback callback{};
        switch (type) {
            case physical_type::BOOL:
                return callback.template operator()<bool>(std::forward<Args>(args)...);
            case physical_type::UINT8:
                return callback.template operator()<uint8_t>(std::forward<Args>(args)...);
            case physical_type::INT8:
                return callback.template operator()<int8_t>(std::forward<Args>(args)...);
            case physical_type::UINT16:
                return callback.template operator()<uint16_t>(std::forward<Args>(args)...);
            case physical_type::INT16:
                return callback.template operator()<int16_t>(std::forward<Args>(args)...);
            case physical_type::UINT32:
                return callback.template operator()<uint32_t>(std::forward<Args>(args)...);
            case physical_type::INT32:
                return callback.template operator()<int32_t>(std::forward<Args>(args)...);
            case physical_type::UINT64:
                return callback.template operator()<uint64_t>(std::forward<Args>(args)...);
            case physical_type::INT64:
                return callback.template operator()<int64_t>(std::forward<Args>(args)...);
            // case physical_type::UINT128:
            //     return callback.template operator()<uint128_t>(std::forward<Args>(args)...);
            // case physical_type::INT128:
            //     return callback.template operator()<int128_t>(std::forward<Args>(args)...);
            case physical_type::FLOAT:
                return callback.template operator()<float>(std::forward<Args>(args)...);
            case physical_type::DOUBLE:
                return callback.template operator()<double>(std::forward<Args>(args)...);
            case physical_type::STRING:
                return callback.template operator()<std::string_view>(std::forward<Args>(args)...);
            // case physical_type::NA:
            //     return callback.template operator()<nullptr_t>(std::forward<Args>(args)...);
            default:
                throw std::logic_error("simple_physical_type_switch got a physical type that it can not handle");
        }
    }

    template<template<typename...> class DoubleCallback, typename TypeLeft, typename... Args>
    auto simple_physical_type_switch(physical_type type, Args&&... args) {
        DoubleCallback double_callback{};
        switch (type) {
            case physical_type::BOOL:
                return double_callback.template operator()<TypeLeft, bool>(std::forward<Args>(args)...);
            case physical_type::UINT8:
                return double_callback.template operator()<TypeLeft, uint8_t>(std::forward<Args>(args)...);
            case physical_type::INT8:
                return double_callback.template operator()<TypeLeft, int8_t>(std::forward<Args>(args)...);
            case physical_type::UINT16:
                return double_callback.template operator()<TypeLeft, uint16_t>(std::forward<Args>(args)...);
            case physical_type::INT16:
                return double_callback.template operator()<TypeLeft, int16_t>(std::forward<Args>(args)...);
            case physical_type::UINT32:
                return double_callback.template operator()<TypeLeft, uint32_t>(std::forward<Args>(args)...);
            case physical_type::INT32:
                return double_callback.template operator()<TypeLeft, int32_t>(std::forward<Args>(args)...);
            case physical_type::UINT64:
                return double_callback.template operator()<TypeLeft, uint64_t>(std::forward<Args>(args)...);
            case physical_type::INT64:
                return double_callback.template operator()<TypeLeft, int64_t>(std::forward<Args>(args)...);
            case physical_type::UINT128:
                return double_callback.template operator()<TypeLeft, uint128_t>(std::forward<Args>(args)...);
            case physical_type::INT128:
                return double_callback.template operator()<TypeLeft, int128_t>(std::forward<Args>(args)...);
            case physical_type::FLOAT:
                return double_callback.template operator()<TypeLeft, float>(std::forward<Args>(args)...);
            case physical_type::DOUBLE:
                return double_callback.template operator()<TypeLeft, double>(std::forward<Args>(args)...);
            case physical_type::STRING:
                return double_callback.template operator()<TypeLeft, std::string_view>(std::forward<Args>(args)...);
            // case physical_type::NA:
            //     return double_callback.template operator()<TypeLeft, nullptr_t>(std::forward<Args>(args)...);
            default:
                throw std::logic_error("simple_physical_type_switch got a physical type that it can not handle");
        }
    }

    template<template<typename...> class DoubleCallback, typename... Args>
    auto double_simple_physical_type_switch(physical_type type_left, physical_type type_right, Args&&... args) {
        switch (type_left) {
            case physical_type::BOOL:
                return simple_physical_type_switch<DoubleCallback, bool>(type_right, std::forward<Args>(args)...);
            case physical_type::UINT8:
                return simple_physical_type_switch<DoubleCallback, uint8_t>(type_right, std::forward<Args>(args)...);
            case physical_type::INT8:
                return simple_physical_type_switch<DoubleCallback, int8_t>(type_right, std::forward<Args>(args)...);
            case physical_type::UINT16:
                return simple_physical_type_switch<DoubleCallback, uint16_t>(type_right, std::forward<Args>(args)...);
            case physical_type::INT16:
                return simple_physical_type_switch<DoubleCallback, int16_t>(type_right, std::forward<Args>(args)...);
            case physical_type::UINT32:
                return simple_physical_type_switch<DoubleCallback, uint32_t>(type_right, std::forward<Args>(args)...);
            case physical_type::INT32:
                return simple_physical_type_switch<DoubleCallback, int32_t>(type_right, std::forward<Args>(args)...);
            case physical_type::UINT64:
                return simple_physical_type_switch<DoubleCallback, uint64_t>(type_right, std::forward<Args>(args)...);
            case physical_type::INT64:
                return simple_physical_type_switch<DoubleCallback, int64_t>(type_right, std::forward<Args>(args)...);
            case physical_type::UINT128:
                return simple_physical_type_switch<DoubleCallback, uint128_t>(type_right, std::forward<Args>(args)...);
            case physical_type::INT128:
                return simple_physical_type_switch<DoubleCallback, int128_t>(type_right, std::forward<Args>(args)...);
            case physical_type::FLOAT:
                return simple_physical_type_switch<DoubleCallback, float>(type_right, std::forward<Args>(args)...);
            case physical_type::DOUBLE:
                return simple_physical_type_switch<DoubleCallback, double>(type_right, std::forward<Args>(args)...);
            case physical_type::STRING:
                return simple_physical_type_switch<DoubleCallback, std::string_view>(type_right,
                                                                                     std::forward<Args>(args)...);
            // case physical_type::NA:
            //     return simple_physical_type_switch<DoubleCallback, nullptr_t>(type_right, std::forward<Args>(args)...);
            default:
                throw std::logic_error("simple_physical_type_switch got a physical type that it can not handle");
        }
    }

} // namespace components::types