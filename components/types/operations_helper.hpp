#pragma once

#include "types.hpp"
#include <boost/math/special_functions/factorials.hpp>
#include <core/operations_helper.hpp>

namespace components::types {

    // Trait to detect numeric types, including absl int128 which std::is_arithmetic_v misses
    template<typename T>
    inline constexpr bool is_numeric_type_v =
        std::is_arithmetic_v<std::decay_t<T>> || std::is_same_v<std::decay_t<T>, int128_t> ||
        std::is_same_v<std::decay_t<T>, uint128_t>;

    // This could be useful in other places, but for now it is here
    // Default only accepts int as amount
    constexpr int128_t operator<<(int128_t lhs, int128_t amount) { return lhs << static_cast<int>(amount); }
    constexpr int128_t operator>>(int128_t lhs, int128_t amount) { return lhs >> static_cast<int>(amount); }

    // there is no std variants for them
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
            if constexpr (std::is_same_v<std::decay_t<T>, int128_t>) {
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
            return boost::math::factorial<double>(static_cast<unsigned>(std::forward<T>(x)));
        }
    };

    template<>
    struct abs<void> {
        template<typename T>
        constexpr auto operator()(T&& x) const {
            if constexpr (std::is_same_v<std::decay_t<T>, int128_t>) {
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
            case physical_type::UINT128:
                return callback.template operator()<uint128_t>(std::forward<Args>(args)...);
            case physical_type::INT128:
                return callback.template operator()<int128_t>(std::forward<Args>(args)...);
            case physical_type::FLOAT:
                return callback.template operator()<float>(std::forward<Args>(args)...);
            case physical_type::DOUBLE:
                return callback.template operator()<double>(std::forward<Args>(args)...);
            case physical_type::STRING:
                return callback.template operator()<std::string_view>(std::forward<Args>(args)...);
            // case physical_type::NA:
            //     return callback.template operator()<std::nullptr_t>(std::forward<Args>(args)...);
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
            //     return double_callback.template operator()<TypeLeft, std::nullptr_t>(std::forward<Args>(args)...);
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
            //     return simple_physical_type_switch<DoubleCallback, std::nullptr_t>(type_right, std::forward<Args>(args)...);
            default:
                throw std::logic_error("simple_physical_type_switch got a physical type that it can not handle");
        }
    }

    static constexpr double DOUBLE_POWERS_OF_TEN[]{1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
                                                   1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
                                                   1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29,
                                                   1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39};

    static constexpr int128_t POWERS_OF_TEN[]{
        absl::MakeInt128(0, 1ull),
        absl::MakeInt128(0, 10ull),
        absl::MakeInt128(0, 100ull),
        absl::MakeInt128(0, 1000ull),
        absl::MakeInt128(0, 10000ull),
        absl::MakeInt128(0, 100000ull),
        absl::MakeInt128(0, 1000000ull),
        absl::MakeInt128(0, 10000000ull),
        absl::MakeInt128(0, 100000000ull),
        absl::MakeInt128(0, 1000000000ull),
        absl::MakeInt128(0, 10000000000ull),
        absl::MakeInt128(0, 100000000000ull),
        absl::MakeInt128(0, 1000000000000ull),
        absl::MakeInt128(0, 10000000000000ull),
        absl::MakeInt128(0, 100000000000000ull),
        absl::MakeInt128(0, 1000000000000000ull),
        absl::MakeInt128(0, 10000000000000000ull),
        absl::MakeInt128(0, 100000000000000000ull),
        absl::MakeInt128(0, 1000000000000000000ull),
        absl::MakeInt128(0, 10000000000000000000ull),
        // some bit magic to maintain constexpr nature of the array
        // there is a test to validate those hex values
        absl::MakeInt128(0x5, 0x6BC75E2D63100000),
        absl::MakeInt128(0x36, 0x35C9ADC5DEA00000),
        absl::MakeInt128(0x21E, 0x19E0C9BAB2400000),
        absl::MakeInt128(0x152D, 0x2C7E14AF6800000),
        absl::MakeInt128(0xD3C2, 0x1BCECCEDA1000000),
        absl::MakeInt128(0x84595, 0x161401484A000000),
        absl::MakeInt128(0x52B7D2, 0xDCC80CD2E4000000),
        absl::MakeInt128(0x33B2E3C, 0x9FD0803CE8000000),
        absl::MakeInt128(0x204FCE5E, 0x3E25026110000000),
        absl::MakeInt128(0x1431E0FAE, 0x6D7217CAA0000000),
        absl::MakeInt128(0xC9F2C9CD0, 0x4674EDEA40000000),
        absl::MakeInt128(0x7E37BE2022, 0xC0914B2680000000),
        absl::MakeInt128(0x4EE2D6D415B, 0x85ACEF8100000000),
        absl::MakeInt128(0x314DC6448D93, 0x38C15B0A00000000),
        absl::MakeInt128(0x1ED09BEAD87C0, 0x378D8E6400000000),
        absl::MakeInt128(0x13426172C74D82, 0x2B878FE800000000),
        absl::MakeInt128(0xC097CE7BC90715, 0xB34B9F1000000000),
        absl::MakeInt128(0x785EE10D5DA46D9, 0xF436A000000000)
        // There is another power of 10, but negative counterpart can not be represented, so we are not using it
        //absl::MakeInt128(0x4B3B4CA85A86C47A, 0x98A224000000000)
    };

    // double supports up to 15 decimal places, so we stop there
    static constexpr std::string_view FORMAT_PRECISION[]{"{:.0f}",
                                                         "{:.1f}",
                                                         "{:.2f}",
                                                         "{:.3f}",
                                                         "{:.4f}",
                                                         "{:.5f}",
                                                         "{:.6f}",
                                                         "{:.7f}",
                                                         "{:.8f}",
                                                         "{:.9f}",
                                                         "{:.10f}",
                                                         "{:.11f}",
                                                         "{:.12f}",
                                                         "{:.13f}",
                                                         "{:.14f}",
                                                         "{:.15f}"};

    struct decimal_limits {
        template<typename T>
        static constexpr T pos_inf() {
            return std::numeric_limits<T>::max();
        }
        template<typename T>
        static constexpr T neg_inf() {
            return std::numeric_limits<T>::min();
        }
        template<typename T>
        static constexpr T nan() {
            return std::numeric_limits<T>::min() + 1;
        }
    };

    template<>
    constexpr int128_t decimal_limits::pos_inf<int128_t>() {
        return absl::Int128Max();
    }

    template<>
    constexpr int128_t decimal_limits::neg_inf<int128_t>() {
        return absl::Int128Min();
    }

    template<>
    constexpr int128_t decimal_limits::nan<int128_t>() {
        return absl::Int128Min() + 1;
    }

    template<typename To, typename From>
    requires(std::is_floating_point_v<From>) To double_to_decimal(From source, uint8_t width, uint8_t scale) {
        if (std::isinf(source)) {
            if (std::signbit(source)) {
                return decimal_limits::neg_inf<To>();
            } else {
                return decimal_limits::pos_inf<To>();
            }
        } else if (std::isnan(source)) {
            return decimal_limits::nan<To>();
        }
        double value = source * DOUBLE_POWERS_OF_TEN[scale];
        double roundedValue = round(value);
        if (roundedValue <= -DOUBLE_POWERS_OF_TEN[width]) {
            return decimal_limits::neg_inf<To>();
        } else if (roundedValue >= DOUBLE_POWERS_OF_TEN[width]) {
            return decimal_limits::pos_inf<To>();
        }
        return static_cast<To>(static_cast<From>(roundedValue));
    }

    template<typename To>
    To int_to_decimal(int128_t input, uint8_t width, uint8_t scale) {
        int128_t max_width = POWERS_OF_TEN[width - scale];
        if (input >= max_width) {
            return decimal_limits::pos_inf<To>();
        } else if (input <= -max_width) {
            return decimal_limits::neg_inf<To>();
        } else {
            return static_cast<To>(input * POWERS_OF_TEN[scale]);
        }
    }

    template<typename To, typename From>
    To to_decimal(From source, uint8_t width, uint8_t scale) {
        if constexpr (std::is_floating_point_v<From>) {
            return double_to_decimal<To, From>(source, width, scale);
        } else {
            if constexpr (sizeof(From) < sizeof(int)) {
                // int128 does not have a constructor for smaller types and compiler complains about it
                return int_to_decimal<To>(static_cast<int128_t>(static_cast<int>(source)), width, scale);
            } else if constexpr (!std::is_same_v<From, int128_t>) {
                return int_to_decimal<To>(static_cast<int128_t>(source), width, scale);
            } else {
                return int_to_decimal<To>(source, width, scale);
            }
        }
    }

    std::pmr::string format_decimal(std::pmr::memory_resource* resource, int128_t value, uint8_t width, uint8_t scale);
    std::string format_decimal(int128_t value, uint8_t width, uint8_t scale);

    template<typename From>
    std::pmr::string
    decimal_to_string(std::pmr::memory_resource* resource, From decimal_storage, uint8_t width, uint8_t scale) {
        if (decimal_storage == decimal_limits::pos_inf<From>()) {
            return {"Infinity", resource};
        } else if (decimal_storage == decimal_limits::neg_inf<From>()) {
            return {"-Infinity", resource};
        } else if (decimal_storage == decimal_limits::nan<From>()) {
            return {"NaN", resource};
        } else {
            return format_decimal(resource, decimal_storage, width, scale);
        }
    }

    template<typename From>
    std::string decimal_to_string(From decimal_storage, uint8_t width, uint8_t scale) {
        if (decimal_storage == decimal_limits::pos_inf<From>()) {
            return "Infinity";
        } else if (decimal_storage == decimal_limits::neg_inf<From>()) {
            return "-Infinity";
        } else if (decimal_storage == decimal_limits::nan<From>()) {
            return "NaN";
        } else {
            return format_decimal(decimal_storage, width, scale);
        }
    }

} // namespace components::types