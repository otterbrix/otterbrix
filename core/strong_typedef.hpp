#pragma once

#include <cstdint>

#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <type_traits>

#include <boost/operators.hpp>

#define STRONG_TYPEDEF(T, D)                                                                                           \
    namespace core {                                                                                                   \
        struct D : boost::totally_ordered1<D> {                                                                        \
            typedef T base_type;                                                                                       \
            T t;                                                                                                       \
            constexpr explicit D(const T& t_) noexcept(std::is_nothrow_copy_constructible_v<T>)                        \
                : t(t_) {}                                                                                             \
            D()                                                                                                        \
            noexcept(std::is_nothrow_default_constructible_v<T>)                                                       \
                : t() {}                                                                                               \
            D& operator=(const T& other) noexcept(std::is_nothrow_assignable_v<T, T>) {                                \
                t = other;                                                                                             \
                return *this;                                                                                          \
            }                                                                                                          \
            operator const T&() const { return t; }                                                                    \
            operator T&() { return t; }                                                                                \
            bool operator==(const D& other) const { return t == other.t; }                                             \
            bool operator<(const D& other) const { return t < other.t; }                                               \
        };                                                                                                             \
                                                                                                                       \
        inline std::ostream& operator<<(std::ostream& stream, const D& value) { return stream << value.t; }            \
                                                                                                                       \
    } /* NOLINT */                                                                                                     \
                                                                                                                       \
    namespace std {                                                                                                    \
        template<>                                                                                                     \
        struct hash<::core::D> {                                                                                       \
            std::size_t operator()(const ::core::D& x) const { return hash<T>{}(x); }                                  \
        };                                                                                                             \
        template<>                                                                                                     \
                                                                                                                       \
        struct numeric_limits<::core::D> {                                                                             \
            static typename std::enable_if_t<std::is_arithmetic_v<T>, ::core::D> min() {                               \
                return ::core::D(numeric_limits<T>::min());                                                            \
            }                                                                                                          \
            static typename std::enable_if_t<std::is_arithmetic_v<T>, ::core::D> max() {                               \
                return ::core::D(numeric_limits<T>::max());                                                            \
            }                                                                                                          \
        };                                                                                                             \
    } /* NOLINT */                                                                                                     \
    static_assert(true, "End call of macro with a semicolon")

#define STRONG_TYPEDEF_EXPLICIT(T, D)                                                                                  \
    namespace core {                                                                                                   \
        struct D : boost::totally_ordered1<D> {                                                                        \
            typedef T base_type;                                                                                       \
            T t;                                                                                                       \
            explicit D(const T& t_) noexcept(std::is_nothrow_copy_constructible_v<T>)                                  \
                : t(t_) {}                                                                                             \
            explicit D(T&& t_) noexcept(std::is_nothrow_move_constructible_v<T>)                                       \
                : t(std::move(t_)) {}                                                                                  \
            D()                                                                                                        \
            noexcept(std::is_nothrow_default_constructible_v<T>)                                                       \
                : t() {}                                                                                               \
            D& operator=(const T& other) noexcept(std::is_nothrow_assignable_v<T, T>) {                                \
                t = other;                                                                                             \
                return *this;                                                                                          \
            }                                                                                                          \
            explicit operator const T&() const noexcept { return t; }                                                  \
            explicit operator T&() noexcept { return t; }                                                              \
            bool operator==(const D& other) const { return t == other.t; }                                             \
            bool operator<(const D& other) const { return t < other.t; }                                               \
        };                                                                                                             \
                                                                                                                       \
        inline std::ostream& operator<<(std::ostream& stream, const D& value) { return stream << value.t; }            \
                                                                                                                       \
    } /* NOLINT */                                                                                                     \
                                                                                                                       \
    namespace std {                                                                                                    \
        template<>                                                                                                     \
        struct hash<::core::D> {                                                                                       \
            std::size_t operator()(const ::core::D& x) const { return hash<T>{}(x.t); }                                \
        };                                                                                                             \
    } /* NOLINT */                                                                                                     \
    static_assert(true, "End call of macro with a semicolon")
