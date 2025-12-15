#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <type_traits>

#include "types.hpp"
#include <core/operations_helper.hpp>

namespace components::types {

    class physical_value {
    public:
        // currently supported values
        // TODO: add memory ownership
        explicit physical_value() = default; // nullptr_t
        explicit physical_value(nullptr_t);
        // string-like
        template<typename T>
        physical_value(const T& value, typename std::enable_if<core::is_buffer_like<T>>::type* = nullptr)
            : physical_value(value.data(), static_cast<uint32_t>(value.size())) {}
        explicit physical_value(const char* data, uint32_t size);
        // all integral types
        template<typename T>
        physical_value(T value, typename std::enable_if<!core::is_buffer_like<T>>::type* = nullptr)
            : type_(physical_value::get_type_<T>()) {
            std::memcpy(&data_, &value, sizeof(value));
        }

        ~physical_value() = default;

        // if types convertable to each other, compared by value, otherwise returns physical type order
        bool operator<(const physical_value& other) const noexcept;
        bool operator>(const physical_value& other) const noexcept;
        bool operator==(const physical_value& other) const noexcept;
        bool operator!=(const physical_value& other) const noexcept;
        bool operator<=(const physical_value& other) const noexcept;
        bool operator>=(const physical_value& other) const noexcept;

        operator bool() const noexcept;

        template<physical_type Type>
        auto value() const noexcept {
            return value_(std::integral_constant<physical_type, Type>{});
        }

        physical_type type() const noexcept;

    private:
        nullptr_t value_(std::integral_constant<physical_type, physical_type::NA>) const noexcept;
        bool value_(std::integral_constant<physical_type, physical_type::BOOL>) const noexcept;
        uint8_t value_(std::integral_constant<physical_type, physical_type::UINT8>) const noexcept;
        uint16_t value_(std::integral_constant<physical_type, physical_type::UINT16>) const noexcept;
        uint32_t value_(std::integral_constant<physical_type, physical_type::UINT32>) const noexcept;
        uint64_t value_(std::integral_constant<physical_type, physical_type::UINT64>) const noexcept;
        int8_t value_(std::integral_constant<physical_type, physical_type::INT8>) const noexcept;
        int16_t value_(std::integral_constant<physical_type, physical_type::INT16>) const noexcept;
        int32_t value_(std::integral_constant<physical_type, physical_type::INT32>) const noexcept;
        int64_t value_(std::integral_constant<physical_type, physical_type::INT64>) const noexcept;
        float value_(std::integral_constant<physical_type, physical_type::FLOAT>) const noexcept;
        double value_(std::integral_constant<physical_type, physical_type::DOUBLE>) const noexcept;
        std::string_view value_(std::integral_constant<physical_type, physical_type::STRING>) const noexcept;

        template<typename T>
        static constexpr physical_type get_type_() {
            if constexpr (std::is_same_v<T, bool>)
                return physical_type::BOOL;
            if constexpr (std::is_same_v<T, uint8_t>)
                return physical_type::UINT8;
            else if constexpr (std::is_same_v<T, uint16_t>)
                return physical_type::UINT16;
            else if constexpr (std::is_same_v<T, uint32_t>)
                return physical_type::UINT32;
            else if constexpr (std::is_same_v<T, uint64_t>)
                return physical_type::UINT64;
            else if constexpr (std::is_same_v<T, int8_t>)
                return physical_type::INT8;
            else if constexpr (std::is_same_v<T, int16_t>)
                return physical_type::INT16;
            else if constexpr (std::is_same_v<T, int32_t>)
                return physical_type::INT32;
            else if constexpr (std::is_same_v<T, int64_t>)
                return physical_type::INT64;
            else if constexpr (std::is_same_v<T, float>)
                return physical_type::FLOAT;
            else if constexpr (std::is_same_v<T, double>)
                return physical_type::DOUBLE;
            //static_assert(false && "should be unreachable");
            return physical_type::NA;
        }

        physical_type type_ = physical_type::NA;
        bool memory_ownership = false; // for now is always false
        uint32_t size_ = 0;            // only for pointers
        uint64_t data_ = 0;            // buffer but allocated on a stack to make it trivially copyable
    };

    static_assert(sizeof(physical_value) == 16);
    static_assert(alignof(physical_value) == 8);
    static_assert(std::is_trivially_copyable_v<physical_value>);
    static_assert(std::is_trivially_copy_assignable_v<physical_value>);
    static_assert(std::is_trivially_move_assignable_v<physical_value>);

} // namespace components::types

namespace std {
    template<>
    class numeric_limits<components::types::physical_value> {
    public:
        static components::types::physical_value min() { return components::types::physical_value(false); }
        static components::types::physical_value max() { return components::types::physical_value(); }
    };
} // namespace std