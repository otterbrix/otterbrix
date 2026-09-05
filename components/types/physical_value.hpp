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
        explicit physical_value() = default; // std::nullptr_t
        // string-like
        template<typename T>
        requires(core::IsBufferLike<T>) explicit physical_value(const T& value)
            : physical_value(value.data(), static_cast<uint32_t>(value.size())) {}
        explicit physical_value(const char* data, uint32_t size);
        // all integral types, including the 16-byte absl int128 family
        template<typename T>
        requires(!core::IsBufferLike<T>) explicit physical_value(T value)
            : type_(physical_value::get_type_<T>()) {
            if constexpr (sizeof(T) == 16) {
                std::memcpy(&data_, &value, sizeof(data_));
                std::memcpy(&data_hi_, reinterpret_cast<const char*>(&value) + sizeof(data_), sizeof(data_hi_));
            } else {
                std::memcpy(&data_, &value, sizeof(value));
            }
        }

        // No DECIMAL tag deliberately: its physical representation IS its storage integer
        // (decimal_storage_for_width), which gives the scan's compare_rows<T> semantics for free.

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
        // Recomputes both operand kinds from type_; the caller's lhs128/rhs128 only gate the call.
        bool less_128_(const physical_value& other) const noexcept;

        std::nullptr_t value_(std::integral_constant<physical_type, physical_type::NA>) const noexcept;
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
        int128_t value_(std::integral_constant<physical_type, physical_type::INT128>) const noexcept;
        uint128_t value_(std::integral_constant<physical_type, physical_type::UINT128>) const noexcept;
        std::string_view value_(std::integral_constant<physical_type, physical_type::STRING>) const noexcept;

        template<typename T>
        static constexpr physical_type get_type_() {
            if constexpr (std::is_same_v<T, bool>)
                return physical_type::BOOL;
            else if constexpr (std::is_same_v<T, uint8_t>)
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
            else if constexpr (std::is_same_v<T, int128_t>)
                return physical_type::INT128;
            else if constexpr (std::is_same_v<T, uint128_t>)
                return physical_type::UINT128;
            else
                // Not a runtime fallback to NA: an unlisted payload type (`long` on LP64, an
                // enum, a pointer) is a compile error instead of a silent NA-typed value.
                static_assert(sizeof(T) == 0, "physical_value: unsupported payload type");
        }

        physical_type type_ = physical_type::NA;
        bool memory_ownership = false; // for now is always false
        uint32_t size_ = 0;            // only for pointers
        uint64_t data_ = 0;            // low word: pointer / all <=8-byte payloads
        uint64_t data_hi_ = 0;         // high word of the 16-byte payloads (INT128/UINT128/DECIMAL)
    };

    // 16 -> 24: sizeof is baked into core/b_plus_tree/block.hpp and segment_tree block_metadata;
    // a disk-format break, allowed only because the format is pre-release (main_header_t::CURRENT_VERSION == 0).
    static_assert(sizeof(physical_value) == 24);
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