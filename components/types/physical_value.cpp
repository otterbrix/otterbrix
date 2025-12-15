#include "physical_value.hpp"

#include <core/operations_helper.hpp>

template<class T, class U>
constexpr bool cmp_less(T t, U u) noexcept {
    if constexpr (std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        return t < u;
    } else if constexpr (std::is_floating_point_v<T> && !std::is_floating_point_v<U>) {
        return t < T(u);
    } else if constexpr (!std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        return U(t) < u;
    } else if constexpr (std::is_signed_v<T> == std::is_signed_v<U>) {
        return t < u;
    } else if constexpr (std::is_signed_v<T>) {
        return t < 0 || std::make_unsigned_t<T>(t) < u;
    } else {
        return u >= 0 && t < std::make_unsigned_t<U>(u);
    }
}

namespace components::types {

    constexpr bool operator<(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) < static_cast<uint8_t>(rhs);
    }
    constexpr bool operator>(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) > static_cast<uint8_t>(rhs);
    }
    constexpr bool operator==(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) == static_cast<uint8_t>(rhs);
    }
    constexpr bool operator!=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) != static_cast<uint8_t>(rhs);
    }
    constexpr bool operator<=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) <= static_cast<uint8_t>(rhs);
    }
    constexpr bool operator>=(physical_type lhs, physical_type rhs) noexcept {
        return static_cast<uint8_t>(lhs) >= static_cast<uint8_t>(rhs);
    }

    bool is_integral_type(const physical_type type) noexcept {
        if (type >= physical_type::BOOL && type <= physical_type::DOUBLE) {
            return true;
        } else {
            return false;
        }
    }

    physical_value::physical_value(const char* data, uint32_t size)
        : type_(physical_type::STRING)
        , size_(size)
        , data_(reinterpret_cast<uint64_t>(data)) {
        assert(size <= uint32_t(-1));
    }

    bool physical_value::operator<(const physical_value& other) const noexcept {
        // types order:
        // 1) booleans
        // 2) numbers (including floating points) by value
        // 3) strings
        // 4) nulls

        // comparing types and values is a mess, but does a job

        // handle nulls
        if (type_ == physical_type::NA && other.type_ != physical_type::NA) {
            return false;
        }
        if (type_ != physical_type::NA && other.type_ == physical_type::NA) {
            return true;
        }
        if (type_ == physical_type::NA && other.type_ == physical_type::NA) {
            return false;
        }

        // handle booleans
        if (type_ == physical_type::BOOL && other.type_ != physical_type::BOOL) {
            return true;
        }
        if (type_ != physical_type::BOOL && other.type_ == physical_type::BOOL) {
            return false;
        }
        if (type_ == physical_type::BOOL && other.type_ == physical_type::BOOL) {
            return data_ < other.data_;
        }

        assert(!(type_ == physical_type::INT128 || type_ == physical_type::UINT128) &&
               "physical value does not support int128 for now");
        assert(!(other.type_ == physical_type::INT128 || other.type_ == physical_type::UINT128) &&
               "physical value does not support int128 for now");

        // handle strings

        if (type_ == physical_type::STRING && other.type_ == physical_type::STRING) {
            return std::string_view(reinterpret_cast<char*>(data_), size_) <
                   std::string_view(reinterpret_cast<char*>(other.data_), other.size_);
        }

        // handle non-comparable types

        if (!is_integral_type(type_) || !is_integral_type(other.type_)) {
            return type_ < other.type_;
        }

        // idealy use something like this
        // return value<type_>() < value<other.type_>();
        // but type is not a constexpr, so here is a huge switch:

#define OTHER_SWITCH                                                                                                   \
    switch (other.type_) {                                                                                             \
        case physical_type::UINT8:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::UINT8>());                                            \
        case physical_type::INT8:                                                                                      \
            return cmp_less(my_value, other.value<physical_type::INT8>());                                             \
        case physical_type::UINT16:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT16>());                                           \
        case physical_type::INT16:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT16>());                                            \
        case physical_type::UINT32:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT32>());                                           \
        case physical_type::INT32:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT32>());                                            \
        case physical_type::UINT64:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::UINT64>());                                           \
        case physical_type::INT64:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::INT64>());                                            \
        case physical_type::FLOAT:                                                                                     \
            return cmp_less(my_value, other.value<physical_type::FLOAT>());                                            \
        case physical_type::DOUBLE:                                                                                    \
            return cmp_less(my_value, other.value<physical_type::DOUBLE>());                                           \
        default:                                                                                                       \
            assert(false && "incorrect type");                                                                         \
            return false;                                                                                              \
    }

        switch (type_) {
            case physical_type::UINT8: {
                auto my_value = value<physical_type::UINT8>();
                OTHER_SWITCH
            }
            case physical_type::INT8: {
                auto my_value = value<physical_type::INT8>();
                OTHER_SWITCH
            }
            case physical_type::UINT16: {
                auto my_value = value<physical_type::UINT16>();
                OTHER_SWITCH
            }
            case physical_type::INT16: {
                auto my_value = value<physical_type::INT16>();
                OTHER_SWITCH
            }
            case physical_type::UINT32: {
                auto my_value = value<physical_type::UINT32>();
                OTHER_SWITCH
            }
            case physical_type::INT32: {
                auto my_value = value<physical_type::INT32>();
                OTHER_SWITCH
            }
            case physical_type::UINT64: {
                auto my_value = value<physical_type::UINT64>();
                OTHER_SWITCH
            }
            case physical_type::INT64: {
                auto my_value = value<physical_type::INT64>();
                OTHER_SWITCH
            }
            case physical_type::FLOAT: {
                auto my_value = value<physical_type::FLOAT>();
                OTHER_SWITCH
            }
            case physical_type::DOUBLE: {
                auto my_value = value<physical_type::DOUBLE>();
                OTHER_SWITCH
            }
            default:
                assert(false && "incorrect type");
                return false;
        }
    }

    bool physical_value::operator>(const physical_value& other) const noexcept { return other < *this; }

    bool physical_value::operator==(const physical_value& other) const noexcept {
        return !(*this < other) && !(*this > other);
    }

    bool physical_value::operator!=(const physical_value& other) const noexcept { return !(*this == other); }

    bool physical_value::operator<=(const physical_value& other) const noexcept { return !(*this > other); }

    bool physical_value::operator>=(const physical_value& other) const noexcept { return !(*this < other); }

    physical_value::operator bool() const noexcept { return data_; }

    physical_type physical_value::type() const noexcept { return type_; }

    nullptr_t physical_value::value_(std::integral_constant<physical_type, physical_type::NA>) const noexcept {
        assert(type_ == physical_type::NA);
        return nullptr;
    }

    bool physical_value::value_(std::integral_constant<physical_type, physical_type::BOOL>) const noexcept {
        assert(type_ == physical_type::BOOL);
        return data_;
    }

    uint8_t physical_value::value_(std::integral_constant<physical_type, physical_type::UINT8>) const noexcept {
        assert(type_ == physical_type::UINT8);
        return core::bit_cast<uint8_t>(data_);
    }

    uint16_t physical_value::value_(std::integral_constant<physical_type, physical_type::UINT16>) const noexcept {
        assert(type_ == physical_type::UINT16);
        return core::bit_cast<uint16_t>(data_);
    }

    uint32_t physical_value::value_(std::integral_constant<physical_type, physical_type::UINT32>) const noexcept {
        assert(type_ == physical_type::UINT32);
        return core::bit_cast<uint32_t>(data_);
    }

    uint64_t physical_value::value_(std::integral_constant<physical_type, physical_type::UINT64>) const noexcept {
        assert(type_ == physical_type::UINT64);
        return data_;
    }

    int8_t physical_value::value_(std::integral_constant<physical_type, physical_type::INT8>) const noexcept {
        assert(type_ == physical_type::INT8);
        return core::bit_cast<int8_t>(data_);
    }

    int16_t physical_value::value_(std::integral_constant<physical_type, physical_type::INT16>) const noexcept {
        assert(type_ == physical_type::INT16);
        return core::bit_cast<int16_t>(data_);
    }

    int32_t physical_value::value_(std::integral_constant<physical_type, physical_type::INT32>) const noexcept {
        assert(type_ == physical_type::INT32);
        return core::bit_cast<int32_t>(data_);
    }

    int64_t physical_value::value_(std::integral_constant<physical_type, physical_type::INT64>) const noexcept {
        assert(type_ == physical_type::INT64);
        return core::bit_cast<int64_t>(data_);
    }

    float physical_value::value_(std::integral_constant<physical_type, physical_type::FLOAT>) const noexcept {
        assert(type_ == physical_type::FLOAT);
        return core::bit_cast<float>(data_);
    }

    double physical_value::value_(std::integral_constant<physical_type, physical_type::DOUBLE>) const noexcept {
        assert(type_ == physical_type::DOUBLE);
        return core::bit_cast<double>(data_);
    }

    std::string_view
    physical_value::value_(std::integral_constant<physical_type, physical_type::STRING>) const noexcept {
        assert(type_ == physical_type::STRING);
        return {reinterpret_cast<const char*>(data_), size_};
    }

} // namespace components::types