#pragma once

#include <algorithm>
#include <components/vector/vector.hpp>

namespace components::table::operators::aggregate::impl {

    template<typename T>
    static types::logical_value_t sum(const T* array, size_t count) {
        auto raw_sum = T();
        for (size_t i = 0; i < count; i++) {
            raw_sum += array[i];
        }
        return types::logical_value_t{raw_sum};
    }

    template<typename T, typename U>
    static types::logical_value_t sum(const U* array, size_t count) {
        auto raw_sum = T();
        for (size_t i = 0; i < count; i++) {
            raw_sum += T(array[i]);
        }
        return types::logical_value_t{raw_sum};
    }

    static types::logical_value_t sum(const vector::vector_t& v, size_t count) {
        switch (v.type().type()) {
            case logical_type::BOOLEAN:
                return sum(v.data<bool>(), count);
            case logical_type::TINYINT:
                return sum(v.data<int8_t>(), count);
            case logical_type::SMALLINT:
                return sum(v.data<int16_t>(), count);
            case logical_type::INTEGER:
                return sum(v.data<int32_t>(), count);
            case logical_type::BIGINT:
                return sum(v.data<int64_t>(), count);
            case logical_type::HUGEINT:
                return sum(v.data<types::int128_t>(), count);
            case logical_type::UTINYINT:
                return sum(v.data<uint8_t>(), count);
            case logical_type::USMALLINT:
                return sum(v.data<uint16_t>(), count);
            case logical_type::UINTEGER:
                return sum(v.data<uint32_t>(), count);
            case logical_type::UBIGINT:
                return sum(v.data<uint64_t>(), count);
            case logical_type::UHUGEINT:
                return sum(v.data<types::uint128_t>(), count);
            case logical_type::TIMESTAMP_SEC:
                return sum<std::chrono::seconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_MS:
                return sum<std::chrono::milliseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_US:
                return sum<std::chrono::microseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_NS:
                return sum<std::chrono::nanoseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                auto int_sum = sum(v.data<int64_t>(), count);
                int_sum = types::logical_value_t::create_decimal(
                    int_sum.value<int64_t>(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return sum(v.data<float>(), count);
            case logical_type::DOUBLE:
                return sum(v.data<double>(), count);
            case logical_type::STRING_LITERAL:
                return sum<std::string, std::string_view>(v.data<std::string_view>(), count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return types::logical_value_t(nullptr);
    }

    template<typename T>
    static types::logical_value_t min(const T* array, size_t count) {
        return types::logical_value_t{*std::min_element(array, array + count)};
    }

    template<typename T, typename U>
    static types::logical_value_t min(const U* array, size_t count) {
        return types::logical_value_t{T(*std::min_element(array, array + count))};
    }

    static types::logical_value_t min(const vector::vector_t& v, size_t count) {
        switch (v.type().type()) {
            case logical_type::BOOLEAN:
                return min(v.data<bool>(), count);
            case logical_type::TINYINT:
                return min(v.data<int8_t>(), count);
            case logical_type::SMALLINT:
                return min(v.data<int16_t>(), count);
            case logical_type::INTEGER:
                return min(v.data<int32_t>(), count);
            case logical_type::BIGINT:
                return min(v.data<int64_t>(), count);
            case logical_type::HUGEINT:
                return min(v.data<types::int128_t>(), count);
            case logical_type::UTINYINT:
                return min(v.data<uint8_t>(), count);
            case logical_type::USMALLINT:
                return min(v.data<uint16_t>(), count);
            case logical_type::UINTEGER:
                return min(v.data<uint32_t>(), count);
            case logical_type::UBIGINT:
                return min(v.data<uint64_t>(), count);
            case logical_type::UHUGEINT:
                return min(v.data<types::uint128_t>(), count);
            case logical_type::TIMESTAMP_SEC:
                return min<std::chrono::seconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_MS:
                return min<std::chrono::milliseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_US:
                return min<std::chrono::microseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_NS:
                return min<std::chrono::nanoseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                auto int_sum = min(v.data<int64_t>(), count);
                int_sum = types::logical_value_t::create_decimal(
                    int_sum.value<int64_t>(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return min(v.data<float>(), count);
            case logical_type::DOUBLE:
                return min(v.data<double>(), count);
            case logical_type::STRING_LITERAL:
                return min<std::string, std::string_view>(v.data<std::string_view>(), count);
            default:
                throw std::runtime_error("operators::aggregate::min unable to process given types");
        }
        return types::logical_value_t(nullptr);
    }

    template<typename T>
    static types::logical_value_t max(const T* array, size_t count) {
        return types::logical_value_t{*std::max_element(array, array + count)};
    }

    template<typename T, typename U>
    static types::logical_value_t max(const U* array, size_t count) {
        return types::logical_value_t{T(*std::max_element(array, array + count))};
    }

    static types::logical_value_t max(const vector::vector_t& v, size_t count) {
        switch (v.type().type()) {
            case logical_type::BOOLEAN:
                return max(v.data<bool>(), count);
            case logical_type::TINYINT:
                return max(v.data<int8_t>(), count);
            case logical_type::SMALLINT:
                return max(v.data<int16_t>(), count);
            case logical_type::INTEGER:
                return max(v.data<int32_t>(), count);
            case logical_type::BIGINT:
                return max(v.data<int64_t>(), count);
            case logical_type::HUGEINT:
                return max(v.data<types::int128_t>(), count);
            case logical_type::UTINYINT:
                return max(v.data<uint8_t>(), count);
            case logical_type::USMALLINT:
                return max(v.data<uint16_t>(), count);
            case logical_type::UINTEGER:
                return max(v.data<uint32_t>(), count);
            case logical_type::UBIGINT:
                return max(v.data<uint64_t>(), count);
            case logical_type::UHUGEINT:
                return max(v.data<types::uint128_t>(), count);
            case logical_type::TIMESTAMP_SEC:
                return max<std::chrono::seconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_MS:
                return max<std::chrono::milliseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_US:
                return max<std::chrono::microseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::TIMESTAMP_NS:
                return max<std::chrono::nanoseconds, int64_t>(v.data<int64_t>(), count);
            case logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                auto int_sum = max(v.data<int64_t>(), count);
                int_sum = types::logical_value_t::create_decimal(
                    int_sum.value<int64_t>(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case logical_type::FLOAT:
                return max(v.data<float>(), count);
            case logical_type::DOUBLE:
                return max(v.data<double>(), count);
            case logical_type::STRING_LITERAL:
                return max<std::string, std::string_view>(v.data<std::string_view>(), count);
            default:
                throw std::runtime_error("operators::aggregate::max unable to process given types");
        }
        return types::logical_value_t(nullptr);
    }
} // namespace components::table::operators::aggregate::impl