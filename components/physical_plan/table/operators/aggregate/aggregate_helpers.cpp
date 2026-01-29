#include "aggregate_helpers.hpp"

#include <algorithm>

namespace components::table::operators::aggregate::impl {

    template<typename T = void>
    struct sum_operator_t;
    template<typename T = void>
    struct min_operator_t;
    template<typename T = void>
    struct max_operator_t;

    template<>
    struct sum_operator_t<void> {
        template<typename T>
        auto operator()(const vector::vector_t& v, size_t count) const {
            auto raw_sum = T();
            for (size_t i = 0; i < count; i++) {
                raw_sum = raw_sum + v.data<T>()[i];
            }
            return types::logical_value_t{raw_sum};
        }
        template<typename T, typename U>
        auto operator()(const vector::vector_t& v, size_t count) const {
            auto raw_sum = T();
            for (size_t i = 0; i < count; i++) {
                raw_sum += T(v.data<U>()[i]);
            }
            return types::logical_value_t{raw_sum};
        }
    };

    template<>
    struct min_operator_t<void> {
        template<typename T>
        auto operator()(const vector::vector_t& v, size_t count) const {
            return types::logical_value_t{*std::min_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector::vector_t& v, size_t count) const {
            return types::logical_value_t{T(*std::min_element(v.data<U>(), v.data<U>() + count))};
        }
    };

    template<>
    struct max_operator_t<void> {
        template<typename T>
        auto operator()(const vector::vector_t& v, size_t count) const {
            return types::logical_value_t{*std::max_element(v.data<T>(), v.data<T>() + count)};
        }
        template<typename T, typename U>
        auto operator()(const vector::vector_t& v, size_t count) const {
            return types::logical_value_t{T(*std::max_element(v.data<U>(), v.data<U>() + count))};
        }
    };

    template<template<typename...> class OP>
    types::logical_value_t operator_switch(const vector::vector_t& v, size_t count) {
        OP op{};
        switch (v.type().type()) {
            case types::logical_type::BOOLEAN:
                return op.template operator()<bool>(v, count);
            case types::logical_type::TINYINT:
                return op.template operator()<int8_t>(v, count);
            case types::logical_type::SMALLINT:
                return op.template operator()<int16_t>(v, count);
            case types::logical_type::INTEGER:
                return op.template operator()<int32_t>(v, count);
            case types::logical_type::BIGINT:
                return op.template operator()<int64_t>(v, count);
            case types::logical_type::HUGEINT:
                return op.template operator()<types::int128_t>(v, count);
            case types::logical_type::UTINYINT:
                return op.template operator()<uint8_t>(v, count);
            case types::logical_type::USMALLINT:
                return op.template operator()<uint16_t>(v, count);
            case types::logical_type::UINTEGER:
                return op.template operator()<uint32_t>(v, count);
            case types::logical_type::UBIGINT:
                return op.template operator()<uint64_t>(v, count);
            case types::logical_type::UHUGEINT:
                return op.template operator()<types::uint128_t>(v, count);
            case types::logical_type::TIMESTAMP_SEC:
                return op.template operator()<std::chrono::seconds, int64_t>(v, count);
            case types::logical_type::TIMESTAMP_MS:
                return op.template operator()<std::chrono::milliseconds, int64_t>(v, count);
            case types::logical_type::TIMESTAMP_US:
                return op.template operator()<std::chrono::microseconds, int64_t>(v, count);
            case types::logical_type::TIMESTAMP_NS:
                return op.template operator()<std::chrono::nanoseconds, int64_t>(v, count);
            case types::logical_type::DECIMAL: {
                // stored as int64_t, but this won't result in a proper type
                // intermediate logical_value_t could be avoided, but convenient for templates
                auto int_sum = op.template operator()<int64_t>(v, count);
                int_sum = types::logical_value_t::create_decimal(
                    int_sum.template value<int64_t>(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->width(),
                    static_cast<types::decimal_logical_type_extension*>(v.type().extension())->scale());
                return int_sum;
            }
            case types::logical_type::FLOAT:
                return op.template operator()<float>(v, count);
            case types::logical_type::DOUBLE:
                return op.template operator()<double>(v, count);
            case types::logical_type::STRING_LITERAL:
                return op.template operator()<std::string, std::string_view>(v, count);
            default:
                throw std::runtime_error("operators::aggregate::sum unable to process given types");
        }
        return types::logical_value_t(nullptr);
    }

    types::logical_value_t sum(const vector::vector_t& v, size_t count) {
        return operator_switch<sum_operator_t>(v, count);
    }

    types::logical_value_t min(const vector::vector_t& v, size_t count) {
        return operator_switch<min_operator_t>(v, count);
    }

    types::logical_value_t max(const vector::vector_t& v, size_t count) {
        return operator_switch<max_operator_t>(v, count);
    }

} // namespace components::table::operators::aggregate::impl