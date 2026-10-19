#include "index_types.hpp"

#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>

#include <cstring>

namespace services::index {

    core::result_wrapper_t<components::vector::vector_t> decode_key_bytes(std::pmr::memory_resource* resource,
                                                                          std::string_view bytes) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        namespace vec = components::vector;

        const auto refuse = [resource](const char* why) {
            return core::error_t{core::error_code_t::data_corruption, std::pmr::string{why, resource}};
        };
        if (bytes.empty()) {
            return refuse("index: a stored key has no type tag");
        }
        const auto tag = static_cast<logical_type>(static_cast<uint8_t>(bytes.front()));
        const auto payload = bytes.substr(1);
        const complex_logical_type type{tag};

        const auto scalar = [&]<typename T>() -> core::result_wrapper_t<vec::vector_t> {
            if (payload.size() != sizeof(T)) {
                return refuse("index: a stored key has a payload of the wrong length");
            }
            T value;
            std::memcpy(&value, payload.data(), sizeof(T));
            return vec::make_scalar<T>(resource, type, value);
        };

        switch (tag) {
            case logical_type::NA:
                if (!payload.empty()) {
                    return refuse("index: a stored NULL key carries a payload");
                }
                return vec::make_null(resource, type);
            case logical_type::BOOLEAN: {
                if (payload.size() != 1) {
                    return refuse("index: a stored key has a payload of the wrong length");
                }
                return vec::make_scalar<bool>(resource, type, payload.front() != 0);
            }
            case logical_type::TINYINT:
                return scalar.template operator()<int8_t>();
            case logical_type::UTINYINT:
                return scalar.template operator()<uint8_t>();
            case logical_type::SMALLINT:
                return scalar.template operator()<int16_t>();
            case logical_type::USMALLINT:
                return scalar.template operator()<uint16_t>();
            case logical_type::INTEGER:
            case logical_type::DATE:
                return scalar.template operator()<int32_t>();
            case logical_type::UINTEGER:
                return scalar.template operator()<uint32_t>();
            case logical_type::BIGINT:
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                return scalar.template operator()<int64_t>();
            case logical_type::UBIGINT:
                return scalar.template operator()<uint64_t>();
            case logical_type::FLOAT:
                return scalar.template operator()<float>();
            case logical_type::DOUBLE:
                return scalar.template operator()<double>();
            case logical_type::STRING_LITERAL: {
                uint32_t length = 0;
                if (payload.size() < sizeof(length)) {
                    return refuse("index: a stored string key is shorter than its length prefix");
                }
                std::memcpy(&length, payload.data(), sizeof(length));
                if (payload.size() - sizeof(length) != length) {
                    return refuse("index: a stored string key disagrees with its length prefix");
                }
                return vec::make_scalar<std::string_view>(resource, type, payload.substr(sizeof(length)));
            }
            case logical_type::DECIMAL: {
                if (payload.size() < 2) {
                    return refuse("index: a stored DECIMAL key is shorter than its width and scale");
                }
                auto decimal_result = complex_logical_type::create_decimal(resource,
                                                                           static_cast<uint8_t>(payload[0]),
                                                                           static_cast<uint8_t>(payload[1]));
                if (decimal_result.has_error()) {
                    return refuse("index: a stored DECIMAL key has a width or scale out of range");
                }
                const auto decimal = std::move(decimal_result.value());
                const auto unscaled = payload.substr(2);
                const auto widen = [&]<typename T>() -> core::result_wrapper_t<vec::vector_t> {
                    if (unscaled.size() != sizeof(T)) {
                        return refuse("index: a stored DECIMAL key has a payload of the wrong length");
                    }
                    T value;
                    std::memcpy(&value, unscaled.data(), sizeof(T));
                    return vec::make_decimal(resource, decimal, static_cast<components::types::int128_t>(value));
                };
                switch (decimal.to_physical_type()) {
                    case components::types::physical_type::INT16:
                        return widen.template operator()<int16_t>();
                    case components::types::physical_type::INT32:
                        return widen.template operator()<int32_t>();
                    case components::types::physical_type::INT64:
                        return widen.template operator()<int64_t>();
                    case components::types::physical_type::INT128:
                        return widen.template operator()<components::types::int128_t>();
                    default:
                        return refuse("index: a stored DECIMAL key has no known storage");
                }
            }
            default:
                return refuse("index: a stored key has a type tag no index key is written with");
        }
    }

    void append_key_batch(key_batch_t* target, const key_batch_t& source, std::pmr::memory_resource* resource) {
        using components::vector::DEFAULT_VECTOR_CAPACITY;
        target->ids.reserve(target->ids.size() + source.size());
        size_t offset = 0;
        for (const auto& [keys, rows] : source.keys) {
            const int64_t* ids = source.ids.data() + offset;
            offset += rows;
            size_t copied = 0;
            while (copied < rows) {
                if (target->keys.empty() || target->keys.back().second == DEFAULT_VECTOR_CAPACITY) {
                    target->keys.emplace_back(
                        components::vector::vector_t(resource, keys.type(), DEFAULT_VECTOR_CAPACITY),
                        size_t{0});
                }
                auto& [target_keys, target_rows] = target->keys.back();
                const size_t taken = std::min(DEFAULT_VECTOR_CAPACITY - target_rows, rows - copied);
                components::vector::vector_ops::copy(keys, target_keys, copied + taken, copied, target_rows);
                target->ids.insert(target->ids.end(), ids + copied, ids + copied + taken);
                target_rows += taken;
                copied += taken;
            }
        }
    }

} // namespace services::index
