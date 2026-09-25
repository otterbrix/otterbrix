#pragma once

#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace services::index {

    // One contiguous run of physical row ids: [row_start, row_start + row_count).
    struct index_row_range_t {
        uint64_t row_start{0};
        uint64_t row_count{0};
    };

    // built_compact_epoch is captured before the send, so a rebuild can only make it too low
    struct index_search_result_t {
        std::pmr::vector<int64_t> row_ids;
        uint64_t built_compact_epoch{0};
    };

    // (key, row id) pairs for one index
    struct key_batch_t {
        key_batch_t(std::pmr::memory_resource* resource)
            : keys(resource)
            , ids(resource) {}

        std::pmr::vector<std::pair<components::vector::vector_t, size_t>> keys;
        std::pmr::vector<int64_t> ids;

        size_t size() const noexcept { return ids.size(); }
        bool empty() const noexcept { return ids.empty(); }
    };

    void append_key_batch(key_batch_t* target, const key_batch_t& source, std::pmr::memory_resource* resource);

    // Calls `emit(row, key_bytes)` for every row of `keys`, NULL rows included.
    template<typename emit_t>
    [[nodiscard]] core::error_t for_each_key_bytes(const components::vector::vector_t& keys,
                                                   size_t count,
                                                   std::pmr::string* key_buffer,
                                                   emit_t&& emit) {
        using components::types::logical_type;
        using components::types::physical_type;
        const auto& type = keys.type();
        char head_bytes[3];
        size_t head_size = 0;
        head_bytes[head_size++] = static_cast<char>(type.type());
        if (type.type() == logical_type::DECIMAL) {
            const auto* decimal =
                static_cast<const components::types::decimal_logical_type_extension*>(type.extension());
            head_bytes[head_size++] = static_cast<char>(decimal->width());
            head_bytes[head_size++] = static_cast<char>(decimal->scale());
        }
        std::string_view head(head_bytes, head_size);
        const auto& validity = keys.validity();
        const auto emit_null = [&](size_t row) {
            key_buffer->assign(1, static_cast<char>(logical_type::NA));
            return emit(row, std::string_view(*key_buffer));
        };

        const auto fixed = [&]<typename T>() -> core::error_t {
            const auto* data = keys.data<T>();
            for (size_t row = 0; row < count; ++row) {
                if (!validity.row_is_valid(row)) {
                    RETURN_IF_ERROR(emit_null(row));
                    continue;
                }
                key_buffer->assign(head);
                key_buffer->append(reinterpret_cast<const char*>(data + row), sizeof(T));
                RETURN_IF_ERROR(emit(row, std::string_view(*key_buffer)));
            }
            return core::error_t::no_error();
        };

        switch (type.to_physical_type()) {
            case physical_type::BOOL:
                return fixed.template operator()<bool>();
            case physical_type::INT8:
                return fixed.template operator()<int8_t>();
            case physical_type::UINT8:
                return fixed.template operator()<uint8_t>();
            case physical_type::INT16:
                return fixed.template operator()<int16_t>();
            case physical_type::UINT16:
                return fixed.template operator()<uint16_t>();
            case physical_type::INT32:
                return fixed.template operator()<int32_t>();
            case physical_type::UINT32:
                return fixed.template operator()<uint32_t>();
            case physical_type::INT64:
                return fixed.template operator()<int64_t>();
            case physical_type::UINT64:
                return fixed.template operator()<uint64_t>();
            case physical_type::INT128:
                return fixed.template operator()<components::types::int128_t>();
            case physical_type::FLOAT:
                return fixed.template operator()<float>();
            case physical_type::DOUBLE:
                return fixed.template operator()<double>();
            case physical_type::STRING: {
                const auto* texts = keys.data<std::string_view>();
                for (size_t row = 0; row < count; ++row) {
                    if (!validity.row_is_valid(row)) {
                        RETURN_IF_ERROR(emit_null(row));
                        continue;
                    }
                    const auto length = static_cast<uint32_t>(texts[row].size());
                    key_buffer->assign(head);
                    key_buffer->append(reinterpret_cast<const char*>(&length), sizeof(length));
                    key_buffer->append(texts[row].data(), length);
                    RETURN_IF_ERROR(emit(row, std::string_view(*key_buffer)));
                }
                return core::error_t::no_error();
            }
            default:
                return core::error_t{core::error_code_t::invalid_parameter,
                                     std::pmr::string{"index: the key type has no encoding", keys.resource()}};
        }
    }

} // namespace services::index
