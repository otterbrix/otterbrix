#include "row_group.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <unordered_map>

#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_manager.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <vector/data_chunk.hpp>

#include "collection.hpp"
#include "row_version_manager.hpp"
#include "standard_column_data.hpp"
#include "struct_column_data.hpp"
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector_operations.hpp>

namespace {

    constexpr uint16_t PAX_FIXED_ROWS_PER_PAGE = 128;
    constexpr uint16_t PAX_GENERIC_ROWS_PER_PAGE = 128;
    constexpr uint32_t PAX_STRING_DICTIONARY_HEADER_SIZE = sizeof(uint32_t) * 5;
    constexpr uint32_t PAX_STRING_BIG_MARKER_SIZE = sizeof(uint32_t) + sizeof(int32_t);
    constexpr uint64_t PAX_STRING_DEFAULT_BLOCK_LIMIT = 4096;

    bool is_pax_fixed_integer_type(const components::types::complex_logical_type& type) {
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
            case logical_type::BIGINT:
            case logical_type::UBIGINT:
                return true;
            default:
                return false;
        }
    }

    components::table::storage::pax_fixed_column_type
    to_pax_fixed_column_type(const components::types::complex_logical_type& type) {
        using components::table::storage::pax_fixed_column_type;
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::TINYINT:
                return pax_fixed_column_type::INT8;
            case logical_type::UTINYINT:
                return pax_fixed_column_type::UINT8;
            case logical_type::SMALLINT:
                return pax_fixed_column_type::INT16;
            case logical_type::USMALLINT:
                return pax_fixed_column_type::UINT16;
            case logical_type::INTEGER:
                return pax_fixed_column_type::INT32;
            case logical_type::UINTEGER:
                return pax_fixed_column_type::UINT32;
            case logical_type::BIGINT:
                return pax_fixed_column_type::INT64;
            case logical_type::UBIGINT:
                return pax_fixed_column_type::UINT64;
            default:
                throw std::logic_error("unsupported logical type for pax_fixed column");
        }
    }

    uint64_t pax_fixed_validity_payload_size(uint64_t tuple_count) {
        return components::vector::validity_mask_t::validity_mask_size(tuple_count);
    }

    bool is_supported_pax_fixed_layout_version(uint16_t version) { return version == 1 || version == 2; }

    bool is_supported_pax_generic_layout_version(uint16_t version) { return version == 1 || version == 2; }

    void write_pax_fixed_slice(components::table::column_data_t& column,
                               uint64_t row_group_start,
                               uint32_t row_offset,
                               uint32_t tuple_count,
                               uint32_t column_index,
                               components::table::storage::partial_block_manager_t& partial_block_manager,
                               std::vector<components::table::storage::data_pointer_t>& columnar_pointers,
                               components::table::storage::pax_fixed_page_t& page) {
        components::vector::vector_t slice(column.resource(), column.type(), tuple_count);
        column.scan_committed_range(row_group_start, row_offset, tuple_count, slice);
        slice.flatten(tuple_count);
        for (uint32_t i = 0; i < tuple_count; i++) {
            if (!column.check_validity(static_cast<int64_t>(row_group_start + row_offset + i))) {
                slice.validity().set(i, false);
            }
        }

        auto slice_size = static_cast<uint64_t>(tuple_count) * column.type().size();
        auto allocation = partial_block_manager.get_block_allocation(slice_size);
        if (slice_size > 0) {
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 slice.data(),
                                                 slice_size);
        }

        components::table::storage::data_pointer_t pointer;
        pointer.row_start = row_group_start + row_offset;
        pointer.tuple_count = tuple_count;
        pointer.block_pointer =
            components::table::storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
        pointer.compression = components::table::compression::compression_type::UNCOMPRESSED;
        pointer.segment_size = slice_size;

        columnar_pointers.push_back(pointer);

        components::table::storage::pax_fixed_slice_t slice_desc;
        slice_desc.column_index = column_index;
        slice_desc.column_type = to_pax_fixed_column_type(column.type());
        slice_desc.data_pointer = pointer;

        const auto valid_count = slice.validity().count_valid(tuple_count);
        if (valid_count == tuple_count) {
            slice_desc.validity_kind = components::table::storage::pax_fixed_validity_kind::ALL_VALID;
        } else if (valid_count == 0) {
            slice_desc.validity_kind = components::table::storage::pax_fixed_validity_kind::ALL_INVALID;
        } else {
            const auto validity_size = pax_fixed_validity_payload_size(tuple_count);
            auto validity_allocation = partial_block_manager.get_block_allocation(validity_size);
            partial_block_manager.write_to_block(validity_allocation.block_id,
                                                 validity_allocation.offset_in_block,
                                                 slice.validity().data(),
                                                 validity_size);

            components::table::storage::data_pointer_t validity_pointer;
            validity_pointer.row_start = row_group_start + row_offset;
            validity_pointer.tuple_count = tuple_count;
            validity_pointer.block_pointer = components::table::storage::block_pointer_t(validity_allocation.block_id,
                                                                                         validity_allocation.offset_in_block);
            validity_pointer.compression =
                components::table::compression::compression_type::VALIDITY_UNCOMPRESSED;
            validity_pointer.segment_size = validity_size;

            slice_desc.validity_kind = components::table::storage::pax_fixed_validity_kind::BITMASK;
            slice_desc.validity_data_pointer = validity_pointer;
        }

        page.slices.push_back(std::move(slice_desc));
    }

    bool is_pax_generic_string_type(const components::types::complex_logical_type& type) {
        return type.type() == components::types::logical_type::STRING_LITERAL;
    }

    bool is_pax_generic_struct_type(const components::types::complex_logical_type& type) {
        return type.type() == components::types::logical_type::STRUCT;
    }

    bool is_pax_generic_fixed_plain_type(const components::types::complex_logical_type& type) {
        using components::types::physical_type;

        switch (type.to_physical_type()) {
            case physical_type::BOOL:
            case physical_type::INT8:
            case physical_type::INT16:
            case physical_type::INT32:
            case physical_type::INT64:
            case physical_type::INT128:
            case physical_type::UINT8:
            case physical_type::UINT16:
            case physical_type::UINT32:
            case physical_type::UINT64:
            case physical_type::UINT128:
            case physical_type::FLOAT:
            case physical_type::DOUBLE:
                return true;
            default:
                return false;
        }
    }

    bool is_supported_pax_generic_column_type(const components::types::complex_logical_type& type) {
        if (is_pax_generic_string_type(type) || is_pax_generic_fixed_plain_type(type)) {
            return true;
        }
        if (!is_pax_generic_struct_type(type)) {
            return false;
        }
        for (const auto& child_type : type.child_types()) {
            if (!is_supported_pax_generic_column_type(child_type)) {
                return false;
            }
        }
        return true;
    }

    uint64_t pax_string_block_limit(uint64_t block_size) {
        return std::min((block_size / 4) / 8 * 8, PAX_STRING_DEFAULT_BLOCK_LIMIT);
    }

    const std::vector<uint16_t>& empty_pax_generic_field_path() {
        static const std::vector<uint16_t> EMPTY_FIELD_PATH;
        return EMPTY_FIELD_PATH;
    }

    void append_unique_block_id(std::vector<uint32_t>& block_ids, uint32_t block_id) {
        if (std::find(block_ids.begin(), block_ids.end(), block_id) == block_ids.end()) {
            block_ids.push_back(block_id);
        }
    }

    struct pax_generic_string_page_write_result_t {
        components::table::storage::data_pointer_t main_pointer;
        std::vector<uint32_t> extra_block_ids;
        components::table::storage::pax_generic_codec_kind validity_codec{
            components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID};
        std::optional<components::table::storage::data_pointer_t> validity_pointer;
    };

    struct pax_generic_fixed_page_write_result_t {
        components::table::storage::data_pointer_t main_pointer;
        components::table::storage::pax_generic_codec_kind validity_codec{
            components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID};
        std::optional<components::table::storage::data_pointer_t> validity_pointer;
    };

    struct pax_generic_validity_write_result_t {
        components::table::storage::pax_generic_codec_kind validity_codec{
            components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID};
        std::optional<components::table::storage::data_pointer_t> validity_pointer;
    };

    pax_generic_string_page_write_result_t
    write_pax_generic_string_page(components::table::column_data_t& column,
                                  uint64_t row_group_start,
                                  uint32_t row_offset,
                                  uint32_t tuple_count,
                                  components::table::storage::partial_block_manager_t& partial_block_manager) {
        using components::table::compression::compression_type;
        using components::table::storage::block_pointer_t;
        using components::table::storage::data_pointer_t;
        using components::table::storage::pax_generic_codec_kind;

        components::vector::vector_t slice(column.resource(), column.type(), tuple_count);
        column.scan_committed_range(row_group_start, row_offset, tuple_count, slice);
        slice.flatten(tuple_count);
        for (uint32_t i = 0; i < tuple_count; i++) {
            if (!column.check_validity(static_cast<int64_t>(row_group_start + row_offset + i))) {
                slice.validity().set(i, false);
            }
        }

        auto* values = slice.data<std::string_view>();
        const auto block_size = column.block_manager().block_size();
        const auto offset_bytes = static_cast<uint64_t>(tuple_count) * sizeof(int32_t);
        const auto header_and_offsets = static_cast<uint64_t>(PAX_STRING_DICTIONARY_HEADER_SIZE) + offset_bytes;
        if (header_and_offsets > block_size) {
            throw std::logic_error("pax_generic string page header exceeds block size");
        }

        std::vector<int32_t> offsets(tuple_count, 0);
        std::vector<std::vector<std::byte>> dictionary_entries;
        dictionary_entries.reserve(tuple_count);
        uint64_t total_dictionary_bytes = 0;
        std::vector<uint32_t> extra_block_ids;

        int32_t current_dict_size = 0;
        uint64_t remaining = block_size - header_and_offsets;
        for (uint32_t i = 0; i < tuple_count; i++) {
            if (!slice.validity().row_is_valid(i)) {
                offsets[i] = i == 0 ? 0 : offsets[i - 1];
                dictionary_entries.emplace_back();
                continue;
            }

            const auto value = values[i];
            bool use_overflow = static_cast<uint64_t>(value.size()) >= pax_string_block_limit(block_size);
            uint64_t required_space = use_overflow ? PAX_STRING_BIG_MARKER_SIZE : value.size();
            if (required_space > remaining) {
                use_overflow = true;
                required_space = PAX_STRING_BIG_MARKER_SIZE;
            }
            if (required_space > remaining) {
                throw std::logic_error("pax_generic string page overflow marker exceeds remaining page space");
            }

            if (use_overflow) {
                const auto overflow_size = static_cast<uint64_t>(sizeof(uint32_t) + value.size());
                auto overflow_allocation = partial_block_manager.get_block_allocation(overflow_size);
                std::vector<std::byte> overflow_payload(overflow_size);
                auto string_size = static_cast<uint32_t>(value.size());
                std::memcpy(overflow_payload.data(), &string_size, sizeof(uint32_t));
                if (!value.empty()) {
                    std::memcpy(overflow_payload.data() + sizeof(uint32_t), value.data(), value.size());
                }
                partial_block_manager.write_to_block(overflow_allocation.block_id,
                                                     overflow_allocation.offset_in_block,
                                                     overflow_payload.data(),
                                                     overflow_payload.size());

                auto marker_offset = static_cast<int32_t>(overflow_allocation.offset_in_block);
                auto marker_block_id = static_cast<uint32_t>(overflow_allocation.block_id);
                std::vector<std::byte> marker(PAX_STRING_BIG_MARKER_SIZE);
                std::memcpy(marker.data(), &marker_block_id, sizeof(uint32_t));
                std::memcpy(marker.data() + sizeof(uint32_t), &marker_offset, sizeof(int32_t));
                total_dictionary_bytes += marker.size();
                dictionary_entries.push_back(std::move(marker));
                append_unique_block_id(extra_block_ids, marker_block_id);
                current_dict_size += static_cast<int32_t>(PAX_STRING_BIG_MARKER_SIZE);
                offsets[i] = -current_dict_size;
            } else {
                std::vector<std::byte> inline_value(value.size());
                if (!value.empty()) {
                    std::memcpy(inline_value.data(), value.data(), value.size());
                }
                total_dictionary_bytes += inline_value.size();
                dictionary_entries.push_back(std::move(inline_value));
                current_dict_size += static_cast<int32_t>(value.size());
                offsets[i] = current_dict_size;
            }

            remaining -= required_space;
        }

        std::vector<std::byte> dictionary_bytes;
        dictionary_bytes.reserve(total_dictionary_bytes);
        for (auto it = dictionary_entries.rbegin(); it != dictionary_entries.rend(); ++it) {
            dictionary_bytes.insert(dictionary_bytes.end(), it->begin(), it->end());
        }

        std::vector<std::byte> payload(header_and_offsets + dictionary_bytes.size(), std::byte{0});
        auto dict_size = static_cast<uint32_t>(dictionary_bytes.size());
        auto dict_end = static_cast<uint32_t>(payload.size());
        std::memcpy(payload.data(), &dict_size, sizeof(uint32_t));
        std::memcpy(payload.data() + sizeof(uint32_t), &dict_end, sizeof(uint32_t));
        if (tuple_count > 0) {
            std::memcpy(payload.data() + PAX_STRING_DICTIONARY_HEADER_SIZE, offsets.data(), offset_bytes);
        }
        if (!dictionary_bytes.empty()) {
            std::memcpy(payload.data() + header_and_offsets, dictionary_bytes.data(), dictionary_bytes.size());
        }

        auto allocation = partial_block_manager.get_block_allocation(payload.size());
        if (!payload.empty()) {
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 payload.data(),
                                                 payload.size());
        }

        pax_generic_string_page_write_result_t result;
        result.main_pointer.row_start = row_group_start + row_offset;
        result.main_pointer.tuple_count = tuple_count;
        result.main_pointer.block_pointer = block_pointer_t(allocation.block_id, allocation.offset_in_block);
        result.main_pointer.compression = compression_type::UNCOMPRESSED;
        result.main_pointer.segment_size = payload.size();
        result.extra_block_ids = std::move(extra_block_ids);

        const auto valid_count = slice.validity().count_valid(tuple_count);
        if (valid_count == tuple_count) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_VALID;
        } else if (valid_count == 0) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_INVALID;
        } else {
            const auto validity_size = pax_fixed_validity_payload_size(tuple_count);
            auto validity_allocation = partial_block_manager.get_block_allocation(validity_size);
            partial_block_manager.write_to_block(validity_allocation.block_id,
                                                 validity_allocation.offset_in_block,
                                                 slice.validity().data(),
                                                 validity_size);

            data_pointer_t validity_pointer;
            validity_pointer.row_start = row_group_start + row_offset;
            validity_pointer.tuple_count = tuple_count;
            validity_pointer.block_pointer = block_pointer_t(validity_allocation.block_id,
                                                             validity_allocation.offset_in_block);
            validity_pointer.compression = compression_type::VALIDITY_UNCOMPRESSED;
            validity_pointer.segment_size = validity_size;

            result.validity_codec = pax_generic_codec_kind::VALIDITY_BITMASK;
            result.validity_pointer = std::move(validity_pointer);
        }

        return result;
    }

    pax_generic_fixed_page_write_result_t
    write_pax_generic_fixed_page(components::table::column_data_t& column,
                                 uint64_t row_group_start,
                                 uint32_t row_offset,
                                 uint32_t tuple_count,
                                 components::table::storage::partial_block_manager_t& partial_block_manager) {
        using components::table::compression::compression_type;
        using components::table::storage::block_pointer_t;
        using components::table::storage::data_pointer_t;
        using components::table::storage::pax_generic_codec_kind;

        components::vector::vector_t slice(column.resource(), column.type(), tuple_count);
        column.scan_committed_range(row_group_start, row_offset, tuple_count, slice);
        slice.flatten(tuple_count);
        for (uint32_t i = 0; i < tuple_count; i++) {
            if (!column.check_validity(static_cast<int64_t>(row_group_start + row_offset + i))) {
                slice.validity().set(i, false);
            }
        }

        const auto slice_size = static_cast<uint64_t>(tuple_count) * column.type().size();
        auto allocation = partial_block_manager.get_block_allocation(slice_size);
        if (slice_size > 0) {
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 slice.data(),
                                                 slice_size);
        }

        pax_generic_fixed_page_write_result_t result;
        result.main_pointer.row_start = row_group_start + row_offset;
        result.main_pointer.tuple_count = tuple_count;
        result.main_pointer.block_pointer = block_pointer_t(allocation.block_id, allocation.offset_in_block);
        result.main_pointer.compression = compression_type::UNCOMPRESSED;
        result.main_pointer.segment_size = slice_size;

        const auto valid_count = slice.validity().count_valid(tuple_count);
        if (valid_count == tuple_count) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_VALID;
        } else if (valid_count == 0) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_INVALID;
        } else {
            const auto validity_size = pax_fixed_validity_payload_size(tuple_count);
            auto validity_allocation = partial_block_manager.get_block_allocation(validity_size);
            partial_block_manager.write_to_block(validity_allocation.block_id,
                                                 validity_allocation.offset_in_block,
                                                 slice.validity().data(),
                                                 validity_size);

            data_pointer_t validity_pointer;
            validity_pointer.row_start = row_group_start + row_offset;
            validity_pointer.tuple_count = tuple_count;
            validity_pointer.block_pointer =
                block_pointer_t(validity_allocation.block_id, validity_allocation.offset_in_block);
            validity_pointer.compression = compression_type::VALIDITY_UNCOMPRESSED;
            validity_pointer.segment_size = validity_size;

            result.validity_codec = pax_generic_codec_kind::VALIDITY_BITMASK;
            result.validity_pointer = std::move(validity_pointer);
        }

        return result;
    }

    pax_generic_validity_write_result_t
    write_pax_generic_validity_page(components::table::column_data_t& column,
                                    uint64_t row_group_start,
                                    uint32_t row_offset,
                                    uint32_t tuple_count,
                                    components::table::storage::partial_block_manager_t& partial_block_manager) {
        using components::table::compression::compression_type;
        using components::table::storage::block_pointer_t;
        using components::table::storage::data_pointer_t;
        using components::table::storage::pax_generic_codec_kind;

        components::vector::validity_mask_t validity(column.resource(), tuple_count);
        uint64_t valid_count = 0;
        for (uint32_t i = 0; i < tuple_count; i++) {
            const auto is_valid = column.check_validity(static_cast<int64_t>(row_group_start + row_offset + i));
            validity.set(i, is_valid);
            valid_count += static_cast<uint64_t>(is_valid);
        }

        pax_generic_validity_write_result_t result;
        if (valid_count == tuple_count) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_VALID;
        } else if (valid_count == 0) {
            result.validity_codec = pax_generic_codec_kind::VALIDITY_ALL_INVALID;
        } else {
            const auto validity_size = pax_fixed_validity_payload_size(tuple_count);
            auto allocation = partial_block_manager.get_block_allocation(validity_size);
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 validity.data(),
                                                 validity_size);

            data_pointer_t validity_pointer;
            validity_pointer.row_start = row_group_start + row_offset;
            validity_pointer.tuple_count = tuple_count;
            validity_pointer.block_pointer = block_pointer_t(allocation.block_id, allocation.offset_in_block);
            validity_pointer.compression = compression_type::VALIDITY_UNCOMPRESSED;
            validity_pointer.segment_size = validity_size;

            result.validity_codec = pax_generic_codec_kind::VALIDITY_BITMASK;
            result.validity_pointer = std::move(validity_pointer);
        }

        return result;
    }

    void append_pax_generic_validity_slice(components::table::storage::pax_generic_page_t& page,
                                           uint32_t root_column_index,
                                           const std::vector<uint16_t>& field_path,
                                           const pax_generic_validity_write_result_t& validity_result) {
        components::table::storage::pax_generic_slice_t validity_slice;
        validity_slice.column_index = root_column_index;
        validity_slice.slice_kind = components::table::storage::pax_generic_slice_kind::VALIDITY;
        validity_slice.codec_kind = validity_result.validity_codec;
        validity_slice.field_path = field_path;
        if (validity_result.validity_pointer.has_value()) {
            validity_slice.payload = components::table::storage::pax_block_payload_t{*validity_result.validity_pointer,
                                                                                     {}};
        }
        page.slices.push_back(std::move(validity_slice));
    }

    void write_pax_generic_column_page(components::table::column_data_t& column,
                                       uint32_t root_column_index,
                                       const std::vector<uint16_t>& field_path,
                                       uint64_t row_group_start,
                                       uint32_t row_offset,
                                       uint32_t tuple_count,
                                       components::table::storage::partial_block_manager_t& partial_block_manager,
                                       components::table::storage::pax_generic_page_t& page) {
        using components::table::storage::pax_block_payload_t;
        using components::table::storage::pax_generic_codec_kind;
        using components::table::storage::pax_generic_slice_kind;
        using components::types::logical_type;

        if (is_pax_generic_struct_type(column.type())) {
            auto validity_result =
                write_pax_generic_validity_page(column, row_group_start, row_offset, tuple_count, partial_block_manager);
            append_pax_generic_validity_slice(page, root_column_index, field_path, validity_result);

            auto& struct_column = dynamic_cast<components::table::struct_column_data_t&>(column);
            for (uint16_t child_index = 0; child_index < struct_column.sub_columns.size(); child_index++) {
                auto child_path = field_path;
                child_path.push_back(child_index);
                write_pax_generic_column_page(*struct_column.sub_columns[child_index],
                                              root_column_index,
                                              child_path,
                                              row_group_start,
                                              row_offset,
                                              tuple_count,
                                              partial_block_manager,
                                              page);
            }
            return;
        }

        if (is_pax_generic_string_type(column.type())) {
            auto page_result = write_pax_generic_string_page(column,
                                                             row_group_start,
                                                             row_offset,
                                                             tuple_count,
                                                             partial_block_manager);

            components::table::storage::pax_generic_slice_t value_slice;
            value_slice.column_index = root_column_index;
            value_slice.slice_kind = pax_generic_slice_kind::STRING_VALUES;
            value_slice.codec_kind = pax_generic_codec_kind::STRING_SEGMENT;
            value_slice.field_path = field_path;
            value_slice.payload = pax_block_payload_t{page_result.main_pointer, page_result.extra_block_ids};
            page.slices.push_back(std::move(value_slice));

            append_pax_generic_validity_slice(page,
                                              root_column_index,
                                              field_path,
                                              {page_result.validity_codec, page_result.validity_pointer});
            return;
        }

        if (is_pax_generic_fixed_plain_type(column.type())) {
            auto page_result = write_pax_generic_fixed_page(column,
                                                            row_group_start,
                                                            row_offset,
                                                            tuple_count,
                                                            partial_block_manager);

            components::table::storage::pax_generic_slice_t value_slice;
            value_slice.column_index = root_column_index;
            value_slice.slice_kind = pax_generic_slice_kind::FIXED_VALUES;
            value_slice.codec_kind = pax_generic_codec_kind::FIXED_PLAIN;
            value_slice.field_path = field_path;
            value_slice.fixed_logical_type = column.type().type();
            value_slice.payload = pax_block_payload_t{page_result.main_pointer, {}};
            page.slices.push_back(std::move(value_slice));

            append_pax_generic_validity_slice(page,
                                              root_column_index,
                                              field_path,
                                              {page_result.validity_codec, page_result.validity_pointer});
            return;
        }

        throw std::logic_error("unsupported column type for pax_generic page writer");
    }

    bool is_supported_pax_projected_filter_compare(components::expressions::compare_type type) {
        using components::expressions::compare_type;

        switch (type) {
            case compare_type::eq:
            case compare_type::ne:
            case compare_type::gt:
            case compare_type::gte:
            case compare_type::lt:
            case compare_type::lte:
                return true;
            default:
                return false;
        }
    }

    const components::table::storage::pax_fixed_slice_t*
    find_pax_fixed_slice(const components::table::storage::pax_fixed_page_t& page, uint32_t column_index) {
        for (const auto& slice : page.slices) {
            if (slice.column_index == column_index) {
                return &slice;
            }
        }
        return nullptr;
    }

    const components::table::storage::pax_generic_slice_t*
    find_pax_generic_slice(const components::table::storage::pax_generic_page_t& page,
                           uint32_t column_index,
                           components::table::storage::pax_generic_slice_kind slice_kind,
                           const std::vector<uint16_t>& field_path = empty_pax_generic_field_path()) {
        for (const auto& slice : page.slices) {
            if (slice.column_index == column_index && slice.slice_kind == slice_kind &&
                slice.field_path == field_path) {
                return &slice;
            }
        }
        return nullptr;
    }

    uint64_t pax_fixed_layout_tuple_count(const components::table::storage::pax_fixed_row_group_layout_t& layout) {
        uint64_t total = 0;
        for (const auto& page : layout.pages) {
            total += page.tuple_count;
        }
        return total;
    }

    uint64_t pax_generic_layout_tuple_count(const components::table::storage::pax_generic_row_group_layout_t& layout) {
        uint64_t total = 0;
        for (const auto& page : layout.pages) {
            total += page.tuple_count;
        }
        return total;
    }

    bool validate_pax_fixed_slice(const components::table::storage::pax_fixed_slice_t& slice,
                                  const components::table::storage::pax_fixed_column_type expected_type,
                                  uint32_t tuple_count,
                                  uint64_t type_size,
                                  uint16_t layout_version) {
        if (slice.column_type != expected_type) {
            return false;
        }
        if (slice.data_pointer.compression != components::table::compression::compression_type::UNCOMPRESSED) {
            return false;
        }
        if (slice.data_pointer.tuple_count != tuple_count ||
            slice.data_pointer.segment_size != static_cast<uint64_t>(tuple_count) * type_size) {
            return false;
        }
        if (layout_version < 2) {
            return true;
        }

        switch (slice.validity_kind) {
            case components::table::storage::pax_fixed_validity_kind::ALL_VALID:
            case components::table::storage::pax_fixed_validity_kind::ALL_INVALID:
                return !slice.validity_data_pointer.has_value();
            case components::table::storage::pax_fixed_validity_kind::BITMASK:
                if (!slice.validity_data_pointer.has_value()) {
                    return false;
                }
                return slice.validity_data_pointer->compression ==
                           components::table::compression::compression_type::VALIDITY_UNCOMPRESSED &&
                       slice.validity_data_pointer->tuple_count == tuple_count &&
                       slice.validity_data_pointer->segment_size == pax_fixed_validity_payload_size(tuple_count);
            case components::table::storage::pax_fixed_validity_kind::RLE:
            default:
                return false;
        }
    }

    bool validate_pax_generic_string_slice(const components::table::storage::pax_generic_slice_t& slice,
                                           uint32_t tuple_count) {
        if (slice.slice_kind != components::table::storage::pax_generic_slice_kind::STRING_VALUES ||
            slice.codec_kind != components::table::storage::pax_generic_codec_kind::STRING_SEGMENT ||
            !slice.payload.has_value()) {
            return false;
        }

        const auto& pointer = slice.payload->main_pointer;
        return pointer.compression == components::table::compression::compression_type::UNCOMPRESSED &&
               pointer.tuple_count == tuple_count &&
               pointer.segment_size >=
                   static_cast<uint64_t>(PAX_STRING_DICTIONARY_HEADER_SIZE) +
                       static_cast<uint64_t>(tuple_count) * sizeof(int32_t);
    }

    bool validate_pax_generic_validity_slice(const components::table::storage::pax_generic_slice_t& slice,
                                             uint32_t tuple_count) {
        if (slice.slice_kind != components::table::storage::pax_generic_slice_kind::VALIDITY) {
            return false;
        }

        switch (slice.codec_kind) {
            case components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID:
            case components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                return !slice.payload.has_value();
            case components::table::storage::pax_generic_codec_kind::VALIDITY_BITMASK:
                if (!slice.payload.has_value()) {
                    return false;
                }
                return slice.payload->extra_block_ids.empty() &&
                       slice.payload->main_pointer.compression ==
                           components::table::compression::compression_type::VALIDITY_UNCOMPRESSED &&
                       slice.payload->main_pointer.tuple_count == tuple_count &&
                       slice.payload->main_pointer.segment_size == pax_fixed_validity_payload_size(tuple_count);
            case components::table::storage::pax_generic_codec_kind::STRING_SEGMENT:
            case components::table::storage::pax_generic_codec_kind::FIXED_PLAIN:
            default:
                return false;
        }
    }

    using pax_fixed_block_cache_t =
        std::unordered_map<uint64_t, components::table::storage::buffer_handle_t>;

    components::table::storage::buffer_handle_t&
    get_or_pin_pax_fixed_block(components::table::row_group_t& row_group,
                               uint64_t block_id,
                               pax_fixed_block_cache_t& block_cache) {
        auto entry = block_cache.find(block_id);
        if (entry != block_cache.end()) {
            return entry->second;
        }

        auto block = row_group.block_manager().register_block(block_id);
        auto handle = row_group.block_manager().buffer_manager.pin(block);
        handle.set_ownership(block);
        return block_cache.emplace(block_id, std::move(handle)).first->second;
    }

    using pax_generic_block_cache_t =
        std::unordered_map<uint64_t, components::table::storage::buffer_handle_t>;

    components::table::storage::buffer_handle_t&
    get_or_pin_pax_generic_block(components::table::row_group_t& row_group,
                                 uint64_t block_id,
                                 pax_generic_block_cache_t& block_cache) {
        auto entry = block_cache.find(block_id);
        if (entry != block_cache.end()) {
            return entry->second;
        }

        auto block = row_group.block_manager().register_block(block_id);
        auto handle = row_group.block_manager().buffer_manager.pin(block);
        handle.set_ownership(block);
        return block_cache.emplace(block_id, std::move(handle)).first->second;
    }

    void mark_vector_range_valid(components::vector::vector_t& result, uint64_t offset, uint64_t count) {
        auto& validity = result.validity();
        for (uint64_t i = 0; i < count; i++) {
            validity.set(offset + i, true);
        }
    }

    void advance_pax_fixed_scan_state(components::table::column_scan_state& state, uint64_t count) {
        state.row_index += static_cast<int64_t>(count);
        state.internal_index = state.row_index;
        for (auto& child_state : state.child_states) {
            advance_pax_fixed_scan_state(child_state, count);
        }
    }

    uint64_t current_pax_fixed_row_offset(const components::table::row_group_t& row_group,
                                          const components::table::collection_scan_state& state) {
        for (const auto& column_state : state.column_scans) {
            if (column_state.current) {
                return static_cast<uint64_t>(column_state.row_index - row_group.start);
            }
        }
        return state.vector_index * components::vector::DEFAULT_VECTOR_CAPACITY;
    }

    bool apply_pax_fixed_validity_window(components::table::row_group_t& row_group,
                                         const components::table::storage::pax_fixed_slice_t& slice,
                                         uint64_t page_row_offset,
                                         uint64_t copy_count,
                                         components::vector::vector_t& result,
                                         uint64_t result_offset,
                                         pax_fixed_block_cache_t& block_cache) {
        using components::table::storage::pax_fixed_validity_kind;

        switch (slice.validity_kind) {
            case pax_fixed_validity_kind::ALL_VALID:
                return true;
            case pax_fixed_validity_kind::ALL_INVALID:
                for (uint64_t i = 0; i < copy_count; i++) {
                    result.validity().set(result_offset + i, false);
                }
                return true;
            case pax_fixed_validity_kind::BITMASK: {
                if (!slice.validity_data_pointer.has_value()) {
                    return false;
                }
                auto& validity_pointer = *slice.validity_data_pointer;
                auto& block_handle =
                    get_or_pin_pax_fixed_block(row_group, validity_pointer.block_pointer.block_id, block_cache);
                auto* validity_data =
                    reinterpret_cast<uint64_t*>(block_handle.ptr() + validity_pointer.block_pointer.offset);
                components::vector::validity_mask_t source_mask(validity_data);
                result.validity().slice_in_place(source_mask, result_offset, page_row_offset, copy_count);
                return true;
            }
            case pax_fixed_validity_kind::RLE:
            default:
                return false;
        }
    }

    bool decode_pax_fixed_window(components::table::row_group_t& row_group,
                                 const components::table::storage::pax_fixed_row_group_layout_t& layout,
                                 uint32_t column_index,
                                 const components::types::complex_logical_type& type,
                                 uint64_t window_row_offset,
                                 uint64_t window_count,
                                 components::vector::vector_t& result,
                                 uint64_t result_offset,
                                 pax_fixed_block_cache_t& block_cache) {
        if (window_count == 0) {
            return true;
        }

        const auto type_size = static_cast<uint64_t>(type.size());
        const auto expected_column_type = to_pax_fixed_column_type(type);
        const auto window_end = window_row_offset + window_count;

        result.set_vector_type(components::vector::vector_type::FLAT);

        uint64_t copied_rows = 0;
        for (const auto& page : layout.pages) {
            const auto page_start = static_cast<uint64_t>(page.row_offset_in_group);
            const auto page_end = page_start + static_cast<uint64_t>(page.tuple_count);
            if (page_end <= window_row_offset || page_start >= window_end) {
                continue;
            }

            const auto* slice = find_pax_fixed_slice(page, column_index);
            if (!slice || slice->column_type != expected_column_type) {
                return false;
            }
            if (slice->data_pointer.compression !=
                components::table::compression::compression_type::UNCOMPRESSED) {
                return false;
            }
            if (slice->data_pointer.tuple_count != page.tuple_count ||
                slice->data_pointer.segment_size != static_cast<uint64_t>(page.tuple_count) * type_size) {
                return false;
            }

            const auto overlap_start = std::max(window_row_offset, page_start);
            const auto overlap_end = std::min(window_end, page_end);
            const auto copy_count = overlap_end - overlap_start;
            const auto page_row_offset = overlap_start - page_start;
            const auto window_result_offset = result_offset + (overlap_start - window_row_offset);
            const auto byte_offset = page_row_offset * type_size;
            const auto byte_count = copy_count * type_size;

            auto& block_handle =
                get_or_pin_pax_fixed_block(row_group, slice->data_pointer.block_pointer.block_id, block_cache);
            auto* source_ptr =
                block_handle.ptr() + slice->data_pointer.block_pointer.offset + static_cast<uint64_t>(byte_offset);
            auto* target_ptr = result.data() + window_result_offset * type_size;
            std::memcpy(target_ptr, source_ptr, byte_count);
            if (!apply_pax_fixed_validity_window(row_group,
                                                 *slice,
                                                 page_row_offset,
                                                 copy_count,
                                                 result,
                                                 window_result_offset,
                                                 block_cache)) {
                return false;
            }
            copied_rows += copy_count;
        }

        return copied_rows == window_count;
    }

    template<typename T>
    uint64_t apply_pax_fixed_constant_filter_typed(const components::vector::vector_t& values,
                                                   const components::table::constant_filter_t& filter,
                                                   uint64_t count,
                                                   components::vector::indexing_vector_t& indexing) {
        const auto* data = values.data<T>();
        const auto& validity = values.validity();
        uint64_t approved_count = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (!validity.row_is_valid(i)) {
                continue;
            }
            if (filter.compare(data[i])) {
                indexing.set_index(approved_count++, i);
            }
        }
        return approved_count;
    }

    uint64_t apply_pax_fixed_constant_filter(const components::vector::vector_t& values,
                                             const components::table::constant_filter_t& filter,
                                             uint64_t count,
                                             components::vector::indexing_vector_t& indexing) {
        using components::types::physical_type;

        switch (values.type().to_physical_type()) {
            case physical_type::INT8:
                return apply_pax_fixed_constant_filter_typed<int8_t>(values, filter, count, indexing);
            case physical_type::INT16:
                return apply_pax_fixed_constant_filter_typed<int16_t>(values, filter, count, indexing);
            case physical_type::INT32:
                return apply_pax_fixed_constant_filter_typed<int32_t>(values, filter, count, indexing);
            case physical_type::INT64:
                return apply_pax_fixed_constant_filter_typed<int64_t>(values, filter, count, indexing);
            case physical_type::UINT8:
                return apply_pax_fixed_constant_filter_typed<uint8_t>(values, filter, count, indexing);
            case physical_type::UINT16:
                return apply_pax_fixed_constant_filter_typed<uint16_t>(values, filter, count, indexing);
            case physical_type::UINT32:
                return apply_pax_fixed_constant_filter_typed<uint32_t>(values, filter, count, indexing);
            case physical_type::UINT64:
                return apply_pax_fixed_constant_filter_typed<uint64_t>(values, filter, count, indexing);
            default:
                return 0;
        }
    }

    std::string_view materialize_pax_generic_string(components::vector::vector_t& result,
                                                    uint64_t row_index,
                                                    const std::string_view& value) {
        auto* auxiliary = static_cast<components::vector::string_vector_buffer_t*>(result.auxiliary().get());
        if (!auxiliary) {
            throw std::logic_error("missing string auxiliary buffer for pax_generic decode");
        }

        auto* result_data = result.data<std::string_view>();
        if (value.empty()) {
            auto* empty = auxiliary->empty_string(0);
            result_data[row_index] = std::string_view(reinterpret_cast<char*>(empty), 0);
        } else {
            auto* stored = auxiliary->insert(value);
            result_data[row_index] = std::string_view(reinterpret_cast<char*>(stored), value.size());
        }
        return result_data[row_index];
    }

    bool decode_pax_generic_string_window(components::table::row_group_t& row_group,
                                          const components::table::storage::pax_generic_row_group_layout_t& layout,
                                          uint32_t column_index,
                                          uint64_t window_row_offset,
                                          uint64_t window_count,
                                          components::vector::vector_t& result,
                                          uint64_t result_offset,
                                          pax_generic_block_cache_t& block_cache) {
        using components::table::storage::pax_generic_codec_kind;
        using components::table::storage::pax_generic_slice_kind;

        if (window_count == 0) {
            return true;
        }
        if (result.type().to_physical_type() != components::types::physical_type::STRING) {
            return false;
        }

        result.set_vector_type(components::vector::vector_type::FLAT);
        mark_vector_range_valid(result, result_offset, window_count);

        const auto window_end = window_row_offset + window_count;
        uint64_t copied_rows = 0;
        for (const auto& page : layout.pages) {
            const auto page_start = static_cast<uint64_t>(page.row_offset_in_group);
            const auto page_end = page_start + static_cast<uint64_t>(page.tuple_count);
            if (page_end <= window_row_offset || page_start >= window_end) {
                continue;
            }

            const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::STRING_VALUES);
            const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
            if (!value_slice || !validity_slice || !validate_pax_generic_string_slice(*value_slice, page.tuple_count) ||
                !validate_pax_generic_validity_slice(*validity_slice, page.tuple_count)) {
                return false;
            }

            auto& value_pointer = value_slice->payload->main_pointer;
            auto& block_handle =
                get_or_pin_pax_generic_block(row_group, value_pointer.block_pointer.block_id, block_cache);
            auto* base_ptr = block_handle.ptr() + value_pointer.block_pointer.offset;
            uint32_t dict_size = 0;
            uint32_t dict_end = 0;
            std::memcpy(&dict_size, base_ptr, sizeof(uint32_t));
            std::memcpy(&dict_end, base_ptr + sizeof(uint32_t), sizeof(uint32_t));

            const auto offset_bytes = static_cast<uint64_t>(page.tuple_count) * sizeof(int32_t);
            const auto minimum_payload_size =
                static_cast<uint64_t>(PAX_STRING_DICTIONARY_HEADER_SIZE) + offset_bytes;
            if (value_pointer.segment_size < minimum_payload_size || dict_end > value_pointer.segment_size ||
                dict_size > dict_end || dict_end < minimum_payload_size) {
                return false;
            }

            std::optional<components::vector::validity_mask_t> page_validity_mask;
            if (validity_slice->codec_kind == pax_generic_codec_kind::VALIDITY_BITMASK) {
                auto& validity_pointer = validity_slice->payload->main_pointer;
                auto& validity_handle =
                    get_or_pin_pax_generic_block(row_group, validity_pointer.block_pointer.block_id, block_cache);
                auto* validity_data =
                    reinterpret_cast<uint64_t*>(validity_handle.ptr() + validity_pointer.block_pointer.offset);
                page_validity_mask.emplace(validity_data);
            }

            auto* page_offsets = reinterpret_cast<int32_t*>(base_ptr + PAX_STRING_DICTIONARY_HEADER_SIZE);
            const auto overlap_start = std::max(window_row_offset, page_start);
            const auto overlap_end = std::min(window_end, page_end);
            const auto copy_count = overlap_end - overlap_start;
            const auto page_row_offset = overlap_start - page_start;

            for (uint64_t i = 0; i < copy_count; i++) {
                const auto page_index = static_cast<uint64_t>(page_row_offset + i);
                const auto target_index = result_offset + (overlap_start - window_row_offset) + i;

                bool is_valid = true;
                switch (validity_slice->codec_kind) {
                    case pax_generic_codec_kind::VALIDITY_ALL_VALID:
                        break;
                    case pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                        is_valid = false;
                        break;
                    case pax_generic_codec_kind::VALIDITY_BITMASK:
                        is_valid = page_validity_mask->row_is_valid(page_index);
                        break;
                    case pax_generic_codec_kind::STRING_SEGMENT:
                    default:
                        return false;
                }

                if (!is_valid) {
                    result.validity().set(target_index, false);
                    result.data<std::string_view>()[target_index] = std::string_view(nullptr, 0);
                    continue;
                }

                const auto dict_offset = page_offsets[page_index];
                const auto prev_offset = page_index == 0 ? 0 : page_offsets[page_index - 1];
                const auto abs_dict_offset = static_cast<uint64_t>(std::abs(dict_offset));
                const auto abs_prev_offset = static_cast<uint64_t>(std::abs(prev_offset));
                if (abs_dict_offset < abs_prev_offset || abs_dict_offset > dict_end) {
                    return false;
                }

                const auto string_length =
                    static_cast<uint32_t>(abs_dict_offset - abs_prev_offset);
                if (dict_offset < 0) {
                    if (abs_dict_offset < PAX_STRING_BIG_MARKER_SIZE) {
                        return false;
                    }
                    auto* marker_ptr = base_ptr + dict_end - abs_dict_offset;
                    uint32_t overflow_block_id = 0;
                    int32_t overflow_offset = 0;
                    std::memcpy(&overflow_block_id, marker_ptr, sizeof(uint32_t));
                    std::memcpy(&overflow_offset, marker_ptr + sizeof(uint32_t), sizeof(int32_t));
                    if (overflow_offset < 0 ||
                        std::find(value_slice->payload->extra_block_ids.begin(),
                                  value_slice->payload->extra_block_ids.end(),
                                  overflow_block_id) == value_slice->payload->extra_block_ids.end()) {
                        return false;
                    }

                    auto& overflow_handle =
                        get_or_pin_pax_generic_block(row_group, overflow_block_id, block_cache);
                    auto* overflow_ptr = overflow_handle.ptr() + static_cast<uint64_t>(overflow_offset);
                    uint32_t overflow_length = 0;
                    std::memcpy(&overflow_length, overflow_ptr, sizeof(uint32_t));
                    materialize_pax_generic_string(result,
                                                  target_index,
                                                  std::string_view(reinterpret_cast<char*>(overflow_ptr + sizeof(uint32_t)),
                                                                   overflow_length));
                } else {
                    auto* source_ptr = reinterpret_cast<char*>(base_ptr + dict_end - abs_dict_offset);
                    materialize_pax_generic_string(result,
                                                  target_index,
                                                  std::string_view(source_ptr, string_length));
                }
            }
            copied_rows += copy_count;
        }

        return copied_rows == window_count;
    }

    uint64_t apply_pax_generic_string_constant_filter(const components::vector::vector_t& values,
                                                      const components::table::constant_filter_t& filter,
                                                      uint64_t count,
                                                      components::vector::indexing_vector_t& indexing) {
        const auto* data = values.data<std::string_view>();
        const auto& validity = values.validity();
        uint64_t approved_count = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (!validity.row_is_valid(i)) {
                continue;
            }
            if (filter.compare(data[i])) {
                indexing.set_index(approved_count++, i);
            }
        }
        return approved_count;
    }

} // namespace

namespace components::table {

    row_group_t::row_group_t(collection_t* collection, int64_t start, uint64_t count)
        : segment_base_t(start, count)
        , collection_(collection)
        , allocation_size_(0) {}

    void row_group_t::move_to_collection(collection_t* collection, int64_t new_start) {
        collection_ = collection;
        start = new_start;
        for (auto& column : columns()) {
            column->set_start(new_start);
        }
    }

    std::vector<std::shared_ptr<column_data_t>>& row_group_t::columns() {
        for (uint64_t c = 0; c < get_column_count(); c++) {
            get_column(c);
        }
        return columns_;
    }

    uint64_t row_group_t::get_column_count() const { return columns_.size(); }

    uint64_t row_group_t::row_group_size() const { return collection_->row_group_size(); }

    column_data_t& row_group_t::get_column(const storage_index_t& c) { return get_column(c.primary_index()); }

    column_data_t& row_group_t::get_column(uint64_t c) {
        assert(c < columns_.size());
        if (!is_loaded_) {
            assert(columns_[c]);
            return *columns_[c];
        }
        if (is_loaded_[c]) {
            assert(columns_[c]);
            return *columns_[c];
        }
        std::lock_guard l(row_group_lock_);
        if (columns_[c]) {
            assert(is_loaded_[c]);
            return *columns_[c];
        }
        if (column_pointers_.size() != columns_.size()) {
            throw std::logic_error("Lazy loading a column but the pointer was not set");
        }
        throw std::runtime_error("row_group_t::get_column: unknown error");
    }

    storage::block_manager_t& row_group_t::block_manager() { return collection_->block_manager(); }

    void row_group_t::initialize_empty(const std::pmr::vector<types::complex_logical_type>& types) {
        assert(columns_.empty());
        for (uint64_t i = 0; i < types.size(); i++) {
            auto column_data =
                column_data_t::create_column(collection_->resource(), block_manager(), i, start, types[i]);
            columns_.push_back(std::move(column_data));
        }
    }

    bool row_group_t::initialize_scan_with_offset(collection_scan_state& state, uint64_t vector_offset) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        state.vector_index = vector_offset;
        state.max_row_group_row =
            start > state.max_row ? 0 : std::min(static_cast<int64_t>(count.load()), state.max_row - start);
        auto row_number = start + static_cast<int64_t>(vector_offset * vector::DEFAULT_VECTOR_CAPACITY);
        if (state.max_row_group_row == 0) {
            return false;
        }
        assert(!state.column_scans.empty());
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan_with_offset(state.column_scans[i], row_number);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    bool row_group_t::initialize_scan(collection_scan_state& state) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        state.max_row_group_row +=
            start > state.max_row ? 0 : std::min(static_cast<int64_t>(count.load()), state.max_row - start);
        if (state.max_row_group_row == 0) {
            return false;
        }
        assert(!state.column_scans.empty());
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan(state.column_scans[i]);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    std::unique_ptr<row_group_t> row_group_t::add_column(collection_t* new_collection,
                                                         column_definition_t& new_column,
                                                         const std::optional<types::logical_value_t>& default_value,
                                                         vector::vector_t& result) {
        auto added_column = column_data_t::create_column(collection_->resource(),
                                                         block_manager(),
                                                         get_column_count(),
                                                         start,
                                                         new_column.type());

        uint64_t rows_to_write = count;
        if (rows_to_write > 0) {
            const types::logical_value_t fill_value = default_value.has_value()
                                                          ? *default_value
                                                          : types::logical_value_t{collection_->resource(), new_column.type()};
            column_append_state state;
            added_column->initialize_append(state);
            for (uint64_t i = 0; i < rows_to_write; i += vector::DEFAULT_VECTOR_CAPACITY) {
                uint64_t rows_in_this_vector = std::min<uint64_t>(rows_to_write - i, vector::DEFAULT_VECTOR_CAPACITY);
                result.reference(fill_value);
                if (!default_value.has_value()) {
                    result.set_null(true);
                }
                added_column->append(state, result, rows_in_this_vector);
            }
        }

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        row_group->columns_ = columns();
        row_group->columns_.push_back(std::move(added_column));

        return row_group;
    }

    std::unique_ptr<row_group_t> row_group_t::remove_column(collection_t* new_collection, uint64_t removed_column) {
        assert(removed_column < columns_.size());

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        auto& cols = columns();
        for (uint64_t i = 0; i < cols.size(); i++) {
            if (i != removed_column) {
                row_group->columns_.push_back(cols[i]);
            }
        }

        return row_group;
    }

    void row_group_t::next_vector(collection_scan_state& state) {
        state.vector_index++;
        const auto& column_ids = state.column_ids();
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.is_row_id_column()) {
                continue;
            }
            get_column(column).skip(state.column_scans[i]);
        }
    }

    bool row_group_t::check_predicate(int64_t row_id, const table_filter_t* filter) {
        switch (filter->filter_type) {
            case expressions::compare_type::union_or: {
                auto& conjunction_or = filter->cast<conjunction_or_filter_t>();
                for (auto& child_filter : conjunction_or.child_filters) {
                    if (check_predicate(row_id, child_filter.get())) {
                        return true;
                    }
                }
                return false;
            }
            case expressions::compare_type::union_and: {
                auto& conjunction_and = filter->cast<conjunction_and_filter_t>();
                for (auto& child_filter : conjunction_and.child_filters) {
                    if (!check_predicate(row_id, child_filter.get())) {
                        return false;
                    }
                }
                return true;
            }
            case expressions::compare_type::union_not: {
                auto& conjunction_not = filter->cast<conjunction_not_filter_t>();
                for (auto& child_filter : conjunction_not.child_filters) {
                    if (check_predicate(row_id, child_filter.get())) {
                        return false;
                    }
                }
                return true;
            }
            case expressions::compare_type::invalid: {
                throw std::logic_error("invalid type for filter selection");
            }
            case expressions::compare_type::is_null:
            case expressions::compare_type::is_not_null: {
                auto& null_filter = filter->cast<is_null_filter_t>();
                column_data_t* column = &get_column(null_filter.table_indices.front());
                for (size_t i = 1; i < null_filter.table_indices.size(); i++) {
                    column =
                        static_cast<struct_column_data_t*>(column)->sub_columns[null_filter.table_indices[i]].get();
                }
                bool is_valid = column->check_validity(row_id);
                return filter->filter_type == expressions::compare_type::is_null ? !is_valid : is_valid;
            }
            default: {
                auto& constant_filter = filter->cast<constant_filter_t>();
                column_data_t* column = &get_column(constant_filter.table_indices.front());
                for (size_t i = 1; i < constant_filter.table_indices.size(); i++) {
                    column =
                        static_cast<struct_column_data_t*>(column)->sub_columns[constant_filter.table_indices[i]].get();
                }
                return column->check_predicate(row_id, filter);
            }
        }
    }

    bool row_group_t::check_zonemap_segments(collection_scan_state& state) {
        auto* f = state.filter();
        if (!f) {
            return true;
        }
        // For constant comparison filters, check if any column's zonemap prunes this segment
        if (f->filter_type == expressions::compare_type::eq || f->filter_type == expressions::compare_type::gt ||
            f->filter_type == expressions::compare_type::gte || f->filter_type == expressions::compare_type::lt ||
            f->filter_type == expressions::compare_type::lte) {
            auto& cf = f->cast<constant_filter_t>();
            if (!cf.table_indices.empty()) {
                auto col_idx = cf.table_indices.front();
                if (col_idx < get_column_count()) {
                    auto& col = get_column(col_idx);
                    column_scan_state dummy;
                    auto result = col.check_zonemap(dummy, const_cast<table_filter_t&>(*f));
                    if (result == filter_propagate_result_t::ALWAYS_FALSE) {
                        next_vector(state);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    void row_group_t::filter_indexing(std::pmr::memory_resource* resource,
                                      uint64_t vector_index,
                                      vector::indexing_vector_t& indexing,
                                      const table_filter_t* filter,
                                      uint64_t& approved_tuple_count) {
        vector::indexing_vector_t new_indexing(resource, approved_tuple_count);
        uint64_t result_count = 0;
        for (uint64_t i = 0; i < approved_tuple_count; i++) {
            auto idx = indexing.get_index(i);
            new_indexing.set_index(result_count, idx);
            result_count +=
                check_predicate(static_cast<int64_t>(idx + vector_index * vector::DEFAULT_VECTOR_CAPACITY), filter);
        }
        indexing = new_indexing;
        approved_tuple_count = result_count;
    }

    bool row_group_t::try_scan_pax_generic_projected(collection_scan_state& state, vector::data_chunk_t& result) {
        if (layout_kind_ != storage::row_group_layout_kind::PAX_GENERIC || !pax_generic_layout_.has_value()) {
            return false;
        }
        if (version_info_.load() != nullptr || !deletes_pointers_.empty()) {
            return false;
        }
        if (state.txn.transaction_id != 0 || state.txn.start_time != 0) {
            return false;
        }

        const auto& column_ids = state.column_ids();
        if (column_ids.empty()) {
            return false;
        }

        const constant_filter_t* constant_filter = nullptr;
        uint32_t filter_column_index = 0;
        int64_t filter_scan_index = -1;

        auto* filter = state.filter();
        if (filter) {
            if (!is_supported_pax_projected_filter_compare(filter->filter_type)) {
                return false;
            }

            auto* constant = dynamic_cast<const constant_filter_t*>(filter);
            if (!constant || constant->table_indices.size() != 1) {
                return false;
            }
            filter_column_index = static_cast<uint32_t>(constant->table_indices.front());
            constant_filter = constant;
        }

        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.is_row_id_column() || column.has_children()) {
                return false;
            }

            const auto column_index = column.primary_index();
            if (column_index >= get_column_count()) {
                return false;
            }

            auto& column_data = get_column(column_index);
            if (!is_pax_generic_string_type(column_data.type()) || column_data.has_updates()) {
                return false;
            }
            if (constant_filter && column_index == filter_column_index) {
                filter_scan_index = static_cast<int64_t>(i);
            }
        }

        if (constant_filter) {
            if (filter_column_index >= get_column_count()) {
                return false;
            }
            auto& filter_column = get_column(filter_column_index);
            if (!is_pax_generic_string_type(filter_column.type()) || filter_column.has_updates()) {
                return false;
            }
            if (filter_scan_index < 0) {
                return false;
            }
        }

        const auto& layout = *pax_generic_layout_;
        if (!is_supported_pax_generic_layout_version(layout.version) || layout.rows_per_page == 0 ||
            layout.pages.empty()) {
            return false;
        }

        uint64_t expected_row_offset = 0;
        for (const auto& page : layout.pages) {
            if (page.tuple_count == 0 || page.row_offset_in_group != expected_row_offset ||
                page.tuple_count > layout.rows_per_page) {
                return false;
            }
            expected_row_offset += page.tuple_count;
        }
        if (expected_row_offset != count.load() || pax_generic_layout_tuple_count(layout) != count.load()) {
            return false;
        }

        for (const auto& column : column_ids) {
            const auto column_index = static_cast<uint32_t>(column.primary_index());
            for (const auto& page : layout.pages) {
                const auto* value_slice =
                    find_pax_generic_slice(page, column_index, storage::pax_generic_slice_kind::STRING_VALUES);
                const auto* validity_slice =
                    find_pax_generic_slice(page, column_index, storage::pax_generic_slice_kind::VALIDITY);
                if (!value_slice || !validity_slice || !validate_pax_generic_string_slice(*value_slice, page.tuple_count) ||
                    !validate_pax_generic_validity_slice(*validity_slice, page.tuple_count)) {
                    return false;
                }
            }
        }

        for (auto& column_state : state.column_scans) {
            column_state.result_offset = result.size();
        }

        pax_generic_block_cache_t block_cache;
        const auto local_max_row_group_row =
            start > state.max_row ? 0 : static_cast<uint64_t>(std::min(static_cast<int64_t>(count.load()),
                                                                       state.max_row - start));

        while (true) {
            const auto current_row = current_pax_fixed_row_offset(*this, state);
            if (current_row >= local_max_row_group_row) {
                return true;
            }

            const auto max_count =
                std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, local_max_row_group_row - current_row);

            if (!check_zonemap_segments(state)) {
                continue;
            }

            const auto result_offset = result.size();
            if (!constant_filter) {
                validate_chunk_capacity(result, result_offset + max_count);
                for (const auto& column : column_ids) {
                    const auto out_idx = column.primary_index();
                    if (out_idx >= result.data.size() || !result.data[out_idx].data() ||
                        result.data[out_idx].type().to_physical_type() != types::physical_type::STRING) {
                        return false;
                    }
                    if (!decode_pax_generic_string_window(*this,
                                                          layout,
                                                          static_cast<uint32_t>(column.primary_index()),
                                                          current_row,
                                                          max_count,
                                                          result.data[out_idx],
                                                          result_offset,
                                                          block_cache)) {
                        return false;
                    }
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < max_count; i++) {
                    row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + i);
                }
                mark_vector_range_valid(result.row_ids, result_offset, max_count);

                result.set_cardinality(result_offset + max_count);
                state.valid_indexing = vector::indexing_vector_t(result.resource(), 0, result.capacity());
                state.vector_index++;
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                    column_state.result_offset += max_count;
                }
                return true;
            }

            vector::vector_t filter_values(result.resource(), get_column(filter_column_index).type(), max_count);
            if (!decode_pax_generic_string_window(*this,
                                                  layout,
                                                  filter_column_index,
                                                  current_row,
                                                  max_count,
                                                  filter_values,
                                                  0,
                                                  block_cache)) {
                return false;
            }

            vector::indexing_vector_t indexing(result.resource(), max_count);
            auto approved_count =
                apply_pax_generic_string_constant_filter(filter_values, *constant_filter, max_count, indexing);
            if (approved_count == 0) {
                state.vector_index++;
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                }
                continue;
            }

            validate_chunk_capacity(result, result_offset + approved_count);
            for (uint64_t i = 0; i < column_ids.size(); i++) {
                const auto& column = column_ids[i];
                const auto out_idx = column.primary_index();
                if (out_idx >= result.data.size() || !result.data[out_idx].data() ||
                    result.data[out_idx].type().to_physical_type() != types::physical_type::STRING) {
                    return false;
                }

                if (static_cast<int64_t>(i) == filter_scan_index) {
                    vector::vector_ops::copy(filter_values,
                                             result.data[out_idx],
                                             indexing,
                                             approved_count,
                                             0,
                                             result_offset);
                    continue;
                }

                vector::vector_t temp_values(result.resource(), get_column(column).type(), max_count);
                if (!decode_pax_generic_string_window(*this,
                                                      layout,
                                                      static_cast<uint32_t>(column.primary_index()),
                                                      current_row,
                                                      max_count,
                                                      temp_values,
                                                      0,
                                                      block_cache)) {
                    return false;
                }
                vector::vector_ops::copy(temp_values,
                                         result.data[out_idx],
                                         indexing,
                                         approved_count,
                                         0,
                                         result_offset);
            }

            result.row_ids.set_vector_type(vector::vector_type::FLAT);
            auto* row_ids = result.row_ids.data<int64_t>();
            for (uint64_t i = 0; i < approved_count; i++) {
                row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + indexing.get_index(i));
            }
            mark_vector_range_valid(result.row_ids, result_offset, approved_count);

            result.set_cardinality(result_offset + approved_count);
            state.valid_indexing = indexing;
            state.vector_index++;
            for (auto& column_state : state.column_scans) {
                advance_pax_fixed_scan_state(column_state, max_count);
                column_state.result_offset += approved_count;
            }
            return true;
        }
    }

    bool row_group_t::try_scan_pax_fixed_projected(collection_scan_state& state, vector::data_chunk_t& result) {
        if (layout_kind_ != storage::row_group_layout_kind::PAX_FIXED || !pax_fixed_layout_.has_value()) {
            return false;
        }
        if (version_info_.load() != nullptr || !deletes_pointers_.empty()) {
            return false;
        }
        if (state.txn.transaction_id != 0 || state.txn.start_time != 0) {
            return false;
        }

        const auto& column_ids = state.column_ids();
        if (column_ids.empty()) {
            return false;
        }

        const constant_filter_t* constant_filter = nullptr;
        uint32_t filter_column_index = 0;
        int64_t filter_scan_index = -1;

        auto* filter = state.filter();
        if (filter) {
            if (!is_supported_pax_projected_filter_compare(filter->filter_type)) {
                return false;
            }

            auto* constant = dynamic_cast<const constant_filter_t*>(filter);
            if (!constant || constant->table_indices.size() != 1) {
                return false;
            }
            filter_column_index = static_cast<uint32_t>(constant->table_indices.front());
            constant_filter = constant;
        }

        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.is_row_id_column() || column.has_children()) {
                return false;
            }
            auto column_index = column.primary_index();
            if (column_index >= get_column_count()) {
                return false;
            }
            auto& column_data = get_column(column_index);
            if (!is_pax_fixed_integer_type(column_data.type()) || column_data.has_updates()) {
                return false;
            }
            if (constant_filter && column_index == filter_column_index) {
                filter_scan_index = static_cast<int64_t>(i);
            }
        }

        if (constant_filter) {
            if (filter_column_index >= get_column_count()) {
                return false;
            }
            auto& filter_column = get_column(filter_column_index);
            if (!is_pax_fixed_integer_type(filter_column.type()) || filter_column.has_updates()) {
                return false;
            }
            if (filter_scan_index < 0) {
                return false;
            }
        }

        const auto& layout = *pax_fixed_layout_;
        if (!is_supported_pax_fixed_layout_version(layout.version) || layout.rows_per_page == 0 ||
            layout.pages.empty()) {
            return false;
        }

        uint64_t expected_row_offset = 0;
        for (const auto& page : layout.pages) {
            if (page.tuple_count == 0 || page.row_offset_in_group != expected_row_offset ||
                page.tuple_count > layout.rows_per_page) {
                return false;
            }
            expected_row_offset += page.tuple_count;
        }
        if (expected_row_offset != count.load() || pax_fixed_layout_tuple_count(layout) != count.load()) {
            return false;
        }

        for (const auto& column : column_ids) {
            const auto column_index = static_cast<uint32_t>(column.primary_index());
            const auto expected_type = to_pax_fixed_column_type(get_column(column_index).type());
            const auto type_size = static_cast<uint64_t>(get_column(column_index).type().size());
            for (const auto& page : layout.pages) {
                const auto* slice = find_pax_fixed_slice(page, column_index);
                if (!slice ||
                    !validate_pax_fixed_slice(*slice, expected_type, page.tuple_count, type_size, layout.version)) {
                    return false;
                }
            }
        }

        for (auto& column_state : state.column_scans) {
            column_state.result_offset = result.size();
        }

        pax_fixed_block_cache_t block_cache;
        const auto local_max_row_group_row =
            start > state.max_row ? 0 : static_cast<uint64_t>(std::min(static_cast<int64_t>(count.load()),
                                                                       state.max_row - start));

        while (true) {
            const auto current_row = current_pax_fixed_row_offset(*this, state);
            if (current_row >= local_max_row_group_row) {
                return true;
            }

            const auto max_count =
                std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, local_max_row_group_row - current_row);

            if (!check_zonemap_segments(state)) {
                continue;
            }

            const auto result_offset = result.size();

            if (!constant_filter) {
                validate_chunk_capacity(result, result_offset + max_count);
                for (const auto& column : column_ids) {
                    const auto out_idx = column.primary_index();
                    if (out_idx >= result.data.size() || !result.data[out_idx].data()) {
                        return false;
                    }
                    if (!decode_pax_fixed_window(*this,
                                                 layout,
                                                 static_cast<uint32_t>(column.primary_index()),
                                                 get_column(column).type(),
                                                 current_row,
                                                 max_count,
                                                 result.data[out_idx],
                                                 result_offset,
                                                 block_cache)) {
                        return false;
                    }
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < max_count; i++) {
                    row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + i);
                }
                mark_vector_range_valid(result.row_ids, result_offset, max_count);

                result.set_cardinality(result_offset + max_count);
                state.valid_indexing = vector::indexing_vector_t(result.resource(), 0, result.capacity());
                state.vector_index++;
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                    column_state.result_offset += max_count;
                }
                return true;
            }

            vector::vector_t filter_values(result.resource(), get_column(filter_column_index).type(), max_count);
            if (!decode_pax_fixed_window(*this,
                                         layout,
                                         filter_column_index,
                                         get_column(filter_column_index).type(),
                                         current_row,
                                         max_count,
                                         filter_values,
                                         0,
                                         block_cache)) {
                return false;
            }

            vector::indexing_vector_t indexing(result.resource(), max_count);
            auto approved_count = apply_pax_fixed_constant_filter(filter_values, *constant_filter, max_count, indexing);
            if (approved_count == 0) {
                state.vector_index++;
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                }
                continue;
            }

            validate_chunk_capacity(result, result_offset + approved_count);
            for (uint64_t i = 0; i < column_ids.size(); i++) {
                const auto& column = column_ids[i];
                const auto out_idx = column.primary_index();
                if (out_idx >= result.data.size() || !result.data[out_idx].data()) {
                    return false;
                }

                if (static_cast<int64_t>(i) == filter_scan_index) {
                    vector::vector_ops::copy(filter_values,
                                             result.data[out_idx],
                                             indexing,
                                             approved_count,
                                             0,
                                             result_offset);
                    continue;
                }

                vector::vector_t temp_values(result.resource(), get_column(column).type(), max_count);
                if (!decode_pax_fixed_window(*this,
                                             layout,
                                             static_cast<uint32_t>(column.primary_index()),
                                             get_column(column).type(),
                                             current_row,
                                             max_count,
                                             temp_values,
                                             0,
                                             block_cache)) {
                    return false;
                }
                vector::vector_ops::copy(temp_values,
                                         result.data[out_idx],
                                         indexing,
                                         approved_count,
                                         0,
                                         result_offset);
            }

            result.row_ids.set_vector_type(vector::vector_type::FLAT);
            auto* row_ids = result.row_ids.data<int64_t>();
            for (uint64_t i = 0; i < approved_count; i++) {
                row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + indexing.get_index(i));
            }
            mark_vector_range_valid(result.row_ids, result_offset, approved_count);

            result.set_cardinality(result_offset + approved_count);
            state.valid_indexing = indexing;
            state.vector_index++;
            for (auto& column_state : state.column_scans) {
                advance_pax_fixed_scan_state(column_state, max_count);
                column_state.result_offset += approved_count;
            }
            return true;
        }
    }

    template<table_scan_type TYPE>
    void row_group_t::templated_scan(collection_scan_state& state, vector::data_chunk_t& result) {
        constexpr bool ALLOW_UPDATES = TYPE != table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES &&
                                       TYPE != table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
        const auto& column_ids = state.column_ids();
        auto* filter = state.filter();
        // Sync result_offset with current chunk cardinality (handles chunk reset between calls)
        for (auto& column_state : state.column_scans) {
            column_state.result_offset = result.size();
        }
        while (true) {
            if (static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY) >= state.max_row_group_row) {
                return;
            }
            int64_t current_row = static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            auto max_count =
                std::min(vector::DEFAULT_VECTOR_CAPACITY, static_cast<size_t>(state.max_row_group_row - current_row));

            if (!check_zonemap_segments(state)) {
                continue;
            }

            uint64_t count;
            if (TYPE == table_scan_type::REGULAR) {
                count = (state.txn.transaction_id != 0 || state.txn.start_time != 0)
                            ? state.row_group->indexing_vector(state.txn,
                                                               state.vector_index,
                                                               state.valid_indexing,
                                                               max_count)
                            : state.row_group->indexing_vector(state.vector_index, state.valid_indexing, max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else if (TYPE == table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
                count = state.row_group->commited_indexing_vector(state.vector_index, state.valid_indexing, max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else {
                count = max_count;
            }
            validate_chunk_capacity(result, result.size() + count);

            if (count == max_count && !filter) {
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    // Write into the output slot corresponding to the storage column index
                    // (for row_id column, write into slot i as the caller expects it there).
                    size_t out_idx = column.is_row_id_column() ? i : column.primary_index();
                    if (column.is_row_id_column()) {
                        assert(result.data[out_idx].type().type() == types::logical_type::BIGINT);
                        result.data[out_idx].sequence(static_cast<int64_t>(start + current_row), 1, count);
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE == table_scan_type::REGULAR) {
                            col_data.scan(state.vector_index, state.column_scans[i], result.data[out_idx]);
                        } else {
                            col_data.scan_committed(state.vector_index,
                                                    state.column_scans[i],
                                                    result.data[out_idx],
                                                    ALLOW_UPDATES);
                        }
                    }
                }
                state.valid_indexing = vector::indexing_vector_t(result.resource(), 0, result.capacity());
            } else {
                uint64_t approved_tuple_count = count;
                vector::indexing_vector_t indexing(result.resource(), result.capacity());
                if (count != max_count) {
                    indexing = state.valid_indexing;
                } else {
                    indexing.reset(nullptr);
                }
                if (filter) {
                    assert(ALLOW_UPDATES);
                    filter_indexing(collection_->resource(),
                                    state.vector_index,
                                    indexing,
                                    filter,
                                    approved_tuple_count);
                }
                if (approved_tuple_count == 0) {
                    for (uint64_t i = 0; i < column_ids.size(); i++) {
                        auto& col_idx = column_ids[i];
                        if (col_idx.is_row_id_column()) {
                            continue;
                        }
                        auto& col_data = get_column(col_idx);
                        col_data.skip(state.column_scans[i]);
                    }
                    state.vector_index++;
                    continue;
                }
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    auto& column = column_ids[i];
                    size_t out_idx = column.is_row_id_column() ? i : column.primary_index();
                    if (column.is_row_id_column()) {
                        assert(result.data[out_idx].type().type() == types::logical_type::BIGINT);
                        result.data[out_idx].set_vector_type(vector::vector_type::FLAT);
                        auto result_data = result.data[out_idx].data<int64_t>();
                        for (size_t indexing_idx = 0; indexing_idx < approved_tuple_count; indexing_idx++) {
                            result_data[indexing_idx] =
                                start + current_row + static_cast<int64_t>(indexing.get_index(indexing_idx));
                        }
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE == table_scan_type::REGULAR) {
                            vector::vector_t select_vector(result.resource(), result.data[out_idx].type(), max_count);
                            auto prev_offset = state.column_scans[i].result_offset;
                            state.column_scans[i].result_offset = 0;
                            col_data.select(state.vector_index,
                                            state.column_scans[i],
                                            select_vector,
                                            indexing,
                                            approved_tuple_count);
                            state.column_scans[i].result_offset = prev_offset;
                            vector::vector_ops::copy(select_vector,
                                                     result.data[out_idx],
                                                     approved_tuple_count,
                                                     0,
                                                     state.column_scans[i].result_offset);
                        } else {
                            col_data.select_committed(state.vector_index,
                                                      state.column_scans[i],
                                                      result.data[out_idx],
                                                      indexing,
                                                      approved_tuple_count,
                                                      ALLOW_UPDATES);
                        }
                    }
                }

                assert(approved_tuple_count > 0);
                count = approved_tuple_count;
                state.valid_indexing = indexing;
            }
            for (uint64_t i = 0; i < count; i++) {
                types::logical_value_t index{result.row_ids.resource(),
                                             static_cast<int64_t>(state.vector_index * vector::DEFAULT_VECTOR_CAPACITY +
                                                                  state.valid_indexing.get_index(i))};
                result.row_ids.set_value(result.size() + i, std::move(index));
            }
            result.set_cardinality(result.size() + count);
            state.vector_index++;
            for (auto& column_state : state.column_scans) {
                column_state.result_offset += count;
            }
            break;
        }
    }

    void row_group_t::scan(collection_scan_state& state, vector::data_chunk_t& result) {
        if (try_scan_pax_generic_projected(state, result)) {
            return;
        }
        if (try_scan_pax_fixed_projected(state, result)) {
            return;
        }
        templated_scan<table_scan_type::REGULAR>(state, result);
    }

    void row_group_t::scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        switch (type) {
            case table_scan_type::COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES:
                templated_scan<table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
            case table_scan_type::LATEST_COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(state, result);
                break;
            default:
                throw std::logic_error("Unrecognized table scan type");
        }
    }

    void row_group_t::fetch_row(column_fetch_state& state,
                                const std::vector<storage_index_t>& column_ids,
                                int64_t row_id,
                                vector::data_chunk_t& result,
                                uint64_t result_idx) {
        for (uint64_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            auto& column = column_ids[col_idx];
            auto& result_vector = result.data[col_idx];
            assert(result_vector.get_vector_type() == vector::vector_type::FLAT);
            assert(!result_vector.is_null(result_idx));
            if (column.is_row_id_column()) {
                assert(result_vector.type().to_physical_type() == types::physical_type::INT64);
                result_vector.set_vector_type(vector::vector_type::FLAT);
                auto data = result_vector.data<int64_t>();
                data[result_idx] = row_id;
            } else {
                auto& col_data = get_column(column);
                col_data.fetch_row(state, row_id, result_vector, result_idx);
            }
        }
    }

    bool row_group_t::row_visible(transaction_data txn, int64_t row_id) {
        auto vinfo = version_info();
        if (!vinfo) {
            return true;
        }

        auto local_row = static_cast<uint64_t>(row_id - start);
        auto vector_idx = local_row / vector::DEFAULT_VECTOR_CAPACITY;
        auto row_in_vector = local_row % vector::DEFAULT_VECTOR_CAPACITY;
        vector::indexing_vector_t visible_rows(collection().resource(), row_in_vector + 1);

        uint64_t visible_count = (txn.transaction_id == 0 && txn.start_time == 0)
                                     ? commited_indexing_vector(vector_idx, visible_rows, row_in_vector + 1)
                                     : indexing_vector(txn, vector_idx, visible_rows, row_in_vector + 1);
        if (visible_count == row_in_vector + 1) {
            return true;
        }
        for (uint64_t i = 0; i < visible_count; i++) {
            if (visible_rows.get_index(i) == row_in_vector) {
                return true;
            }
        }
        return false;
    }

    void row_group_t::append_version_info(transaction_data txn, uint64_t count) {
        uint64_t row_group_start = this->count.load();
        uint64_t row_group_end = row_group_start + count;
        if (row_group_end > row_group_size()) {
            row_group_end = row_group_size();
        }
        this->count = row_group_end;
        get_or_create_version_info().append_version_info(txn, count, row_group_start, row_group_end);
    }

    void row_group_t::commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_append(commit_id, row_group_start, count);
        }
        // Update current_version_ so that scans without explicit txn data can see committed rows
        if (commit_id > current_version_) {
            current_version_ = commit_id;
        }
    }

    void row_group_t::revert_append(uint64_t row_group_start) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_append(row_group_start);
        }
        if (row_group_start < this->count.load()) {
            this->count = row_group_start;
        }
    }

    void row_group_t::initialize_append(row_group_append_state& append_state) {
        append_state.row_group = this;
        append_state.offset_in_row_group = count;
        append_state.states = std::make_unique<column_append_state[]>(get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            col_data.initialize_append(append_state.states[i]);
        }
    }

    void row_group_t::append(row_group_append_state& state, vector::data_chunk_t& chunk, uint64_t append_count) {
        assert(chunk.column_count() == get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            auto prev_allocation_size = col_data.allocation_size();
            col_data.append(state.states[i], chunk.data[i], append_count);
            allocation_size_ += col_data.allocation_size() - prev_allocation_size;
        }
        state.offset_in_row_group += append_count;
    }

    void row_group_t::update(vector::data_chunk_t& update_chunk,
                             int64_t* ids,
                             uint64_t offset,
                             uint64_t count,
                             const std::vector<uint64_t>& column_ids) {
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            assert(column != std::numeric_limits<uint64_t>::max());
            auto& col_data = get_column(column);
            assert(col_data.type().type() == update_chunk.data[i].type().type());
            if (offset > 0) {
                vector::vector_t sliced_vector(update_chunk.data[i], offset, count);
                sliced_vector.flatten(count);
                col_data.update(column, sliced_vector, ids + offset, count);
            } else {
                col_data.update(column, update_chunk.data[i], ids, count);
            }
        }
    }

    void row_group_t::update_column(vector::data_chunk_t& updates,
                                    vector::vector_t& row_ids,
                                    const std::vector<uint64_t>& column_path) {
        assert(updates.column_count() == 1);
        auto ids = row_ids.data<int64_t>();

        auto primary_column_idx = column_path[0];
        assert(primary_column_idx != std::numeric_limits<uint64_t>::max());
        assert(primary_column_idx < columns_.size());
        auto& col_data = get_column(primary_column_idx);
        col_data.update_column(column_path, updates.data[0], ids, updates.size(), 1);
    }

    uint64_t row_group_t::committed_row_count() {
        auto* vi = version_info_.load();
        if (vi) {
            return count - vi->commited_deleted_count(count);
        }
        return count;
    }

    bool row_group_t::has_unloaded_deletes() const {
        if (deletes_pointers_.empty()) {
            return false;
        }
        return !deletes_is_loaded_;
    }

    void row_group_t::get_column_segment_info(uint64_t row_group_index, std::vector<column_segment_info>& result) {
        for (uint64_t col_idx = 0; col_idx < get_column_count(); col_idx++) {
            auto& col_data = get_column(col_idx);
            col_data.get_column_segment_info(row_group_index, {col_idx}, result);
        }
    }

    class version_delete_state {
    public:
        version_delete_state(row_group_t& info,
                             uint64_t current_version,
                             data_table_t& table,
                             int64_t base_row,
                             bool is_txn = false)
            : info(info)
            , table(table)
            , current_chunk(storage::INVALID_INDEX)
            , current_version(current_version)
            , base_row(base_row)
            , delete_count(0)
            , count(0)
            , is_txn_(is_txn) {}

        row_group_t& info;
        data_table_t& table;
        uint64_t current_chunk;
        uint64_t current_version;
        int64_t rows[vector::DEFAULT_VECTOR_CAPACITY];
        int64_t base_row;
        uint64_t chunk_row;
        uint64_t delete_count;
        uint64_t count;
        bool is_txn_;

        void delete_row(int64_t row_id);
        void flush();
    };

    uint64_t row_group_t::delete_rows(uint64_t vector_idx, int64_t rows[], uint64_t count) {
        return get_or_create_version_info().delete_rows(vector_idx, ++current_version_, rows, count);
    }

    uint64_t row_group_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id) {
        version_delete_state del_state(*this, transaction_id, table, start, true);

        for (uint64_t i = 0; i < count; i++) {
            assert(ids[i] >= 0);
            assert(ids[i] >= start && ids[i] < start + static_cast<int64_t>(this->count));
            del_state.delete_row(ids[i]);
        }
        del_state.flush();
        return del_state.delete_count;
    }

    void row_group_t::commit_delete(uint64_t commit_id, uint64_t vector_idx, const delete_info& info) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_delete(vector_idx, commit_id, info);
        }
    }

    void row_group_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->commit_all_deletes(txn_id, commit_id);
        }
        // Advance current_version_ past commit_id so that committed deletes
        // are visible to scans using commited_version_operator
        if (commit_id >= current_version_) {
            current_version_ = commit_id + 1;
        }
    }

    row_version_manager_t& row_group_t::get_or_create_version_info() {
        auto vinfo = version_info();
        if (vinfo) {
            return *vinfo;
        }
        return *get_or_create_version_info_internal();
    }

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_ptr() {
        auto vinfo = version_info();
        if (vinfo) {
            return owned_version_info_;
        }
        return get_or_create_version_info_internal();
    }

    uint64_t row_group_t::calculate_size() {
        vector::indexing_vector_t temp_indexing(collection().resource(), count);
        return indexing_vector(index, temp_indexing, count);
    }

    uint64_t
    row_group_t::indexing_vector(uint64_t vector_idx, vector::indexing_vector_t& indexing_vector, uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->indexing_vector({current_version_, current_version_}, vector_idx, indexing_vector, max_count);
    }

    uint64_t row_group_t::indexing_vector(transaction_data txn,
                                          uint64_t vector_idx,
                                          vector::indexing_vector_t& indexing_vector,
                                          uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->indexing_vector(txn, vector_idx, indexing_vector, max_count);
    }

    uint64_t row_group_t::commited_indexing_vector(uint64_t vector_idx,
                                                   vector::indexing_vector_t& indexing_vector,
                                                   uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->commited_indexing_vector(current_version_,
                                               current_version_,
                                               vector_idx,
                                               indexing_vector,
                                               max_count);
    }

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_internal() {
        std::lock_guard lock(row_group_lock_);
        if (!owned_version_info_) {
            auto new_info = std::make_shared<row_version_manager_t>(start);
            set_version_info(std::move(new_info));
        }
        return owned_version_info_;
    }

    row_version_manager_t* row_group_t::version_info() {
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        std::lock_guard lock(row_group_lock_);
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        set_version_info(nullptr);
        deletes_is_loaded_ = true;
        return version_info_;
    }

    void row_group_t::set_version_info(std::shared_ptr<row_version_manager_t> version) {
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void version_delete_state::delete_row(int64_t row_id) {
        assert(row_id >= 0);
        uint64_t vector_idx = static_cast<uint64_t>(row_id) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t idx_in_vector = static_cast<uint64_t>(row_id) - vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        if (current_chunk != vector_idx) {
            flush();

            current_chunk = vector_idx;
            chunk_row = vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        }
        rows[count++] = static_cast<int64_t>(idx_in_vector);
    }

    void version_delete_state::flush() {
        if (count == 0) {
            return;
        }
        uint64_t actual_delete_count;
        if (is_txn_) {
            actual_delete_count =
                info.get_or_create_version_info().delete_rows(current_chunk, current_version, rows, count);
        } else {
            actual_delete_count = info.delete_rows(current_chunk, rows, count);
        }
        delete_count += actual_delete_count;
        count = 0;
    }

    storage::row_group_pointer_t row_group_t::write_to_disk(storage::partial_block_manager_t& partial_block_manager) {
        storage::row_group_pointer_t pointer;
        pointer.row_start = static_cast<uint64_t>(start);
        pointer.tuple_count = count;

        auto col_count = get_column_count();
        pointer.columnar_data_pointers.resize(col_count);

        std::vector<uint64_t> pax_generic_columns;
        pax_generic_columns.reserve(col_count);
        std::vector<uint64_t> pax_fixed_columns;
        pax_fixed_columns.reserve(col_count);
        bool pax_generic_requires_v2 = false;
        for (uint64_t i = 0; i < col_count; i++) {
            auto& column = get_column(i);
            if (is_pax_generic_string_type(column.type())) {
                pax_generic_columns.push_back(i);
            } else if (is_pax_generic_struct_type(column.type())) {
                if (!is_supported_pax_generic_column_type(column.type())) {
                    auto persistent = column.checkpoint(partial_block_manager);
                    pointer.columnar_data_pointers[i] = std::move(persistent.data_pointers);
                    continue;
                }
                pax_generic_columns.push_back(i);
                pax_generic_requires_v2 = true;
            } else if (is_pax_fixed_integer_type(column.type())) {
                pax_fixed_columns.push_back(i);
            } else {
                auto persistent = column.checkpoint(partial_block_manager);
                pointer.columnar_data_pointers[i] = std::move(persistent.data_pointers);
            }
        }

        if (!pax_generic_columns.empty() && pointer.tuple_count > 0) {
            for (uint64_t i = 0; i < col_count; i++) {
                if (!pointer.columnar_data_pointers[i].empty()) {
                    continue;
                }
                if (std::find(pax_generic_columns.begin(), pax_generic_columns.end(), i) != pax_generic_columns.end()) {
                    continue;
                }
                auto persistent = get_column(i).checkpoint(partial_block_manager);
                pointer.columnar_data_pointers[i] = std::move(persistent.data_pointers);
            }

            storage::pax_generic_row_group_layout_t pax_layout;
            pax_layout.version = pax_generic_requires_v2 ? 2 : 1;
            pax_layout.rows_per_page = PAX_GENERIC_ROWS_PER_PAGE;

            for (uint64_t row_offset = 0; row_offset < pointer.tuple_count; row_offset += PAX_GENERIC_ROWS_PER_PAGE) {
                storage::pax_generic_page_t page;
                page.row_offset_in_group = static_cast<uint32_t>(row_offset);
                page.tuple_count = static_cast<uint32_t>(std::min<uint64_t>(PAX_GENERIC_ROWS_PER_PAGE,
                                                                            pointer.tuple_count - row_offset));

                for (auto column_index : pax_generic_columns) {
                    auto& column = get_column(column_index);
                    if (is_pax_generic_string_type(column.type())) {
                        auto page_result = write_pax_generic_string_page(column,
                                                                         pointer.row_start,
                                                                         page.row_offset_in_group,
                                                                         page.tuple_count,
                                                                         partial_block_manager);
                        pointer.columnar_data_pointers[column_index].push_back(page_result.main_pointer);

                        storage::pax_generic_slice_t value_slice;
                        value_slice.column_index = static_cast<uint32_t>(column_index);
                        value_slice.slice_kind = storage::pax_generic_slice_kind::STRING_VALUES;
                        value_slice.codec_kind = storage::pax_generic_codec_kind::STRING_SEGMENT;
                        value_slice.payload = storage::pax_block_payload_t{page_result.main_pointer,
                                                                           page_result.extra_block_ids};
                        page.slices.push_back(std::move(value_slice));

                        append_pax_generic_validity_slice(page,
                                                          static_cast<uint32_t>(column_index),
                                                          empty_pax_generic_field_path(),
                                                          {page_result.validity_codec, page_result.validity_pointer});
                    } else {
                        write_pax_generic_column_page(column,
                                                      static_cast<uint32_t>(column_index),
                                                      empty_pax_generic_field_path(),
                                                      pointer.row_start,
                                                      page.row_offset_in_group,
                                                      page.tuple_count,
                                                      partial_block_manager,
                                                      page);
                    }
                }

                pax_layout.pages.push_back(std::move(page));
            }

            pointer.layout_kind = storage::row_group_layout_kind::PAX_GENERIC;
            pointer.pax_fixed_layout.reset();
            pointer.pax_generic_layout = pax_layout;
            layout_kind_ = pointer.layout_kind;
            pax_fixed_layout_.reset();
            pax_generic_layout_ = std::move(pax_layout);
            return pointer;
        }

        if (pax_fixed_columns.empty() || pointer.tuple_count == 0) {
            for (uint64_t i = 0; i < col_count; i++) {
                if (!pointer.columnar_data_pointers[i].empty()) {
                    continue;
                }
                auto persistent = get_column(i).checkpoint(partial_block_manager);
                pointer.columnar_data_pointers[i] = std::move(persistent.data_pointers);
            }
            pointer.layout_kind = storage::row_group_layout_kind::COLUMNAR;
            pax_fixed_layout_.reset();
            pax_generic_layout_.reset();
            layout_kind_ = pointer.layout_kind;
            return pointer;
        }

        storage::pax_fixed_row_group_layout_t pax_layout;
        pax_layout.version = 2;
        pax_layout.rows_per_page = PAX_FIXED_ROWS_PER_PAGE;

        for (uint64_t row_offset = 0; row_offset < pointer.tuple_count; row_offset += PAX_FIXED_ROWS_PER_PAGE) {
            storage::pax_fixed_page_t page;
            page.row_offset_in_group = static_cast<uint32_t>(row_offset);
            page.tuple_count = static_cast<uint32_t>(std::min<uint64_t>(PAX_FIXED_ROWS_PER_PAGE,
                                                                        pointer.tuple_count - row_offset));

            for (auto column_index : pax_fixed_columns) {
                auto& column = get_column(column_index);
                write_pax_fixed_slice(column,
                                      pointer.row_start,
                                      page.row_offset_in_group,
                                      page.tuple_count,
                                      static_cast<uint32_t>(column_index),
                                      partial_block_manager,
                                      pointer.columnar_data_pointers[column_index],
                                      page);
            }

            pax_layout.pages.push_back(std::move(page));
        }

        pointer.layout_kind = storage::row_group_layout_kind::PAX_FIXED;
        pointer.pax_fixed_layout = pax_layout;
        pointer.pax_generic_layout.reset();
        layout_kind_ = pointer.layout_kind;
        pax_fixed_layout_ = std::move(pax_layout);
        pax_generic_layout_.reset();
        return pointer;
    }

    void row_group_t::create_from_pointer(const storage::row_group_pointer_t& pointer) {
        count = pointer.tuple_count;
        layout_kind_ = pointer.layout_kind;
        pax_fixed_layout_ = pointer.pax_fixed_layout;
        pax_generic_layout_ = pointer.pax_generic_layout;
        auto col_count = get_column_count();
        auto ptrs_count = pointer.columnar_data_pointers.size();
        auto min_count = std::min(col_count, static_cast<uint64_t>(ptrs_count));

        for (uint64_t i = 0; i < min_count; i++) {
            persistent_column_data_t pcd(columns_[i]->resource());
            pcd.data_pointers = pointer.columnar_data_pointers[i];
            columns_[i]->initialize_column(pcd);
        }

        if (layout_kind_ != storage::row_group_layout_kind::PAX_GENERIC || !pax_generic_layout_.has_value()) {
            return;
        }

        if (!is_supported_pax_generic_layout_version(pax_generic_layout_->version)) {
            throw std::logic_error("unsupported pax_generic layout version");
        }

        struct pax_generic_validity_page_info_t {
            uint64_t row_start;
            uint32_t tuple_count;
            storage::pax_generic_codec_kind codec_kind;
            std::optional<storage::pax_block_payload_t> payload;
        };

        auto collect_generic_slices =
            [&](uint32_t root_column_index,
                const std::vector<uint16_t>& field_path,
                storage::pax_generic_slice_kind slice_kind) -> std::vector<const storage::pax_generic_slice_t*> {
            std::vector<const storage::pax_generic_slice_t*> slices;
            for (const auto& page : pax_generic_layout_->pages) {
                const auto* slice = find_pax_generic_slice(page, root_column_index, slice_kind, field_path);
                if (slice) {
                    slices.push_back(slice);
                }
            }
            return slices;
        };

        auto collect_generic_validity_infos =
            [&](uint32_t root_column_index,
                const std::vector<uint16_t>& field_path) -> std::vector<pax_generic_validity_page_info_t> {
            std::vector<pax_generic_validity_page_info_t> infos;
            for (const auto& page : pax_generic_layout_->pages) {
                const auto* slice =
                    find_pax_generic_slice(page, root_column_index, storage::pax_generic_slice_kind::VALIDITY, field_path);
                if (!slice) {
                    continue;
                }
                infos.push_back({static_cast<uint64_t>(start) + page.row_offset_in_group,
                                 page.tuple_count,
                                 slice->codec_kind,
                                 slice->payload});
            }
            return infos;
        };

        auto apply_validity_infos = [&](column_data_t& validity_column,
                                        const std::vector<pax_generic_validity_page_info_t>& infos,
                                        bool initialize_segments) {
            if (infos.empty()) {
                return;
            }

            if (initialize_segments) {
                persistent_column_data_t pcd(validity_column.resource());
                pcd.data_pointers.reserve(infos.size());
                for (const auto& info : infos) {
                    storage::data_pointer_t pointer_info;
                    pointer_info.row_start = info.row_start;
                    pointer_info.tuple_count = info.tuple_count;
                    pointer_info.segment_size = vector::validity_mask_t::validity_mask_size(info.tuple_count);
                    pcd.data_pointers.push_back(pointer_info);
                }
                validity_column.initialize_column_validity(pcd);
            }

            auto validity_lock = validity_column.data_.lock();
            for (uint64_t segment_index = 0; segment_index < infos.size(); segment_index++) {
                const auto& info = infos[segment_index];
                auto* validity_segment = validity_column.data_.segment_at(validity_lock,
                                                                         static_cast<int64_t>(segment_index));
                if (!validity_segment) {
                    throw std::logic_error("missing validity segment for pax_generic load");
                }

                auto validity_handle = block_manager().buffer_manager.pin(validity_segment->block);
                auto* target_ptr = validity_handle.ptr() + validity_segment->block_offset();
                switch (info.codec_kind) {
                    case storage::pax_generic_codec_kind::VALIDITY_ALL_VALID:
                        break;
                    case storage::pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                        std::memset(target_ptr, 0, validity_segment->segment_size());
                        break;
                    case storage::pax_generic_codec_kind::VALIDITY_BITMASK: {
                        if (!info.payload.has_value()) {
                            throw std::logic_error("missing pax_generic validity payload");
                        }
                        auto source_block =
                            block_manager().register_block(info.payload->main_pointer.block_pointer.block_id);
                        auto source_handle = block_manager().buffer_manager.pin(source_block);
                        auto* source_ptr =
                            source_handle.ptr() + info.payload->main_pointer.block_pointer.offset;
                        std::memcpy(target_ptr, source_ptr, info.payload->main_pointer.segment_size);
                        break;
                    }
                    case storage::pax_generic_codec_kind::STRING_SEGMENT:
                    case storage::pax_generic_codec_kind::FIXED_PLAIN:
                    default:
                        throw std::logic_error("invalid pax_generic validity codec");
                }
            }
        };

        auto register_string_blocks =
            [&](standard_column_data_t& standard_column,
                const std::vector<const storage::pax_generic_slice_t*>& slices) {
            auto data_lock = standard_column.data_.lock();
            for (uint64_t segment_index = 0; segment_index < slices.size(); segment_index++) {
                const auto* slice = slices[segment_index];
                if (!slice->payload.has_value()) {
                    throw std::logic_error("missing pax_generic string payload");
                }

                auto* segment = standard_column.data_.segment_at(data_lock, static_cast<int64_t>(segment_index));
                if (!segment || !segment->segment_state()) {
                    throw std::logic_error("missing string segment state for pax_generic load");
                }
                auto& string_state = segment->segment_state()->cast<uncompressed_string_segment_state>();
                for (auto block_id : slice->payload->extra_block_ids) {
                    string_state.register_block(block_manager(), block_id);
                }
            }
        };

        std::function<void(column_data_t&, uint32_t, const std::vector<uint16_t>&, bool)> load_generic_column;
        load_generic_column = [&](column_data_t& column,
                                  uint32_t root_column_index,
                                  const std::vector<uint16_t>& field_path,
                                  bool is_top_level) {
            if (is_pax_generic_struct_type(column.type())) {
                auto* struct_column = dynamic_cast<struct_column_data_t*>(&column);
                if (!struct_column) {
                    throw std::logic_error("pax_generic struct column is not a struct column");
                }

                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path);
                apply_validity_infos(struct_column->validity, validity_infos, true);
                struct_column->count_ = count.load();

                for (uint16_t child_index = 0; child_index < struct_column->sub_columns.size(); child_index++) {
                    auto child_path = field_path;
                    child_path.push_back(child_index);
                    load_generic_column(*struct_column->sub_columns[child_index],
                                        root_column_index,
                                        child_path,
                                        false);
                }
                return;
            }

            if (is_pax_generic_string_type(column.type())) {
                auto* standard_column = dynamic_cast<standard_column_data_t*>(&column);
                if (!standard_column) {
                    throw std::logic_error("pax_generic string column is not a standard column");
                }

                auto value_slices =
                    collect_generic_slices(root_column_index, field_path, storage::pax_generic_slice_kind::STRING_VALUES);
                if (value_slices.empty()) {
                    throw std::logic_error("missing pax_generic string slices");
                }

                const bool already_initialized =
                    is_top_level && root_column_index < pointer.columnar_data_pointers.size() &&
                    !pointer.columnar_data_pointers[root_column_index].empty();
                if (!already_initialized) {
                    persistent_column_data_t pcd(standard_column->resource());
                    pcd.data_pointers.reserve(value_slices.size());
                    for (const auto* slice : value_slices) {
                        if (slice->codec_kind != storage::pax_generic_codec_kind::STRING_SEGMENT ||
                            !slice->payload.has_value()) {
                            throw std::logic_error("invalid pax_generic string slice payload");
                        }
                        pcd.data_pointers.push_back(slice->payload->main_pointer);
                    }
                    standard_column->initialize_column(pcd);
                }

                register_string_blocks(*standard_column, value_slices);
                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path);
                apply_validity_infos(standard_column->validity, validity_infos, false);
                return;
            }

            if (is_pax_generic_fixed_plain_type(column.type())) {
                auto* standard_column = dynamic_cast<standard_column_data_t*>(&column);
                if (!standard_column) {
                    throw std::logic_error("pax_generic fixed column is not a standard column");
                }

                auto value_slices =
                    collect_generic_slices(root_column_index, field_path, storage::pax_generic_slice_kind::FIXED_VALUES);
                if (value_slices.empty()) {
                    throw std::logic_error("missing pax_generic fixed slices");
                }

                persistent_column_data_t pcd(standard_column->resource());
                pcd.data_pointers.reserve(value_slices.size());
                for (const auto* slice : value_slices) {
                    if (slice->codec_kind != storage::pax_generic_codec_kind::FIXED_PLAIN ||
                        !slice->payload.has_value()) {
                        throw std::logic_error("invalid pax_generic fixed slice payload");
                    }
                    if (pax_generic_layout_->version >= 2 && slice->fixed_logical_type != column.type().type()) {
                        throw std::logic_error("pax_generic fixed slice type mismatch");
                    }
                    pcd.data_pointers.push_back(slice->payload->main_pointer);
                }
                standard_column->initialize_column(pcd);

                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path);
                apply_validity_infos(standard_column->validity, validity_infos, false);
                return;
            }

            throw std::logic_error("unsupported column type in pax_generic load");
        };

        std::vector<bool> initialized_roots(col_count, false);
        for (const auto& page : pax_generic_layout_->pages) {
            for (const auto& slice : page.slices) {
                const auto column_index = static_cast<uint64_t>(slice.column_index);
                if (column_index >= columns_.size() || initialized_roots[column_index]) {
                    continue;
                }
                load_generic_column(*columns_[column_index],
                                    slice.column_index,
                                    empty_pax_generic_field_path(),
                                    true);
                initialized_roots[column_index] = true;
            }
        }
    }

} // namespace components::table
