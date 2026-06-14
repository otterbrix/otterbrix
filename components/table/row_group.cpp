#include "row_group.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_manager.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/types/type_spec.hpp>
#include <core/operations_helper.hpp>
#include <limits>
#include <vector/data_chunk.hpp>

#include "collection.hpp"
#include "array_column_data.hpp"
#include "list_column_data.hpp"
#include "row_version_manager.hpp"
#include "standard_column_data.hpp"
#include "struct_column_data.hpp"
#include "table_state.hpp"
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::table::detail {

    bool is_explicit_pax_columnar_only_root_type(const components::types::complex_logical_type& type) {
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::BLOB:
            case logical_type::INTERVAL:
            case logical_type::MAP:
            case logical_type::VARIANT:
                return true;
            default:
                return false;
        }
    }

} // namespace components::table::detail

namespace {

    constexpr uint32_t PAX_STRING_DICTIONARY_HEADER_SIZE = sizeof(uint32_t) * 5;
    constexpr uint32_t PAX_STRING_BIG_MARKER_SIZE = sizeof(uint32_t) + sizeof(int32_t);
    constexpr uint64_t PAX_STRING_DEFAULT_BLOCK_LIMIT = 4096;

    bool is_unprojected_placeholder(const components::vector::vector_t& vector) noexcept {
        return vector.data() == nullptr && vector.auxiliary() == nullptr;
    }

    bool is_pax_fixed_scalar_type(const components::types::complex_logical_type& type) {
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::BOOLEAN:
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
            case logical_type::BIGINT:
            case logical_type::UBIGINT:
            case logical_type::HUGEINT:
            case logical_type::UHUGEINT:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
            case logical_type::DECIMAL:
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
            case logical_type::UUID:
            case logical_type::ENUM:
                return true;
            default:
                return false;
        }
    }

    bool is_pax_fixed_projected_type(const components::types::complex_logical_type& type) {
        return is_pax_fixed_scalar_type(type);
    }

    std::string describe_pax_root_type(const components::types::complex_logical_type& type) {
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::BOOLEAN:
                return "boolean";
            case logical_type::TINYINT:
                return "tinyint";
            case logical_type::UTINYINT:
                return "utinyint";
            case logical_type::SMALLINT:
                return "smallint";
            case logical_type::USMALLINT:
                return "usmallint";
            case logical_type::INTEGER:
                return "integer";
            case logical_type::UINTEGER:
                return "uinteger";
            case logical_type::BIGINT:
                return "bigint";
            case logical_type::UBIGINT:
                return "ubigint";
            case logical_type::HUGEINT:
                return "hugeint";
            case logical_type::UHUGEINT:
                return "uhugeint";
            case logical_type::FLOAT:
                return "float";
            case logical_type::DOUBLE:
                return "double";
            case logical_type::STRING_LITERAL:
                return "string";
            case logical_type::TIMESTAMP:
                return "timestamp";
            case logical_type::TIMESTAMP_TZ:
                return "timestamptz";
            case logical_type::DATE:
                return "date";
            case logical_type::TIME:
                return "time";
            case logical_type::TIME_TZ:
                return "timetz";
            case logical_type::INTERVAL:
                return "interval";
            case logical_type::BLOB:
                return "blob";
            case logical_type::UUID:
                return "uuid";
            default: {
                auto encoded = components::types::encode_type_spec(type);
                if (!encoded.empty()) {
                    return encoded;
                }
                return "logical_type#" + std::to_string(static_cast<uint32_t>(type.type()));
            }
        }
    }

    components::table::storage::pax_fixed_column_type
    to_pax_fixed_column_type(const components::types::complex_logical_type& type) {
        using components::table::storage::pax_fixed_column_type;
        using components::types::physical_type;

        switch (type.to_physical_type()) {
            case physical_type::BOOL:
                return pax_fixed_column_type::BOOL;
            case physical_type::INT8:
                return pax_fixed_column_type::INT8;
            case physical_type::INT16:
                return pax_fixed_column_type::INT16;
            case physical_type::INT32:
                return pax_fixed_column_type::INT32;
            case physical_type::INT64:
                return pax_fixed_column_type::INT64;
            case physical_type::INT128:
                return pax_fixed_column_type::INT128;
            case physical_type::UINT8:
                return pax_fixed_column_type::UINT8;
            case physical_type::UINT16:
                return pax_fixed_column_type::UINT16;
            case physical_type::UINT32:
                return pax_fixed_column_type::UINT32;
            case physical_type::UINT64:
                return pax_fixed_column_type::UINT64;
            case physical_type::UINT128:
                return pax_fixed_column_type::UINT128;
            case physical_type::FLOAT:
                return pax_fixed_column_type::FLOAT;
            case physical_type::DOUBLE:
                return pax_fixed_column_type::DOUBLE;
            default:
                throw std::logic_error("unsupported logical type for pax_fixed column");
        }
    }

    uint64_t pax_fixed_validity_payload_size(uint64_t tuple_count) {
        return components::vector::validity_mask_t::validity_mask_size(tuple_count);
    }

    struct pax_byte_vector_less {
        bool operator()(const std::vector<std::byte>& lhs, const std::vector<std::byte>& rhs) const {
            if (lhs.size() != rhs.size()) {
                return lhs.size() < rhs.size();
            }
            if (lhs.empty()) {
                return false;
            }
            return std::memcmp(lhs.data(), rhs.data(), lhs.size()) < 0;
        }
    };

    bool pax_fixed_is_constant_data(const std::byte* data, uint64_t type_size, uint64_t count) {
        if (count <= 1) {
            return true;
        }
        const auto* base = data;
        for (uint64_t i = 1; i < count; i++) {
            if (std::memcmp(base, data + i * type_size, type_size) != 0) {
                return false;
            }
        }
        return true;
    }

    uint32_t pax_fixed_count_runs(const std::byte* data, uint64_t type_size, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        uint32_t runs = 1;
        for (uint64_t i = 1; i < count; i++) {
            if (std::memcmp(data + (i - 1) * type_size, data + i * type_size, type_size) != 0) {
                runs++;
            }
        }
        return runs;
    }

    uint64_t
    build_pax_fixed_rle_buffer(const std::byte* data, uint64_t type_size, uint64_t count, std::vector<std::byte>& out) {
        if (count == 0) {
            out.resize(sizeof(uint32_t));
            uint32_t zero = 0;
            std::memcpy(out.data(), &zero, sizeof(uint32_t));
            return sizeof(uint32_t);
        }

        const auto num_runs = pax_fixed_count_runs(data, type_size, count);
        const auto entry_size = type_size + sizeof(uint32_t);
        const auto total_size = sizeof(uint32_t) + static_cast<uint64_t>(num_runs) * entry_size;
        out.resize(total_size);

        auto* ptr = out.data();
        std::memcpy(ptr, &num_runs, sizeof(uint32_t));
        ptr += sizeof(uint32_t);

        uint32_t run_length = 1;
        for (uint64_t i = 1; i <= count; i++) {
            if (i < count && std::memcmp(data + (i - 1) * type_size, data + i * type_size, type_size) == 0) {
                run_length++;
                continue;
            }

            std::memcpy(ptr, data + (i - 1) * type_size, type_size);
            ptr += type_size;
            std::memcpy(ptr, &run_length, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            run_length = 1;
        }

        return total_size;
    }

    static constexpr uint16_t PAX_FIXED_MAX_DICT_ENTRIES = 65535;

    struct pax_fixed_dict_analysis_t {
        uint16_t num_unique{0};
        uint64_t compressed_size{0};
        std::map<std::vector<std::byte>, uint16_t, pax_byte_vector_less> value_map;
    };

    pax_fixed_dict_analysis_t pax_fixed_analyze_dictionary(const std::byte* data,
                                                           uint64_t type_size,
                                                           uint64_t count) {
        pax_fixed_dict_analysis_t result;
        if (count == 0) {
            return result;
        }

        std::map<std::vector<std::byte>, uint16_t, pax_byte_vector_less> mapping;
        for (uint64_t i = 0; i < count; i++) {
            std::vector<std::byte> key(data + i * type_size, data + (i + 1) * type_size);
            if (mapping.find(key) != mapping.end()) {
                continue;
            }
            if (mapping.size() >= PAX_FIXED_MAX_DICT_ENTRIES) {
                return result;
            }
            mapping[key] = static_cast<uint16_t>(mapping.size());
        }

        result.num_unique = static_cast<uint16_t>(mapping.size());
        const auto index_size = result.num_unique <= 256 ? uint64_t(1) : uint64_t(2);
        result.compressed_size = sizeof(uint16_t) + result.num_unique * type_size + count * index_size;
        result.value_map = std::move(mapping);
        return result;
    }

    uint64_t build_pax_fixed_dict_buffer(const std::byte* data,
                                         uint64_t type_size,
                                         uint64_t count,
                                         const pax_fixed_dict_analysis_t& analysis,
                                         std::vector<std::byte>& out) {
        out.resize(analysis.compressed_size);
        auto* ptr = out.data();

        std::memcpy(ptr, &analysis.num_unique, sizeof(uint16_t));
        ptr += sizeof(uint16_t);

        std::vector<const std::byte*> ordered(analysis.num_unique);
        for (const auto& entry : analysis.value_map) {
            ordered[entry.second] = entry.first.data();
        }
        for (uint16_t i = 0; i < analysis.num_unique; i++) {
            std::memcpy(ptr, ordered[i], type_size);
            ptr += type_size;
        }

        const bool use_uint8 = analysis.num_unique <= 256;
        for (uint64_t i = 0; i < count; i++) {
            std::vector<std::byte> key(data + i * type_size, data + (i + 1) * type_size);
            const auto dict_index = analysis.value_map.at(key);
            if (use_uint8) {
                const auto u8 = static_cast<uint8_t>(dict_index);
                std::memcpy(ptr, &u8, sizeof(uint8_t));
                ptr += sizeof(uint8_t);
            } else {
                std::memcpy(ptr, &dict_index, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
            }
        }

        return analysis.compressed_size;
    }

    components::table::storage::data_pointer_t
    write_pax_fixed_payload(uint64_t row_start,
                            const std::byte* data,
                            uint64_t type_size,
                            uint64_t tuple_count,
                            components::table::storage::partial_block_manager_t& partial_block_manager) {
        using components::table::compression::compression_type;

        const auto uncompressed_size = tuple_count * type_size;
        const std::byte* payload_data = data;
        uint64_t payload_size = uncompressed_size;
        compression_type compression = compression_type::UNCOMPRESSED;
        std::vector<std::byte> compressed_payload;

        if (tuple_count > 1 && type_size > 0 && data) {
            if (pax_fixed_is_constant_data(data, type_size, tuple_count)) {
                payload_size = type_size;
                compression = compression_type::CONSTANT;
            } else {
                const auto run_count = pax_fixed_count_runs(data, type_size, tuple_count);
                const auto rle_size = sizeof(uint32_t) + static_cast<uint64_t>(run_count) * (type_size + sizeof(uint32_t));
                if (rle_size < uncompressed_size) {
                    payload_size = build_pax_fixed_rle_buffer(data, type_size, tuple_count, compressed_payload);
                    payload_data = compressed_payload.data();
                    compression = compression_type::RLE;
                } else {
                    auto dict_info = pax_fixed_analyze_dictionary(data, type_size, tuple_count);
                    if (dict_info.num_unique > 1 && dict_info.compressed_size < uncompressed_size) {
                        payload_size =
                            build_pax_fixed_dict_buffer(data, type_size, tuple_count, dict_info, compressed_payload);
                        payload_data = compressed_payload.data();
                        compression = compression_type::DICTIONARY;
                    }
                }
            }
        }

        auto allocation = partial_block_manager.get_block_allocation(payload_size);
        if (payload_size > 0) {
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 payload_data,
                                                 payload_size);
        }

        components::table::storage::data_pointer_t pointer;
        pointer.row_start = row_start;
        pointer.tuple_count = tuple_count;
        pointer.block_pointer =
            components::table::storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
        pointer.compression = compression;
        pointer.segment_size = payload_size;
        return pointer;
    }

    bool is_supported_pax_fixed_layout_version(uint16_t version) { return version >= 1 && version <= 4; }

    bool is_supported_pax_generic_layout_version(uint16_t version) { return version >= 1 && version <= 4; }

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
        auto pointer = write_pax_fixed_payload(row_group_start + row_offset,
                                               slice.data(),
                                               static_cast<uint64_t>(column.type().size()),
                                               tuple_count,
                                               partial_block_manager);

        columnar_pointers.push_back(pointer);

        components::table::storage::pax_fixed_slice_t slice_desc;
        slice_desc.column_index = column_index;
        slice_desc.column_type = to_pax_fixed_column_type(column.type());
        slice_desc.data_pointer = pointer;
        components::table::base_statistics_t page_stats(column.resource(), column.type().type());
        page_stats.update(slice, tuple_count);
        slice_desc.statistics = std::move(page_stats);

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
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::STRUCT:
            case logical_type::UNION:
                return true;
            default:
                return false;
        }
    }

    bool is_pax_generic_collection_type(const components::types::complex_logical_type& type) {
        using components::types::logical_type;

        switch (type.type()) {
            case logical_type::LIST:
            case logical_type::ARRAY:
                return true;
            default:
                return false;
        }
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
        if (is_pax_generic_collection_type(type)) {
            return is_supported_pax_generic_column_type(type.child_type());
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

} // namespace

namespace components::table::detail {

    explicit_pax_root_kind classify_explicit_pax_root_type(const components::types::complex_logical_type& type) {
        if (is_explicit_pax_columnar_only_root_type(type)) {
            return explicit_pax_root_kind::COLUMNAR_ONLY;
        }
        if (is_pax_generic_string_type(type) || is_pax_generic_struct_type(type) || is_pax_generic_collection_type(type)) {
            return is_supported_pax_generic_column_type(type) ? explicit_pax_root_kind::GENERIC
                                                              : explicit_pax_root_kind::UNSUPPORTED;
        }
        if (is_pax_fixed_scalar_type(type)) {
            return explicit_pax_root_kind::FIXED;
        }
        return explicit_pax_root_kind::UNSUPPORTED;
    }

    bool supports_explicit_pax_schema(const std::vector<column_definition_t>& columns, std::string* error_message) {
        if (columns.empty()) {
            if (error_message) {
                *error_message = "USING PAX requires a declared schema";
            }
            return false;
        }

        for (const auto& column : columns) {
            const auto kind = classify_explicit_pax_root_type(column.type());
            if (kind == explicit_pax_root_kind::COLUMNAR_ONLY || kind == explicit_pax_root_kind::UNSUPPORTED) {
                if (error_message) {
                    *error_message = "column '" + column.name() + "' of type '" +
                                     describe_pax_root_type(column.type()) + "' is not supported by USING PAX";
                }
                return false;
            }
        }
        return true;
    }

} // namespace components::table::detail

namespace {

    uint64_t pax_string_block_limit(uint64_t block_size) {
        return std::min((block_size / 4) / 8 * 8, PAX_STRING_DEFAULT_BLOCK_LIMIT);
    }

    // Bounds-check a disk-derived [offset, offset+length) slice against the pinned block: the CRC
    // proves the bytes are intact, not that an offset/length decoded from them is in range. Checks
    // are ordered so offset+length is only evaluated once both are <= block_size (no overflow).
    inline std::byte* checked_pax_block_ptr(components::table::storage::buffer_handle_t& handle,
                                            uint64_t offset,
                                            uint64_t length,
                                            uint64_t block_size) {
        if (offset > block_size || length > block_size || offset + length > block_size) {
            throw std::logic_error("pax decode: block slice out of bounds (corrupt on-disk pointer)");
        }
        return handle.ptr() + offset;
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

    void append_unique_block_id(std::vector<uint64_t>& block_ids, uint64_t block_id) {
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
        std::optional<components::table::base_statistics_t> statistics;
    };

    struct pax_generic_fixed_page_write_result_t {
        components::table::storage::data_pointer_t main_pointer;
        components::table::storage::pax_generic_codec_kind validity_codec{
            components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID};
        std::optional<components::table::storage::data_pointer_t> validity_pointer;
        std::optional<components::table::base_statistics_t> statistics;
    };

    struct pax_generic_validity_write_result_t {
        components::table::storage::pax_generic_codec_kind validity_codec{
            components::table::storage::pax_generic_codec_kind::VALIDITY_ALL_VALID};
        std::optional<components::table::storage::data_pointer_t> validity_pointer;
    };

    struct pax_generic_plain_payload_write_result_t {
        components::table::storage::data_pointer_t main_pointer;
    };

    pax_generic_plain_payload_write_result_t
    write_pax_generic_plain_payload(uint64_t absolute_row_start,
                                    uint32_t tuple_count,
                                    const std::byte* data,
                                    uint64_t type_size,
                                    components::table::storage::partial_block_manager_t& partial_block_manager) {
        using components::table::compression::compression_type;
        using components::table::storage::block_pointer_t;

        const auto payload_size = static_cast<uint64_t>(tuple_count) * type_size;
        auto allocation = partial_block_manager.get_block_allocation(payload_size);
        if (payload_size > 0) {
            partial_block_manager.write_to_block(allocation.block_id, allocation.offset_in_block, data, payload_size);
        }

        pax_generic_plain_payload_write_result_t result;
        result.main_pointer.row_start = absolute_row_start;
        result.main_pointer.tuple_count = tuple_count;
        result.main_pointer.block_pointer = block_pointer_t(allocation.block_id, allocation.offset_in_block);
        result.main_pointer.compression = compression_type::UNCOMPRESSED;
        result.main_pointer.segment_size = payload_size;
        return result;
    }

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
        components::table::base_statistics_t page_stats(column.resource(), column.type().type());
        page_stats.update(slice, tuple_count);
        result.statistics = std::move(page_stats);

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
        components::table::base_statistics_t page_stats(column.resource(), column.type().type());
        page_stats.update(slice, tuple_count);
        result.statistics = std::move(page_stats);

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

        if (is_pax_generic_collection_type(column.type())) {
            auto validity_result =
                write_pax_generic_validity_page(column, row_group_start, row_offset, tuple_count, partial_block_manager);
            append_pax_generic_validity_slice(page, root_column_index, field_path, validity_result);

            if (column.type().type() == logical_type::LIST) {
                auto& list_column = dynamic_cast<components::table::list_column_data_t&>(column);
                const auto page_row_start = static_cast<int64_t>(row_group_start + row_offset);
                std::vector<uint64_t> offsets(tuple_count, 0);
                for (uint32_t i = 0; i < tuple_count; i++) {
                    offsets[i] = list_column.list_offset(page_row_start + static_cast<int64_t>(i));
                }

                auto offsets_payload = write_pax_generic_plain_payload(row_group_start + row_offset,
                                                                       tuple_count,
                                                                       reinterpret_cast<const std::byte*>(offsets.data()),
                                                                       sizeof(uint64_t),
                                                                       partial_block_manager);

                components::table::storage::pax_generic_slice_t offsets_slice;
                offsets_slice.column_index = root_column_index;
                offsets_slice.slice_kind = pax_generic_slice_kind::FIXED_VALUES;
                offsets_slice.codec_kind = pax_generic_codec_kind::FIXED_PLAIN;
                offsets_slice.field_path = field_path;
                offsets_slice.fixed_logical_type = logical_type::UBIGINT;
                offsets_slice.payload = pax_block_payload_t{offsets_payload.main_pointer, {}};
                page.slices.push_back(std::move(offsets_slice));

                const auto previous_offset =
                    page_row_start == list_column.start() ? 0 : list_column.list_offset(page_row_start - 1);
                const auto child_tuple_count =
                    offsets.empty() ? 0 : static_cast<uint32_t>(offsets.back() - previous_offset);
                if (child_tuple_count > 0) {
                    auto child_path = field_path;
                    child_path.push_back(0);
                    write_pax_generic_column_page(*list_column.child_column,
                                                  root_column_index,
                                                  child_path,
                                                  row_group_start + previous_offset,
                                                  0,
                                                  child_tuple_count,
                                                  partial_block_manager,
                                                  page);
                }
                return;
            }

            auto& array_column = dynamic_cast<components::table::array_column_data_t&>(column);
            auto child_path = field_path;
            child_path.push_back(0);
            const auto child_tuple_count = static_cast<uint32_t>(static_cast<uint64_t>(tuple_count) * array_column.array_size());
            if (child_tuple_count > 0) {
                const auto child_row_start =
                    row_group_start + static_cast<uint64_t>(row_offset) * static_cast<uint64_t>(array_column.array_size());
                write_pax_generic_column_page(*array_column.child_column,
                                              root_column_index,
                                              child_path,
                                              child_row_start,
                                              0,
                                              child_tuple_count,
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
            value_slice.statistics = std::move(page_result.statistics);
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
            value_slice.statistics = std::move(page_result.statistics);
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
            case compare_type::is_null:
            case compare_type::is_not_null:
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
        if (slice.data_pointer.tuple_count != tuple_count) {
            return false;
        }

        using components::table::compression::compression_type;
        switch (slice.data_pointer.compression) {
            case compression_type::UNCOMPRESSED:
                if (slice.data_pointer.segment_size != static_cast<uint64_t>(tuple_count) * type_size) {
                    return false;
                }
                break;
            case compression_type::CONSTANT:
                if (slice.data_pointer.segment_size != type_size) {
                    return false;
                }
                break;
            case compression_type::RLE: {
                const auto entry_size = type_size + sizeof(uint32_t);
                if (slice.data_pointer.segment_size < sizeof(uint32_t) ||
                    (slice.data_pointer.segment_size - sizeof(uint32_t)) % entry_size != 0) {
                    return false;
                }
                break;
            }
            case compression_type::DICTIONARY:
                if (slice.data_pointer.segment_size < sizeof(uint16_t) + type_size + tuple_count) {
                    return false;
                }
                break;
            default:
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

    bool validate_pax_generic_fixed_slice(const components::table::storage::pax_generic_slice_t& slice,
                                          uint32_t tuple_count,
                                          const components::types::complex_logical_type& type) {
        if (slice.slice_kind != components::table::storage::pax_generic_slice_kind::FIXED_VALUES ||
            slice.codec_kind != components::table::storage::pax_generic_codec_kind::FIXED_PLAIN ||
            !slice.payload.has_value() || !slice.payload->extra_block_ids.empty()) {
            return false;
        }
        if (slice.fixed_logical_type != components::types::logical_type::INVALID &&
            slice.fixed_logical_type != type.type()) {
            return false;
        }

        const auto& pointer = slice.payload->main_pointer;
        return pointer.compression == components::table::compression::compression_type::UNCOMPRESSED &&
               pointer.tuple_count == tuple_count &&
               pointer.segment_size == static_cast<uint64_t>(tuple_count) * static_cast<uint64_t>(type.size());
    }

    bool is_pax_generic_root_projected_type(const components::types::complex_logical_type& type) {
        return is_pax_generic_string_type(type) || is_pax_generic_fixed_plain_type(type);
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

    template<typename Page>
    struct pax_page_window_t {
        const Page* page{nullptr};
        uint64_t overlap_start{0};
        uint64_t overlap_end{0};
        uint64_t page_count{0};
        uint64_t page_offset_in_window{0};
    };

    template<typename Page>
    std::vector<pax_page_window_t<Page>>
    collect_pax_page_windows(const std::vector<Page>& pages, uint64_t window_start, uint64_t window_end) {
        std::vector<pax_page_window_t<Page>> windows;
        for (const auto& page : pages) {
            const auto page_start = static_cast<uint64_t>(page.row_offset_in_group);
            const auto page_end = page_start + static_cast<uint64_t>(page.tuple_count);
            if (page_end <= window_start || page_start >= window_end) {
                continue;
            }

            const auto overlap_start = std::max(window_start, page_start);
            const auto overlap_end = std::min(window_end, page_end);
            windows.push_back({&page,
                               overlap_start,
                               overlap_end,
                               overlap_end - overlap_start,
                               overlap_start - window_start});
        }
        return windows;
    }

    void collect_pax_fixed_slice_blocks(const components::table::storage::pax_fixed_slice_t& slice,
                                        std::vector<uint64_t>& block_ids) {
        append_unique_block_id(block_ids, slice.data_pointer.block_pointer.block_id);
        if (slice.validity_data_pointer.has_value()) {
            append_unique_block_id(block_ids, slice.validity_data_pointer->block_pointer.block_id);
        }
    }

    bool collect_pax_fixed_page_column_blocks(const components::table::storage::pax_fixed_page_t& page,
                                              uint32_t column_index,
                                              std::vector<uint64_t>& block_ids) {
        const auto* slice = find_pax_fixed_slice(page, column_index);
        if (!slice) {
            return false;
        }
        collect_pax_fixed_slice_blocks(*slice, block_ids);
        return true;
    }

    bool collect_pax_generic_string_column_blocks(const components::table::storage::pax_generic_page_t& page,
                                                  uint32_t column_index,
                                                  std::vector<uint64_t>& block_ids) {
        using components::table::storage::pax_generic_codec_kind;
        using components::table::storage::pax_generic_slice_kind;

        const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::STRING_VALUES);
        const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
        if (!value_slice || !validity_slice || !value_slice->payload.has_value()) {
            return false;
        }

        append_unique_block_id(block_ids, value_slice->payload->main_pointer.block_pointer.block_id);
        for (auto block_id : value_slice->payload->extra_block_ids) {
            append_unique_block_id(block_ids, static_cast<uint64_t>(block_id));
        }

        if (validity_slice->codec_kind == pax_generic_codec_kind::VALIDITY_BITMASK) {
            if (!validity_slice->payload.has_value()) {
                return false;
            }
            append_unique_block_id(block_ids, validity_slice->payload->main_pointer.block_pointer.block_id);
        }
        return true;
    }

    bool collect_pax_generic_fixed_column_blocks(const components::table::storage::pax_generic_page_t& page,
                                                 uint32_t column_index,
                                                 const components::types::complex_logical_type& type,
                                                 std::vector<uint64_t>& block_ids) {
        using components::table::storage::pax_generic_codec_kind;
        using components::table::storage::pax_generic_slice_kind;

        const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::FIXED_VALUES);
        const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
        if (!value_slice || !validity_slice || !validate_pax_generic_fixed_slice(*value_slice, page.tuple_count, type) ||
            !validate_pax_generic_validity_slice(*validity_slice, page.tuple_count)) {
            return false;
        }

        append_unique_block_id(block_ids, value_slice->payload->main_pointer.block_pointer.block_id);
        if (validity_slice->codec_kind == pax_generic_codec_kind::VALIDITY_BITMASK) {
            if (!validity_slice->payload.has_value()) {
                return false;
            }
            append_unique_block_id(block_ids, validity_slice->payload->main_pointer.block_pointer.block_id);
        }
        return true;
    }

    bool collect_pax_generic_root_column_blocks(const components::table::storage::pax_generic_page_t& page,
                                                uint32_t column_index,
                                                const components::types::complex_logical_type& type,
                                                std::vector<uint64_t>& block_ids) {
        if (is_pax_generic_string_type(type)) {
            return collect_pax_generic_string_column_blocks(page, column_index, block_ids);
        }
        if (is_pax_generic_fixed_plain_type(type)) {
            return collect_pax_generic_fixed_column_blocks(page, column_index, type, block_ids);
        }
        return false;
    }

    bool validate_pax_generic_root_column_slices(const components::table::storage::pax_generic_page_t& page,
                                                 uint32_t column_index,
                                                 const components::types::complex_logical_type& type) {
        using components::table::storage::pax_generic_slice_kind;

        if (is_pax_generic_string_type(type)) {
            const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::STRING_VALUES);
            const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
            return value_slice && validity_slice &&
                   validate_pax_generic_string_slice(*value_slice, page.tuple_count) &&
                   validate_pax_generic_validity_slice(*validity_slice, page.tuple_count);
        }
        if (is_pax_generic_fixed_plain_type(type)) {
            const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::FIXED_VALUES);
            const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
            return value_slice && validity_slice &&
                   validate_pax_generic_fixed_slice(*value_slice, page.tuple_count, type) &&
                   validate_pax_generic_validity_slice(*validity_slice, page.tuple_count);
        }
        return false;
    }

    template<typename BlockCache>
    uint64_t prefetch_and_pin_pax_blocks(components::table::row_group_t& row_group,
                                         const std::vector<uint64_t>& block_ids,
                                         BlockCache& block_cache) {
        std::vector<std::shared_ptr<components::table::storage::block_handle_t>> handles;
        handles.reserve(block_ids.size());

        for (auto block_id : block_ids) {
            if (block_cache.find(block_id) != block_cache.end()) {
                continue;
            }
            bool already_planned = false;
            for (const auto& handle : handles) {
                if (handle->block_id() == block_id) {
                    already_planned = true;
                    break;
                }
            }
            if (!already_planned) {
                handles.push_back(row_group.block_manager().register_block(block_id));
            }
        }

        if (handles.empty()) {
            return 0;
        }

        row_group.block_manager().buffer_manager.prefetch(handles);
        uint64_t pinned_count = 0;
        for (auto& block : handles) {
            const auto block_id = block->block_id();
            if (block_cache.find(block_id) != block_cache.end()) {
                continue;
            }
            auto handle = row_group.block_manager().buffer_manager.pin(block);
            handle.set_ownership(block);
            block_cache.emplace(block_id, std::move(handle));
            pinned_count++;
        }
        return pinned_count;
    }

    void mark_vector_range_valid(components::vector::vector_t& result, uint64_t offset, uint64_t count) {
        auto& validity = result.validity();
        for (uint64_t i = 0; i < count; i++) {
            validity.set(offset + i, true);
        }
    }

    std::vector<uint8_t> build_pax_visibility_mask(const components::vector::indexing_vector_t& visible_indexing,
                                                   uint64_t visible_count,
                                                   uint64_t max_count) {
        std::vector<uint8_t> mask(max_count, 0);
        for (uint64_t i = 0; i < visible_count; i++) {
            const auto row_offset = visible_indexing.get_index(i);
            assert(row_offset < max_count);
            mask[row_offset] = 1;
        }
        return mask;
    }

    uint64_t apply_pax_visibility_mask(const std::vector<uint8_t>& visible_mask,
                                       uint64_t page_offset_in_window,
                                       components::vector::indexing_vector_t& page_indexing,
                                       uint64_t page_approved_count) {
        if (visible_mask.empty()) {
            return page_approved_count;
        }

        uint64_t result_count = 0;
        for (uint64_t i = 0; i < page_approved_count; i++) {
            const auto page_row_offset = page_indexing.get_index(i);
            const auto window_row_offset = page_offset_in_window + page_row_offset;
            assert(window_row_offset < visible_mask.size());
            if (visible_mask[window_row_offset] != 0) {
                page_indexing.set_index(result_count++, page_row_offset);
            }
        }
        return result_count;
    }

    bool fill_projected_row_id_columns(const std::vector<components::table::storage_index_t>& column_ids,
                                       components::vector::data_chunk_t& result,
                                       uint64_t result_offset,
                                       uint64_t count) {
        auto* row_ids = result.row_ids.data<int64_t>();
        for (uint64_t column_pos = 0; column_pos < column_ids.size(); column_pos++) {
            if (!column_ids[column_pos].is_row_id_column()) {
                continue;
            }
            if (column_pos >= result.data.size()) {
                return false;
            }
            if (is_unprojected_placeholder(result.data[column_pos])) {
                continue;
            }
            if (!result.data[column_pos].data() || result.data[column_pos].type().type() !=
                                                       components::types::logical_type::BIGINT) {
                return false;
            }
            result.data[column_pos].set_vector_type(components::vector::vector_type::FLAT);
            auto* output = result.data[column_pos].data<int64_t>();
            for (uint64_t i = 0; i < count; i++) {
                output[result_offset + i] = row_ids[result_offset + i];
            }
            mark_vector_range_valid(result.data[column_pos], result_offset, count);
        }
        return true;
    }

    void apply_pax_committed_updates(components::table::column_data_t& column_data,
                                     uint64_t row_offset_in_group,
                                     uint64_t count,
                                     components::vector::vector_t& result,
                                     uint64_t result_offset = 0) {
        if (count == 0) {
            return;
        }
        if (!column_data.has_updates()) {
            return;
        }
        column_data.fetch_committed_updates_range(row_offset_in_group, count, result, result_offset);
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
        if (state.row_offset_override_active) {
            if (state.vector_index_relative_to_row_group) {
                return state.row_offset_override;
            }
            if (state.row_offset_override < static_cast<uint64_t>(row_group.start)) {
                return 0;
            }
            return state.row_offset_override - static_cast<uint64_t>(row_group.start);
        }

        for (const auto& column_state : state.column_scans) {
            if (column_state.current) {
                return static_cast<uint64_t>(column_state.row_index - row_group.start);
            }
        }

        uint64_t local_vector_index = state.vector_index;
        if (!state.vector_index_relative_to_row_group) {
            const auto start_vector =
                static_cast<uint64_t>(row_group.start) / components::vector::DEFAULT_VECTOR_CAPACITY;
            local_vector_index -= start_vector;
        }
        return local_vector_index * components::vector::DEFAULT_VECTOR_CAPACITY;
    }

    void set_pax_scan_row_offset(components::table::row_group_t& row_group,
                                 components::table::collection_scan_state& state,
                                 uint64_t row_offset_in_group) {
        const auto absolute_row_offset = static_cast<uint64_t>(row_group.start) + row_offset_in_group;
        state.vector_index =
            (state.vector_index_relative_to_row_group ? row_offset_in_group : absolute_row_offset) /
            components::vector::DEFAULT_VECTOR_CAPACITY;
        state.row_offset_override_active = true;
        state.row_offset_override =
            state.vector_index_relative_to_row_group ? row_offset_in_group : absolute_row_offset;
    }

    uint64_t current_local_vector_index(const components::table::row_group_t& row_group,
                                        const components::table::collection_scan_state& state) {
        uint64_t local_vector_index = state.vector_index;
        if (!state.vector_index_relative_to_row_group) {
            const auto start_vector =
                static_cast<uint64_t>(row_group.start) / components::vector::DEFAULT_VECTOR_CAPACITY;
            local_vector_index -= start_vector;
        }
        return local_vector_index;
    }

    uint64_t current_version_vector_index(const components::table::row_group_t& row_group,
                                          const components::table::collection_scan_state& state) {
        if (!state.vector_index_relative_to_row_group) {
            return state.vector_index;
        }
        const auto start_vector =
            static_cast<uint64_t>(row_group.start) / components::vector::DEFAULT_VECTOR_CAPACITY;
        return start_vector + state.vector_index;
    }

    int64_t current_regular_row_id_base(const components::table::row_group_t& row_group,
                                        const components::table::collection_scan_state& state) {
        for (const auto& column_state : state.column_scans) {
            if (column_state.current) {
                return column_state.row_index;
            }
        }

        const auto local_vector_index = current_local_vector_index(row_group, state);
        return row_group.start +
               static_cast<int64_t>(local_vector_index * components::vector::DEFAULT_VECTOR_CAPACITY);
    }

    // A PAX validity bitmask sits at an arbitrary, possibly-unaligned byte offset in its block, but
    // validity_mask_t reads it as uint64_t (misaligned load is UB). Copy it into the 8-aligned `out`.
    // The returned mask aliases out.data(), so `out` must outlive every use of the mask.
    inline void copy_aligned_pax_validity(const std::byte* raw, uint64_t byte_size, std::vector<uint64_t>& out) {
        out.assign(static_cast<size_t>((byte_size + sizeof(uint64_t) - 1) / sizeof(uint64_t)), 0);
        std::memcpy(out.data(), raw, static_cast<size_t>(byte_size));
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
                std::vector<uint64_t> aligned; // must outlive source_mask (which aliases it)
                copy_aligned_pax_validity(checked_pax_block_ptr(block_handle,
                                                                validity_pointer.block_pointer.offset,
                                                                validity_pointer.segment_size,
                                                                row_group.block_manager().block_size()),
                                          validity_pointer.segment_size,
                                          aligned);
                components::vector::validity_mask_t source_mask(result.resource(), aligned.data());
                result.validity().slice_in_place(source_mask, result_offset, page_row_offset, copy_count);
                return true;
            }
            case pax_fixed_validity_kind::RLE:
            default:
                return false;
        }
    }

    // Broadcast a single fixed-width value into `count` contiguous slots at `dst`. Common
    // power-of-two widths use a typed std::fill; other widths fall back to a memcpy loop.
    inline void fill_fixed_value(std::byte* dst, const std::byte* value, uint64_t type_size, uint64_t count) {
        switch (type_size) {
            case 1:
                std::memset(dst, static_cast<int>(std::to_integer<unsigned char>(value[0])), count);
                return;
            case 2: {
                uint16_t v;
                std::memcpy(&v, value, sizeof(v));
                std::fill_n(reinterpret_cast<uint16_t*>(dst), count, v);
                return;
            }
            case 4: {
                uint32_t v;
                std::memcpy(&v, value, sizeof(v));
                std::fill_n(reinterpret_cast<uint32_t*>(dst), count, v);
                return;
            }
            case 8: {
                uint64_t v;
                std::memcpy(&v, value, sizeof(v));
                std::fill_n(reinterpret_cast<uint64_t*>(dst), count, v);
                return;
            }
            case 16: {
                __uint128_t v;
                std::memcpy(&v, value, sizeof(v));
                std::fill_n(reinterpret_cast<__uint128_t*>(dst), count, v);
                return;
            }
            default:
                for (uint64_t i = 0; i < count; i++) {
                    std::memcpy(dst + i * type_size, value, type_size);
                }
                return;
        }
    }

    bool decode_pax_fixed_uncompressed_window(const components::table::storage::data_pointer_t& pointer,
                                              const std::byte* source,
                                              uint64_t type_size,
                                              uint64_t page_tuple_count,
                                              uint64_t page_row_offset,
                                              uint64_t copy_count,
                                              components::vector::vector_t& result,
                                              uint64_t result_offset) {
        if (pointer.segment_size != page_tuple_count * type_size) {
            return false;
        }

        const auto byte_offset = page_row_offset * type_size;
        const auto byte_count = copy_count * type_size;
        auto* target_ptr = result.data() + result_offset * type_size;
        std::memcpy(target_ptr, source + byte_offset, byte_count);
        return true;
    }

    bool decode_pax_fixed_constant_window(const components::table::storage::data_pointer_t& pointer,
                                          const std::byte* source,
                                          uint64_t type_size,
                                          uint64_t copy_count,
                                          components::vector::vector_t& result,
                                          uint64_t result_offset) {
        if (pointer.segment_size != type_size) {
            return false;
        }

        auto* target_ptr = result.data() + result_offset * type_size;
        fill_fixed_value(target_ptr, source, type_size, copy_count);
        return true;
    }

    bool decode_pax_fixed_rle_window(const components::table::storage::data_pointer_t& pointer,
                                     const std::byte* source,
                                     uint64_t type_size,
                                     uint64_t page_tuple_count,
                                     uint64_t page_row_offset,
                                     uint64_t copy_count,
                                     components::vector::vector_t& result,
                                     uint64_t result_offset) {
        if (pointer.segment_size < sizeof(uint32_t)) {
            return false;
        }

        uint32_t run_count = 0;
        std::memcpy(&run_count, source, sizeof(uint32_t));
        const auto entry_size = type_size + sizeof(uint32_t);
        const auto expected_size = sizeof(uint32_t) + static_cast<uint64_t>(run_count) * entry_size;
        if (pointer.segment_size != expected_size) {
            return false;
        }

        const auto window_end = page_row_offset + copy_count;
        auto* target_ptr = result.data() + result_offset * type_size;
        const auto* ptr = source + sizeof(uint32_t);
        uint64_t logical_offset = 0;
        uint64_t copied_rows = 0;

        for (uint32_t run = 0; run < run_count; run++) {
            const auto* value_ptr = ptr;
            ptr += type_size;

            uint32_t run_length = 0;
            std::memcpy(&run_length, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            if (run_length == 0) {
                return false;
            }

            const auto run_start = logical_offset;
            const auto run_end = run_start + static_cast<uint64_t>(run_length);
            if (run_end < run_start || run_end > page_tuple_count) {
                return false;
            }

            const auto overlap_start = std::max(page_row_offset, run_start);
            const auto overlap_end = std::min(window_end, run_end);
            if (overlap_start < overlap_end) {
                const auto output_start = overlap_start - page_row_offset;
                const auto output_count = overlap_end - overlap_start;
                fill_fixed_value(target_ptr + output_start * type_size, value_ptr, type_size, output_count);
                copied_rows += output_count;
            }

            logical_offset = run_end;
        }

        return logical_offset == page_tuple_count && copied_rows == copy_count;
    }

    bool decode_pax_fixed_dictionary_window(const components::table::storage::data_pointer_t& pointer,
                                            const std::byte* source,
                                            uint64_t type_size,
                                            uint64_t page_tuple_count,
                                            uint64_t page_row_offset,
                                            uint64_t copy_count,
                                            components::vector::vector_t& result,
                                            uint64_t result_offset) {
        if (pointer.segment_size < sizeof(uint16_t)) {
            return false;
        }

        uint16_t num_unique = 0;
        std::memcpy(&num_unique, source, sizeof(uint16_t));
        if (num_unique == 0) {
            return false;
        }

        const auto index_size = num_unique <= 256 ? uint64_t(1) : uint64_t(2);
        const auto dictionary_bytes = static_cast<uint64_t>(num_unique) * type_size;
        const auto expected_size = sizeof(uint16_t) + dictionary_bytes + page_tuple_count * index_size;
        if (pointer.segment_size != expected_size) {
            return false;
        }

        const auto* dictionary_values = source + sizeof(uint16_t);
        const auto* indices = dictionary_values + dictionary_bytes;
        auto* target_ptr = result.data() + result_offset * type_size;

        // Hoist the index-width branch out of the gather loop so each width is its own typed-store loop.
        if (index_size == 1) {
            const auto* idx = reinterpret_cast<const uint8_t*>(indices) + page_row_offset;
            for (uint64_t i = 0; i < copy_count; i++) {
                const uint16_t dict_index = idx[i];
                if (dict_index >= num_unique) {
                    return false;
                }
                std::memcpy(target_ptr + i * type_size,
                            dictionary_values + static_cast<uint64_t>(dict_index) * type_size,
                            type_size);
            }
        } else {
            const auto* idx = indices + page_row_offset * sizeof(uint16_t);
            for (uint64_t i = 0; i < copy_count; i++) {
                uint16_t dict_index = 0;
                std::memcpy(&dict_index, idx + i * sizeof(uint16_t), sizeof(uint16_t));
                if (dict_index >= num_unique) {
                    return false;
                }
                std::memcpy(target_ptr + i * type_size,
                            dictionary_values + static_cast<uint64_t>(dict_index) * type_size,
                            type_size);
            }
        }
        return true;
    }

    bool decode_pax_fixed_values_window(components::table::row_group_t& row_group,
                                        const components::table::storage::pax_fixed_slice_t& slice,
                                        uint64_t type_size,
                                        uint64_t page_tuple_count,
                                        uint64_t page_row_offset,
                                        uint64_t copy_count,
                                        components::vector::vector_t& result,
                                        uint64_t result_offset,
                                        pax_fixed_block_cache_t& block_cache) {
        using components::table::compression::compression_type;

        const auto& pointer = slice.data_pointer;
        if (pointer.tuple_count != page_tuple_count) {
            return false;
        }

        auto& block_handle = get_or_pin_pax_fixed_block(row_group, pointer.block_pointer.block_id, block_cache);
        const auto* source = checked_pax_block_ptr(block_handle,
                                                   pointer.block_pointer.offset,
                                                   pointer.segment_size,
                                                   row_group.block_manager().block_size());

        switch (pointer.compression) {
            case compression_type::UNCOMPRESSED:
                return decode_pax_fixed_uncompressed_window(pointer,
                                                            source,
                                                            type_size,
                                                            page_tuple_count,
                                                            page_row_offset,
                                                            copy_count,
                                                            result,
                                                            result_offset);
            case compression_type::CONSTANT:
                return decode_pax_fixed_constant_window(pointer, source, type_size, copy_count, result, result_offset);
            case compression_type::RLE:
                return decode_pax_fixed_rle_window(pointer,
                                                   source,
                                                   type_size,
                                                   page_tuple_count,
                                                   page_row_offset,
                                                   copy_count,
                                                   result,
                                                   result_offset);
            case compression_type::DICTIONARY:
                return decode_pax_fixed_dictionary_window(pointer,
                                                          source,
                                                          type_size,
                                                          page_tuple_count,
                                                          page_row_offset,
                                                          copy_count,
                                                          result,
                                                          result_offset);
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

            const auto overlap_start = std::max(window_row_offset, page_start);
            const auto overlap_end = std::min(window_end, page_end);
            const auto copy_count = overlap_end - overlap_start;
            const auto page_row_offset = overlap_start - page_start;
            const auto window_result_offset = result_offset + (overlap_start - window_row_offset);

            if (!decode_pax_fixed_values_window(row_group,
                                                *slice,
                                                type_size,
                                                page.tuple_count,
                                                page_row_offset,
                                                copy_count,
                                                result,
                                                window_result_offset,
                                                block_cache)) {
                return false;
            }
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
    bool apply_compare(components::expressions::compare_type type, const T& lhs, const T& rhs) {
        using components::expressions::compare_type;

        switch (type) {
            case compare_type::eq:
                if constexpr (std::is_floating_point_v<T>) {
                    return core::is_equals(lhs, rhs);
                } else {
                    return lhs == rhs;
                }
            case compare_type::ne:
                if constexpr (std::is_floating_point_v<T>) {
                    return !core::is_equals(lhs, rhs);
                } else {
                    return lhs != rhs;
                }
            case compare_type::gt:
                return lhs > rhs;
            case compare_type::gte:
                return lhs >= rhs;
            case compare_type::lt:
                return lhs < rhs;
            case compare_type::lte:
                return lhs <= rhs;
            default:
                return false;
        }
    }

    components::table::filter_propagate_result_t
    check_pax_statistics(const std::optional<components::table::base_statistics_t>& statistics,
                         uint32_t tuple_count,
                         const components::table::table_filter_t& filter) {
        using components::expressions::compare_type;
        using components::table::filter_propagate_result_t;

        if (!statistics.has_value()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        const auto& stats = *statistics;
        if (filter.filter_type == compare_type::is_null) {
            if (stats.null_count() == 0) {
                return filter_propagate_result_t::ALWAYS_FALSE;
            }
            if (stats.null_count() == tuple_count) {
                return filter_propagate_result_t::ALWAYS_TRUE;
            }
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        if (filter.filter_type == compare_type::is_not_null) {
            if (stats.null_count() == tuple_count) {
                return filter_propagate_result_t::ALWAYS_FALSE;
            }
            if (stats.null_count() == 0) {
                return filter_propagate_result_t::ALWAYS_TRUE;
            }
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        if (stats.null_count() == tuple_count) {
            return filter_propagate_result_t::ALWAYS_FALSE;
        }
        if (!stats.has_stats() || stats.min_value().is_null() || stats.max_value().is_null()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        if (filter.filter_type != compare_type::eq && filter.filter_type != compare_type::ne &&
            filter.filter_type != compare_type::gt && filter.filter_type != compare_type::gte &&
            filter.filter_type != compare_type::lt && filter.filter_type != compare_type::lte) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        const auto& constant_filter = filter.cast<components::table::constant_filter_t>();
        const auto& constant = constant_filter.constant;
        const auto& min = stats.min_value();
        const auto& max = stats.max_value();

        switch (filter.filter_type) {
            case compare_type::eq:
                if (constant < min || constant > max) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                if (stats.null_count() == 0 && min == constant && max == constant) {
                    return filter_propagate_result_t::ALWAYS_TRUE;
                }
                break;
            case compare_type::ne:
                if (min == constant && max == constant) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                break;
            case compare_type::gt:
                if (max <= constant) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                if (stats.null_count() == 0 && min > constant) {
                    return filter_propagate_result_t::ALWAYS_TRUE;
                }
                break;
            case compare_type::gte:
                if (max < constant) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                if (stats.null_count() == 0 && min >= constant) {
                    return filter_propagate_result_t::ALWAYS_TRUE;
                }
                break;
            case compare_type::lt:
                if (min >= constant) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                if (stats.null_count() == 0 && max < constant) {
                    return filter_propagate_result_t::ALWAYS_TRUE;
                }
                break;
            case compare_type::lte:
                if (min > constant) {
                    return filter_propagate_result_t::ALWAYS_FALSE;
                }
                if (stats.null_count() == 0 && max <= constant) {
                    return filter_propagate_result_t::ALWAYS_TRUE;
                }
                break;
            default:
                break;
        }

        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    components::table::filter_propagate_result_t
    check_pax_fixed_page_statistics(const components::table::storage::pax_fixed_page_t& page,
                                    uint32_t column_index,
                                    const components::table::table_filter_t& filter) {
        const auto* slice = find_pax_fixed_slice(page, column_index);
        if (!slice) {
            return components::table::filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        return check_pax_statistics(slice->statistics, page.tuple_count, filter);
    }

    components::table::filter_propagate_result_t
    check_pax_generic_page_statistics(const components::table::storage::pax_generic_page_t& page,
                                      uint32_t column_index,
                                      const components::table::table_filter_t& filter) {
        const auto* slice = find_pax_generic_slice(page,
                                                   column_index,
                                                   components::table::storage::pax_generic_slice_kind::STRING_VALUES);
        if (!slice) {
            slice = find_pax_generic_slice(page,
                                           column_index,
                                           components::table::storage::pax_generic_slice_kind::FIXED_VALUES);
        }
        if (!slice) {
            return components::table::filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        return check_pax_statistics(slice->statistics, page.tuple_count, filter);
    }

    components::table::filter_propagate_result_t check_pax_generic_page_statistics(
        const components::table::storage::pax_generic_page_t& page,
        uint32_t column_index,
        const std::vector<const components::table::table_filter_t*>& filters) {
        using components::table::filter_propagate_result_t;

        if (filters.empty()) {
            return filter_propagate_result_t::ALWAYS_TRUE;
        }

        bool all_true = true;
        for (const auto* filter : filters) {
            const auto result = check_pax_generic_page_statistics(page, column_index, *filter);
            if (result == filter_propagate_result_t::ALWAYS_FALSE) {
                return filter_propagate_result_t::ALWAYS_FALSE;
            }
            if (result != filter_propagate_result_t::ALWAYS_TRUE) {
                all_true = false;
            }
        }
        return all_true ? filter_propagate_result_t::ALWAYS_TRUE
                        : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    template<typename T>
    uint64_t apply_pax_fixed_constant_filter_value(const components::vector::vector_t& values,
                                                   components::expressions::compare_type compare_type,
                                                   const T& constant_value,
                                                   uint64_t count,
                                                   components::vector::indexing_vector_t& indexing) {
        const auto* data = values.data<T>();
        const auto& validity = values.validity();
        uint64_t approved_count = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (!validity.row_is_valid(i)) {
                continue;
            }
            if (apply_compare(compare_type, data[i], constant_value)) {
                indexing.set_index(approved_count++, i);
            }
        }
        return approved_count;
    }

    template<typename T>
    uint64_t apply_pax_fixed_constant_filter_typed(const components::vector::vector_t& values,
                                                   const components::table::constant_filter_t& filter,
                                                   uint64_t count,
                                                   components::vector::indexing_vector_t& indexing) {
        return apply_pax_fixed_constant_filter_value(values,
                                                     filter.filter_type,
                                                     filter.constant.value<T>(),
                                                     count,
                                                     indexing);
    }

    uint64_t apply_pax_fixed_constant_filter(const components::vector::vector_t& values,
                                             const components::table::constant_filter_t& filter,
                                             uint64_t count,
                                             components::vector::indexing_vector_t& indexing) {
        using components::types::logical_type;

        switch (values.type().type()) {
            case logical_type::BOOLEAN:
                return apply_pax_fixed_constant_filter_typed<bool>(values, filter, count, indexing);
            case logical_type::TINYINT:
                return apply_pax_fixed_constant_filter_typed<int8_t>(values, filter, count, indexing);
            case logical_type::UTINYINT:
                return apply_pax_fixed_constant_filter_typed<uint8_t>(values, filter, count, indexing);
            case logical_type::SMALLINT:
                return apply_pax_fixed_constant_filter_typed<int16_t>(values, filter, count, indexing);
            case logical_type::USMALLINT:
                return apply_pax_fixed_constant_filter_typed<uint16_t>(values, filter, count, indexing);
            case logical_type::INTEGER:
            case logical_type::ENUM:
                return apply_pax_fixed_constant_filter_typed<int32_t>(values, filter, count, indexing);
            case logical_type::UINTEGER:
                return apply_pax_fixed_constant_filter_typed<uint32_t>(values, filter, count, indexing);
            case logical_type::BIGINT:
                return apply_pax_fixed_constant_filter_typed<int64_t>(values, filter, count, indexing);
            case logical_type::UBIGINT:
                return apply_pax_fixed_constant_filter_typed<uint64_t>(values, filter, count, indexing);
            case logical_type::HUGEINT:
                return apply_pax_fixed_constant_filter_typed<components::types::int128_t>(values,
                                                                                           filter,
                                                                                           count,
                                                                                           indexing);
            case logical_type::UHUGEINT:
                return apply_pax_fixed_constant_filter_typed<components::types::uint128_t>(values,
                                                                                            filter,
                                                                                            count,
                                                                                            indexing);
            case logical_type::UUID:
                return apply_pax_fixed_constant_filter_typed<components::types::int128_t>(values,
                                                                                           filter,
                                                                                           count,
                                                                                           indexing);
            case logical_type::FLOAT:
                return apply_pax_fixed_constant_filter_typed<float>(values, filter, count, indexing);
            case logical_type::DOUBLE:
                return apply_pax_fixed_constant_filter_typed<double>(values, filter, count, indexing);
            case logical_type::TIMESTAMP:
                return apply_pax_fixed_constant_filter_value(values,
                                                             filter.filter_type,
                                                             filter.constant.value<core::date::timestamp_t>().value.count(),
                                                             count,
                                                             indexing);
            case logical_type::TIMESTAMP_TZ:
                return apply_pax_fixed_constant_filter_value(values,
                                                             filter.filter_type,
                                                             filter.constant.value<core::date::timestamptz_t>().value.count(),
                                                             count,
                                                             indexing);
            case logical_type::DECIMAL:
                if (values.type().to_physical_type() == components::types::physical_type::INT128) {
                    return apply_pax_fixed_constant_filter_value(values,
                                                                 filter.filter_type,
                                                                 filter.constant.value<components::types::int128_t>(),
                                                                 count,
                                                                 indexing);
                }
                return apply_pax_fixed_constant_filter_value(values,
                                                             filter.filter_type,
                                                             filter.constant.value<int64_t>(),
                                                             count,
                                                             indexing);
            default:
                return 0;
        }
    }

    uint64_t apply_pax_validity_filter(const components::vector::vector_t& values,
                                       components::expressions::compare_type compare_type,
                                       uint64_t count,
                                       components::vector::indexing_vector_t& indexing) {
        const auto& validity = values.validity();
        const bool select_valid = compare_type == components::expressions::compare_type::is_not_null;
        uint64_t approved_count = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (validity.row_is_valid(i) == select_valid) {
                indexing.set_index(approved_count++, i);
            }
        }
        return approved_count;
    }

    uint64_t apply_pax_fixed_single_filter(const components::vector::vector_t& values,
                                           const components::table::table_filter_t& filter,
                                           uint64_t count,
                                           components::vector::indexing_vector_t& indexing) {
        if (filter.filter_type == components::expressions::compare_type::is_null ||
            filter.filter_type == components::expressions::compare_type::is_not_null) {
            return apply_pax_validity_filter(values, filter.filter_type, count, indexing);
        }

        const auto* constant_filter = dynamic_cast<const components::table::constant_filter_t*>(&filter);
        if (!constant_filter) {
            return 0;
        }
        return apply_pax_fixed_constant_filter(values, *constant_filter, count, indexing);
    }

    uint64_t apply_pax_fixed_filter_list(const components::vector::vector_t& values,
                                         const std::vector<const components::table::table_filter_t*>& filters,
                                         uint64_t count,
                                         components::vector::indexing_vector_t& indexing) {
        if (filters.empty()) {
            indexing = components::vector::indexing_vector_t(values.resource(), 0, count);
            return count;
        }

        if (filters.size() == 1) {
            return apply_pax_fixed_single_filter(values, *filters.front(), count, indexing);
        }

        std::vector<uint8_t> approved(count, 1);
        std::vector<uint8_t> matched(count, 0);
        for (const auto* filter : filters) {
            std::fill(matched.begin(), matched.end(), uint8_t{0});

            components::vector::indexing_vector_t filter_indexing(values.resource(), count);
            const auto filter_count = apply_pax_fixed_single_filter(values, *filter, count, filter_indexing);
            if (filter_count == 0) {
                return 0;
            }
            for (uint64_t i = 0; i < filter_count; i++) {
                matched[filter_indexing.get_index(i)] = 1;
            }

            for (uint64_t i = 0; i < count; i++) {
                approved[i] = approved[i] && matched[i];
            }
        }

        uint64_t approved_count = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (approved[i]) {
                indexing.set_index(approved_count++, i);
            }
        }
        return approved_count;
    }

    components::table::filter_propagate_result_t check_pax_fixed_page_statistics(
        const components::table::storage::pax_fixed_page_t& page,
        uint32_t column_index,
        const std::vector<const components::table::table_filter_t*>& filters) {
        using components::table::filter_propagate_result_t;

        if (filters.empty()) {
            return filter_propagate_result_t::ALWAYS_TRUE;
        }

        bool all_true = true;
        for (const auto* filter : filters) {
            const auto result = check_pax_fixed_page_statistics(page, column_index, *filter);
            if (result == filter_propagate_result_t::ALWAYS_FALSE) {
                return filter_propagate_result_t::ALWAYS_FALSE;
            }
            if (result != filter_propagate_result_t::ALWAYS_TRUE) {
                all_true = false;
            }
        }
        return all_true ? filter_propagate_result_t::ALWAYS_TRUE
                        : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    bool extract_pax_fixed_simple_filter_column(const components::table::table_filter_t* filter,
                                                uint32_t& column_index) {
        if (!filter || !is_supported_pax_projected_filter_compare(filter->filter_type)) {
            return false;
        }

        if (filter->filter_type == components::expressions::compare_type::is_null ||
            filter->filter_type == components::expressions::compare_type::is_not_null) {
            auto* null_filter = dynamic_cast<const components::table::is_null_filter_t*>(filter);
            if (!null_filter || null_filter->table_indices.size() != 1) {
                return false;
            }
            column_index = static_cast<uint32_t>(null_filter->table_indices.front());
            return true;
        }

        auto* constant_filter = dynamic_cast<const components::table::constant_filter_t*>(filter);
        if (!constant_filter || constant_filter->table_indices.size() != 1) {
            return false;
        }
        column_index = static_cast<uint32_t>(constant_filter->table_indices.front());
        return true;
    }

    bool is_pax_fixed_conjunction_filter_tree(const components::table::table_filter_t* filter) {
        if (!filter) {
            return true;
        }

        if (filter->filter_type == components::expressions::compare_type::union_and) {
            auto* and_filter = dynamic_cast<const components::table::conjunction_and_filter_t*>(filter);
            if (!and_filter || and_filter->child_filters.empty()) {
                return false;
            }
            for (const auto& child_filter : and_filter->child_filters) {
                if (!is_pax_fixed_conjunction_filter_tree(child_filter.get())) {
                    return false;
                }
            }
            return true;
        }

        uint32_t column_index = 0;
        return extract_pax_fixed_simple_filter_column(filter, column_index);
    }

    void append_unique_column_index(std::vector<uint32_t>& columns, uint32_t column_index) {
        if (std::find(columns.begin(), columns.end(), column_index) == columns.end()) {
            columns.push_back(column_index);
        }
    }

    bool collect_pax_fixed_filter_tree(const components::table::table_filter_t* filter,
                                       std::vector<const components::table::table_filter_t*>& simple_filters,
                                       std::vector<uint32_t>& filter_columns) {
        using components::expressions::compare_type;

        if (!filter) {
            return true;
        }

        switch (filter->filter_type) {
            case compare_type::union_and:
            case compare_type::union_or:
            case compare_type::union_not: {
                auto* conjunction = dynamic_cast<const components::table::conjunction_filter_t*>(filter);
                if (!conjunction || conjunction->child_filters.empty()) {
                    return false;
                }
                for (const auto& child_filter : conjunction->child_filters) {
                    if (!collect_pax_fixed_filter_tree(child_filter.get(), simple_filters, filter_columns)) {
                        return false;
                    }
                }
                return true;
            }
            default: {
                uint32_t column_index = 0;
                if (!extract_pax_fixed_simple_filter_column(filter, column_index)) {
                    return false;
                }
                simple_filters.push_back(filter);
                append_unique_column_index(filter_columns, column_index);
                return true;
            }
        }
    }

    components::table::filter_propagate_result_t check_pax_fixed_filter_tree_statistics(
        const components::table::storage::pax_fixed_page_t& page,
        const components::table::table_filter_t* filter,
        const std::function<bool(uint32_t)>& column_has_updates) {
        using components::expressions::compare_type;
        using components::table::filter_propagate_result_t;

        if (!filter) {
            return filter_propagate_result_t::ALWAYS_TRUE;
        }

        switch (filter->filter_type) {
            case compare_type::union_and: {
                auto* and_filter = dynamic_cast<const components::table::conjunction_and_filter_t*>(filter);
                if (!and_filter || and_filter->child_filters.empty()) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }

                bool all_true = true;
                for (const auto& child_filter : and_filter->child_filters) {
                    const auto child_result =
                        check_pax_fixed_filter_tree_statistics(page, child_filter.get(), column_has_updates);
                    if (child_result == filter_propagate_result_t::ALWAYS_FALSE) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (child_result != filter_propagate_result_t::ALWAYS_TRUE) {
                        all_true = false;
                    }
                }
                return all_true ? filter_propagate_result_t::ALWAYS_TRUE
                                : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            }
            case compare_type::union_or: {
                auto* or_filter = dynamic_cast<const components::table::conjunction_or_filter_t*>(filter);
                if (!or_filter || or_filter->child_filters.empty()) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }

                bool all_false = true;
                for (const auto& child_filter : or_filter->child_filters) {
                    const auto child_result =
                        check_pax_fixed_filter_tree_statistics(page, child_filter.get(), column_has_updates);
                    if (child_result == filter_propagate_result_t::ALWAYS_TRUE) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    if (child_result != filter_propagate_result_t::ALWAYS_FALSE) {
                        all_false = false;
                    }
                }
                return all_false ? filter_propagate_result_t::ALWAYS_FALSE
                                 : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            }
            case compare_type::union_not:
                return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            default: {
                uint32_t column_index = 0;
                if (!extract_pax_fixed_simple_filter_column(filter, column_index) || column_has_updates(column_index)) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }
                return check_pax_fixed_page_statistics(page, column_index, *filter);
            }
        }
    }

    components::table::filter_propagate_result_t check_pax_generic_filter_tree_statistics(
        const components::table::storage::pax_generic_page_t& page,
        const components::table::table_filter_t* filter,
        const std::function<bool(uint32_t)>& column_has_updates) {
        using components::expressions::compare_type;
        using components::table::filter_propagate_result_t;

        if (!filter) {
            return filter_propagate_result_t::ALWAYS_TRUE;
        }

        switch (filter->filter_type) {
            case compare_type::union_and: {
                auto* and_filter = dynamic_cast<const components::table::conjunction_and_filter_t*>(filter);
                if (!and_filter || and_filter->child_filters.empty()) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }

                bool all_true = true;
                for (const auto& child_filter : and_filter->child_filters) {
                    const auto child_result =
                        check_pax_generic_filter_tree_statistics(page, child_filter.get(), column_has_updates);
                    if (child_result == filter_propagate_result_t::ALWAYS_FALSE) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (child_result != filter_propagate_result_t::ALWAYS_TRUE) {
                        all_true = false;
                    }
                }
                return all_true ? filter_propagate_result_t::ALWAYS_TRUE
                                : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            }
            case compare_type::union_or: {
                auto* or_filter = dynamic_cast<const components::table::conjunction_or_filter_t*>(filter);
                if (!or_filter || or_filter->child_filters.empty()) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }

                bool all_false = true;
                for (const auto& child_filter : or_filter->child_filters) {
                    const auto child_result =
                        check_pax_generic_filter_tree_statistics(page, child_filter.get(), column_has_updates);
                    if (child_result == filter_propagate_result_t::ALWAYS_TRUE) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    if (child_result != filter_propagate_result_t::ALWAYS_FALSE) {
                        all_false = false;
                    }
                }
                return all_false ? filter_propagate_result_t::ALWAYS_FALSE
                                 : filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            }
            case compare_type::union_not:
                return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
            default: {
                uint32_t column_index = 0;
                if (!extract_pax_fixed_simple_filter_column(filter, column_index) || column_has_updates(column_index)) {
                    return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
                }
                return check_pax_generic_page_statistics(page, column_index, *filter);
            }
        }
    }

    struct pax_fixed_decoded_filter_column_t {
        uint32_t column_index{0};
        // Non-owning: points into the per-scan reusable decode-buffer pool, not a per-page allocation.
        components::vector::vector_t* values{nullptr};
    };

    const components::vector::vector_t*
    find_pax_fixed_decoded_filter_column(const std::vector<pax_fixed_decoded_filter_column_t>& decoded_columns,
                                         uint32_t column_index) {
        for (const auto& decoded_column : decoded_columns) {
            if (decoded_column.column_index == column_index) {
                return decoded_column.values;
            }
        }
        return nullptr;
    }

    uint64_t apply_pax_fixed_filter_conjunction(
        const std::vector<const components::table::table_filter_t*>& simple_filters,
        const std::vector<pax_fixed_decoded_filter_column_t>& decoded_filter_columns,
        uint64_t count,
        components::vector::indexing_vector_t& indexing) {
        if (simple_filters.empty()) {
            indexing = components::vector::indexing_vector_t(indexing.resource(), 0, count);
            return count;
        }

        // `approved` starts all-zero and the first filter writes its matches straight into it.
        // `matched` is only needed from the second filter onward, so allocate it lazily.
        std::vector<uint8_t> approved(count, 0);
        std::vector<uint8_t> matched;
        bool first_filter = true;
        for (const auto* simple_filter : simple_filters) {
            uint32_t column_index = 0;
            if (!extract_pax_fixed_simple_filter_column(simple_filter, column_index)) {
                return 0;
            }

            const auto* values = find_pax_fixed_decoded_filter_column(decoded_filter_columns, column_index);
            if (!values) {
                return 0;
            }

            components::vector::indexing_vector_t filter_indexing(values->resource(), count);
            const auto filter_count = apply_pax_fixed_single_filter(*values, *simple_filter, count, filter_indexing);
            if (filter_count == 0) {
                return 0;
            }

            if (first_filter) {
                for (uint64_t i = 0; i < filter_count; i++) {
                    approved[filter_indexing.get_index(i)] = 1;
                }
                first_filter = false;
            } else {
                if (matched.empty()) {
                    matched.assign(count, 0);
                } else {
                    std::fill(matched.begin(), matched.end(), uint8_t{0});
                }
                for (uint64_t i = 0; i < filter_count; i++) {
                    matched[filter_indexing.get_index(i)] = 1;
                }
                for (uint64_t row = 0; row < count; row++) {
                    approved[row] = approved[row] && matched[row];
                }
            }
        }

        uint64_t approved_count = 0;
        for (uint64_t row = 0; row < count; row++) {
            if (approved[row]) {
                indexing.set_index(approved_count++, row);
            }
        }
        return approved_count;
    }

    bool evaluate_pax_fixed_filter_tree_row(
        const components::table::table_filter_t* filter,
        uint64_t row_index,
        const std::unordered_map<const components::table::table_filter_t*, std::vector<uint8_t>>& simple_filter_masks) {
        using components::expressions::compare_type;

        if (!filter) {
            return true;
        }

        switch (filter->filter_type) {
            case compare_type::union_and: {
                auto* and_filter = dynamic_cast<const components::table::conjunction_and_filter_t*>(filter);
                if (!and_filter) {
                    return false;
                }
                for (const auto& child_filter : and_filter->child_filters) {
                    if (!evaluate_pax_fixed_filter_tree_row(child_filter.get(), row_index, simple_filter_masks)) {
                        return false;
                    }
                }
                return true;
            }
            case compare_type::union_or: {
                auto* or_filter = dynamic_cast<const components::table::conjunction_or_filter_t*>(filter);
                if (!or_filter) {
                    return false;
                }
                for (const auto& child_filter : or_filter->child_filters) {
                    if (evaluate_pax_fixed_filter_tree_row(child_filter.get(), row_index, simple_filter_masks)) {
                        return true;
                    }
                }
                return false;
            }
            case compare_type::union_not: {
                auto* not_filter = dynamic_cast<const components::table::conjunction_not_filter_t*>(filter);
                if (!not_filter) {
                    return false;
                }
                for (const auto& child_filter : not_filter->child_filters) {
                    if (evaluate_pax_fixed_filter_tree_row(child_filter.get(), row_index, simple_filter_masks)) {
                        return false;
                    }
                }
                return true;
            }
            default: {
                const auto it = simple_filter_masks.find(filter);
                return it != simple_filter_masks.end() && row_index < it->second.size() && it->second[row_index] != 0;
            }
        }
    }

    uint64_t apply_pax_fixed_filter_tree(
        const components::table::table_filter_t* filter,
        const std::vector<const components::table::table_filter_t*>& simple_filters,
        const std::vector<pax_fixed_decoded_filter_column_t>& decoded_filter_columns,
        uint64_t count,
        components::vector::indexing_vector_t& indexing) {
        std::unordered_map<const components::table::table_filter_t*, std::vector<uint8_t>> simple_filter_masks;
        simple_filter_masks.reserve(simple_filters.size());

        for (const auto* simple_filter : simple_filters) {
            uint32_t column_index = 0;
            if (!extract_pax_fixed_simple_filter_column(simple_filter, column_index)) {
                return 0;
            }
            const auto* values = find_pax_fixed_decoded_filter_column(decoded_filter_columns, column_index);
            if (!values) {
                return 0;
            }

            components::vector::indexing_vector_t filter_indexing(values->resource(), count);
            const auto filter_count = apply_pax_fixed_single_filter(*values, *simple_filter, count, filter_indexing);
            std::vector<uint8_t> mask(count, 0);
            for (uint64_t i = 0; i < filter_count; i++) {
                mask[filter_indexing.get_index(i)] = 1;
            }
            simple_filter_masks.emplace(simple_filter, std::move(mask));
        }

        uint64_t approved_count = 0;
        for (uint64_t row = 0; row < count; row++) {
            if (evaluate_pax_fixed_filter_tree_row(filter, row, simple_filter_masks)) {
                indexing.set_index(approved_count++, row);
            }
        }
        return approved_count;
    }

    bool apply_pax_generic_validity_window(components::table::row_group_t& row_group,
                                           const components::table::storage::pax_generic_slice_t& validity_slice,
                                           uint64_t page_row_offset,
                                           uint64_t copy_count,
                                           components::vector::vector_t& result,
                                           uint64_t result_offset,
                                           pax_generic_block_cache_t& block_cache) {
        using components::table::storage::pax_generic_codec_kind;

        switch (validity_slice.codec_kind) {
            case pax_generic_codec_kind::VALIDITY_ALL_VALID:
                return true;
            case pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                for (uint64_t i = 0; i < copy_count; i++) {
                    result.validity().set(result_offset + i, false);
                }
                return true;
            case pax_generic_codec_kind::VALIDITY_BITMASK: {
                if (!validity_slice.payload.has_value()) {
                    return false;
                }
                auto& validity_pointer = validity_slice.payload->main_pointer;
                auto& block_handle =
                    get_or_pin_pax_generic_block(row_group, validity_pointer.block_pointer.block_id, block_cache);
                std::vector<uint64_t> aligned; // must outlive source_mask (which aliases it)
                copy_aligned_pax_validity(checked_pax_block_ptr(block_handle,
                                                                validity_pointer.block_pointer.offset,
                                                                validity_pointer.segment_size,
                                                                row_group.block_manager().block_size()),
                                          validity_pointer.segment_size,
                                          aligned);
                components::vector::validity_mask_t source_mask(result.resource(), aligned.data());
                result.validity().slice_in_place(source_mask, result_offset, page_row_offset, copy_count);
                return true;
            }
            case pax_generic_codec_kind::STRING_SEGMENT:
            case pax_generic_codec_kind::FIXED_PLAIN:
            default:
                return false;
        }
    }

    bool decode_pax_generic_fixed_window(components::table::row_group_t& row_group,
                                         const components::table::storage::pax_generic_row_group_layout_t& layout,
                                         uint32_t column_index,
                                         const components::types::complex_logical_type& type,
                                         uint64_t window_row_offset,
                                         uint64_t window_count,
                                         components::vector::vector_t& result,
                                         uint64_t result_offset,
                                         pax_generic_block_cache_t& block_cache) {
        using components::table::storage::pax_generic_slice_kind;

        if (window_count == 0) {
            return true;
        }
        if (!is_pax_generic_fixed_plain_type(type) || result.type() != type) {
            return false;
        }

        const auto type_size = static_cast<uint64_t>(type.size());
        const auto window_end = window_row_offset + window_count;
        result.set_vector_type(components::vector::vector_type::FLAT);
        mark_vector_range_valid(result, result_offset, window_count);

        uint64_t copied_rows = 0;
        for (const auto& page : layout.pages) {
            const auto page_start = static_cast<uint64_t>(page.row_offset_in_group);
            const auto page_end = page_start + static_cast<uint64_t>(page.tuple_count);
            if (page_end <= window_row_offset || page_start >= window_end) {
                continue;
            }

            const auto* value_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::FIXED_VALUES);
            const auto* validity_slice = find_pax_generic_slice(page, column_index, pax_generic_slice_kind::VALIDITY);
            if (!value_slice || !validity_slice ||
                !validate_pax_generic_fixed_slice(*value_slice, page.tuple_count, type) ||
                !validate_pax_generic_validity_slice(*validity_slice, page.tuple_count)) {
                return false;
            }

            const auto overlap_start = std::max(window_row_offset, page_start);
            const auto overlap_end = std::min(window_end, page_end);
            const auto copy_count = overlap_end - overlap_start;
            const auto page_row_offset = overlap_start - page_start;
            const auto window_result_offset = result_offset + (overlap_start - window_row_offset);

            auto& value_pointer = value_slice->payload->main_pointer;
            auto& block_handle =
                get_or_pin_pax_generic_block(row_group, value_pointer.block_pointer.block_id, block_cache);
            auto* source = checked_pax_block_ptr(block_handle,
                                                 value_pointer.block_pointer.offset,
                                                 value_pointer.segment_size,
                                                 row_group.block_manager().block_size()) +
                           page_row_offset * type_size;
            auto* target = result.data() + window_result_offset * type_size;
            std::memcpy(target, source, copy_count * type_size);

            if (!apply_pax_generic_validity_window(row_group,
                                                   *validity_slice,
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
            auto* stored = auxiliary->insert(value.data(), value.size());
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
            auto* base_ptr = checked_pax_block_ptr(block_handle,
                                                   value_pointer.block_pointer.offset,
                                                   value_pointer.segment_size,
                                                   row_group.block_manager().block_size());
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
            std::vector<uint64_t> page_validity_aligned; // must outlive page_validity_mask (aliased below)
            if (validity_slice->codec_kind == pax_generic_codec_kind::VALIDITY_BITMASK) {
                auto& validity_pointer = validity_slice->payload->main_pointer;
                auto& validity_handle =
                    get_or_pin_pax_generic_block(row_group, validity_pointer.block_pointer.block_id, block_cache);
                copy_aligned_pax_validity(checked_pax_block_ptr(validity_handle,
                                                                validity_pointer.block_pointer.offset,
                                                                validity_pointer.segment_size,
                                                                row_group.block_manager().block_size()),
                                          validity_pointer.segment_size,
                                          page_validity_aligned);
                page_validity_mask.emplace(result.resource(), page_validity_aligned.data());
            }

            // The int32 offset array sits at a possibly-unaligned byte offset, so load each entry
            // via memcpy rather than dereferencing an int32_t* (misaligned load is UB).
            const auto* page_offsets_raw = base_ptr + PAX_STRING_DICTIONARY_HEADER_SIZE;
            const auto load_page_offset = [page_offsets_raw](uint64_t idx) {
                int32_t v;
                std::memcpy(&v, page_offsets_raw + idx * sizeof(int32_t), sizeof(int32_t));
                return v;
            };
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

                const auto dict_offset = load_page_offset(page_index);
                const auto prev_offset = page_index == 0 ? 0 : load_page_offset(page_index - 1);
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
                    const uint64_t overflow_bs = row_group.block_manager().block_size();
                    const uint64_t overflow_pos = static_cast<uint64_t>(overflow_offset);
                    // Bound the disk-derived offset/length against the overflow block before reading
                    // the length prefix and the string bytes.
                    if (overflow_pos + sizeof(uint32_t) > overflow_bs) {
                        return false;
                    }
                    auto* overflow_ptr = overflow_handle.ptr() + overflow_pos;
                    uint32_t overflow_length = 0;
                    std::memcpy(&overflow_length, overflow_ptr, sizeof(uint32_t));
                    if (overflow_pos + sizeof(uint32_t) + static_cast<uint64_t>(overflow_length) > overflow_bs) {
                        return false;
                    }
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

    bool decode_pax_generic_root_window(components::table::row_group_t& row_group,
                                        const components::table::storage::pax_generic_row_group_layout_t& layout,
                                        uint32_t column_index,
                                        const components::types::complex_logical_type& type,
                                        uint64_t window_row_offset,
                                        uint64_t window_count,
                                        components::vector::vector_t& result,
                                        uint64_t result_offset,
                                        pax_generic_block_cache_t& block_cache) {
        if (is_pax_generic_string_type(type)) {
            return decode_pax_generic_string_window(row_group,
                                                    layout,
                                                    column_index,
                                                    window_row_offset,
                                                    window_count,
                                                    result,
                                                    result_offset,
                                                    block_cache);
        }
        if (is_pax_generic_fixed_plain_type(type)) {
            return decode_pax_generic_fixed_window(row_group,
                                                   layout,
                                                   column_index,
                                                   type,
                                                   window_row_offset,
                                                   window_count,
                                                   result,
                                                   result_offset,
                                                   block_cache);
        }
        return false;
    }

    uint64_t apply_pax_generic_root_single_filter(const components::vector::vector_t& values,
                                                  const components::table::table_filter_t& filter,
                                                  uint64_t count,
                                                  components::vector::indexing_vector_t& indexing) {
        if (filter.filter_type == components::expressions::compare_type::is_null ||
            filter.filter_type == components::expressions::compare_type::is_not_null) {
            return apply_pax_validity_filter(values, filter.filter_type, count, indexing);
        }

        const auto* constant_filter = dynamic_cast<const components::table::constant_filter_t*>(&filter);
        if (!constant_filter) {
            return 0;
        }
        if (values.type().to_physical_type() == components::types::physical_type::STRING) {
            return apply_pax_generic_string_constant_filter(values, *constant_filter, count, indexing);
        }
        if (is_pax_generic_fixed_plain_type(values.type())) {
            return apply_pax_fixed_constant_filter(values, *constant_filter, count, indexing);
        }
        return 0;
    }

    struct pax_generic_decoded_filter_column_t {
        uint32_t column_index{0};
        std::unique_ptr<components::vector::vector_t> values;
    };

    const components::vector::vector_t*
    find_pax_generic_decoded_filter_column(const std::vector<pax_generic_decoded_filter_column_t>& decoded_columns,
                                           uint32_t column_index) {
        for (const auto& decoded_column : decoded_columns) {
            if (decoded_column.column_index == column_index) {
                return decoded_column.values.get();
            }
        }
        return nullptr;
    }

    uint64_t apply_pax_generic_root_filter_list(
        const components::vector::vector_t& values,
        const std::vector<const components::table::table_filter_t*>& filters,
        uint64_t count,
        components::vector::indexing_vector_t& indexing) {
        if (filters.empty()) {
            indexing = components::vector::indexing_vector_t(values.resource(), 0, count);
            return count;
        }

        if (filters.size() == 1) {
            return apply_pax_generic_root_single_filter(values, *filters.front(), count, indexing);
        }

        std::vector<uint8_t> approved(count, 1);
        std::vector<uint8_t> matched(count, 0);
        for (const auto* filter : filters) {
            std::fill(matched.begin(), matched.end(), uint8_t{0});

            components::vector::indexing_vector_t filter_indexing(values.resource(), count);
            const auto filter_count = apply_pax_generic_root_single_filter(values, *filter, count, filter_indexing);
            if (filter_count == 0) {
                return 0;
            }
            for (uint64_t i = 0; i < filter_count; i++) {
                matched[filter_indexing.get_index(i)] = 1;
            }

            for (uint64_t row = 0; row < count; row++) {
                approved[row] = approved[row] && matched[row];
            }
        }

        uint64_t approved_count = 0;
        for (uint64_t row = 0; row < count; row++) {
            if (approved[row]) {
                indexing.set_index(approved_count++, row);
            }
        }
        return approved_count;
    }

    uint64_t apply_pax_generic_root_filter_conjunction(
        const std::vector<const components::table::table_filter_t*>& simple_filters,
        const std::vector<pax_generic_decoded_filter_column_t>& decoded_filter_columns,
        uint64_t count,
        components::vector::indexing_vector_t& indexing) {
        if (simple_filters.empty()) {
            indexing = components::vector::indexing_vector_t(indexing.resource(), 0, count);
            return count;
        }

        std::vector<uint8_t> approved(count, 1);
        std::vector<uint8_t> matched(count, 0);
        for (const auto* simple_filter : simple_filters) {
            uint32_t column_index = 0;
            if (!extract_pax_fixed_simple_filter_column(simple_filter, column_index)) {
                return 0;
            }

            const auto* values = find_pax_generic_decoded_filter_column(decoded_filter_columns, column_index);
            if (!values) {
                return 0;
            }

            std::fill(matched.begin(), matched.end(), uint8_t{0});
            components::vector::indexing_vector_t filter_indexing(values->resource(), count);
            const auto filter_count =
                apply_pax_generic_root_single_filter(*values, *simple_filter, count, filter_indexing);
            if (filter_count == 0) {
                return 0;
            }
            for (uint64_t i = 0; i < filter_count; i++) {
                matched[filter_indexing.get_index(i)] = 1;
            }

            for (uint64_t row = 0; row < count; row++) {
                approved[row] = approved[row] && matched[row];
            }
        }

        uint64_t approved_count = 0;
        for (uint64_t row = 0; row < count; row++) {
            if (approved[row]) {
                indexing.set_index(approved_count++, row);
            }
        }
        return approved_count;
    }

    bool evaluate_pax_generic_filter_tree_row(
        const components::table::table_filter_t* filter,
        uint64_t row_index,
        const std::unordered_map<const components::table::table_filter_t*, std::vector<uint8_t>>& simple_filter_masks) {
        return evaluate_pax_fixed_filter_tree_row(filter, row_index, simple_filter_masks);
    }

    uint64_t apply_pax_generic_root_filter_tree(
        const components::table::table_filter_t* filter,
        const std::vector<const components::table::table_filter_t*>& simple_filters,
        const std::vector<pax_generic_decoded_filter_column_t>& decoded_filter_columns,
        uint64_t count,
        components::vector::indexing_vector_t& indexing) {
        std::unordered_map<const components::table::table_filter_t*, std::vector<uint8_t>> simple_filter_masks;
        simple_filter_masks.reserve(simple_filters.size());

        for (const auto* simple_filter : simple_filters) {
            uint32_t column_index = 0;
            if (!extract_pax_fixed_simple_filter_column(simple_filter, column_index)) {
                return 0;
            }
            const auto* values = find_pax_generic_decoded_filter_column(decoded_filter_columns, column_index);
            if (!values) {
                return 0;
            }

            components::vector::indexing_vector_t filter_indexing(values->resource(), count);
            const auto filter_count =
                apply_pax_generic_root_single_filter(*values, *simple_filter, count, filter_indexing);
            std::vector<uint8_t> mask(count, 0);
            for (uint64_t i = 0; i < filter_count; i++) {
                mask[filter_indexing.get_index(i)] = 1;
            }
            simple_filter_masks.emplace(simple_filter, std::move(mask));
        }

        uint64_t approved_count = 0;
        for (uint64_t row = 0; row < count; row++) {
            if (evaluate_pax_generic_filter_tree_row(filter, row, simple_filter_masks)) {
                indexing.set_index(approved_count++, row);
            }
        }
        return approved_count;
    }

} // namespace

namespace components::table {

    row_group_t::row_group_t(collection_t* collection, int64_t start, uint64_t count)
        : segment_base_t(start, count)
        , collection_(collection)
        , deletes_is_loaded_(true)
        , allocation_size_(0) {}

    void row_group_t::move_to_collection(collection_t* collection, int64_t new_start) {
        collection_ = collection;
        start = new_start;
        mark_dirty();
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
        assert(column_pointers_.size() == columns_.size() && "Lazy loading a column but the pointer was not set");
        assert(false && "row_group_t::get_column: unknown error");
        std::abort();
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
        state.row_offset_override_active = false;
        state.row_offset_override = 0;
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
        state.row_offset_override_active = false;
        state.row_offset_override = 0;
        const auto row_group_limit =
            start > state.max_row ? 0 : std::min(static_cast<int64_t>(count.load()), state.max_row - start);
        if (state.vector_index_relative_to_row_group) {
            state.vector_index = 0;
            state.max_row_group_row = row_group_limit;
        } else {
            state.max_row_group_row += row_group_limit;
        }
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
            const types::logical_value_t fill_value =
                default_value.has_value() ? *default_value
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
        row_group->mark_dirty();

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
        row_group->mark_dirty();

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
                assert(false && "invalid type for filter selection");
                std::abort();
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
                // Works for both constant_filter_t and set_membership_filter_t.
                const auto& indices = table_filter_table_indices(filter);
                column_data_t* column = &get_column(indices.front());
                for (size_t i = 1; i < indices.size(); i++) {
                    column = static_cast<struct_column_data_t*>(column)->sub_columns[indices[i]].get();
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
        std::vector<const table_filter_t*> simple_filters;
        std::vector<uint32_t> filter_columns;
        if (collect_pax_fixed_filter_tree(f, simple_filters, filter_columns)) {
            for (const auto column_index : filter_columns) {
                if (column_index < get_column_count() &&
                    get_column(column_index).has_updates()) {
                    return true;
                }
            }
        }
        // For constant comparison filters, check if any column's zonemap prunes this segment
        if (f->filter_type == expressions::compare_type::eq || f->filter_type == expressions::compare_type::gt ||
            f->filter_type == expressions::compare_type::gte || f->filter_type == expressions::compare_type::lt ||
            f->filter_type == expressions::compare_type::lte) {
            // Support both constant_filter_t and set_membership_filter_t for zonemap pruning.
            const auto& cf_indices = table_filter_table_indices(f);
            if (!cf_indices.empty()) {
                auto col_idx = cf_indices.front();
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
                                      int64_t row_id_base,
                                      vector::indexing_vector_t& indexing,
                                      const table_filter_t* filter,
                                      uint64_t& approved_tuple_count) {
        vector::indexing_vector_t new_indexing(resource, approved_tuple_count);
        uint64_t result_count = 0;
        for (uint64_t i = 0; i < approved_tuple_count; i++) {
            auto idx = indexing.get_index(i);
            new_indexing.set_index(result_count, idx);
            result_count += check_predicate(row_id_base + static_cast<int64_t>(idx), filter);
        }
        indexing = new_indexing;
        approved_tuple_count = result_count;
    }


	    bool row_group_t::try_scan_pax_generic_projected(collection_scan_state& state, vector::data_chunk_t& result) {
	        const bool has_persisted_pax_layout =
	            layout_kind_ == storage::row_group_layout_kind::PAX_GENERIC && pax_generic_layout_.has_value();
	        if (!has_persisted_pax_layout) {
	            return false;
	        }
	        const bool transaction_scan = state.txn.transaction_id != 0 || state.txn.start_time != 0;
		        const bool apply_visibility_filter = requires_pax_version_visibility(transaction_scan);

        const auto& column_ids = state.column_ids();
        if (column_ids.empty()) {
            return false;
        }

        auto* filter = state.filter();
        std::vector<const table_filter_t*> generic_simple_filters;
        std::vector<uint32_t> generic_filter_columns;
        if (filter && !collect_pax_fixed_filter_tree(filter, generic_simple_filters, generic_filter_columns)) {
            return false;
        }
        const bool has_filter = filter != nullptr;
        const bool conjunction_filter = has_filter && is_pax_fixed_conjunction_filter_tree(filter);
        const bool single_column_conjunction_filter = conjunction_filter && generic_filter_columns.size() == 1;

        std::vector<uint32_t> required_generic_columns;
        for (const auto& column_index : generic_filter_columns) {
            append_unique_column_index(required_generic_columns, column_index);
        }

        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.has_children()) {
                return false;
            }
            if (column.is_row_id_column()) {
                continue;
            }

            const auto column_index = column.primary_index();
            if (column_index >= get_column_count()) {
                return false;
            }

	            auto& column_data = get_column(column_index);
	            if (!is_pax_generic_root_projected_type(column_data.type())) {
	                return false;
	            }
	            if (transaction_scan && column_data.has_uncommitted_updates()) {
	                return false;
	            }
	            append_unique_column_index(required_generic_columns, static_cast<uint32_t>(column_index));
	        }

        for (const auto column_index : generic_filter_columns) {
            if (column_index >= get_column_count()) {
                return false;
            }
            auto& filter_column = get_column(column_index);
	            if (!is_pax_generic_root_projected_type(filter_column.type())) {
	                return false;
	            }
	            if (transaction_scan && filter_column.has_uncommitted_updates()) {
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
        const auto pax_tuple_count = pax_generic_layout_tuple_count(layout);
        const auto row_group_tuple_count = count.load();
        if (expected_row_offset != pax_tuple_count || pax_tuple_count > row_group_tuple_count) {
            return false;
        }
        const auto row_group_scan_limit =
            start > state.max_row ? uint64_t(0)
                                  : static_cast<uint64_t>(std::min(static_cast<int64_t>(row_group_tuple_count),
                                                                   state.max_row - start));
        if (row_group_scan_limit > pax_tuple_count &&
            pax_tuple_count % vector::DEFAULT_VECTOR_CAPACITY != 0) {
            return false;
        }

        for (const auto column_index : required_generic_columns) {
            const auto& column_type = get_column(column_index).type();
            for (const auto& page : layout.pages) {
                if (!validate_pax_generic_root_column_slices(page, column_index, column_type)) {
                    return false;
                }
            }
        }

        for (auto& column_state : state.column_scans) {
            column_state.result_offset = result.size();
        }

        pax_generic_block_cache_t block_cache;
        const auto local_max_row_group_row = std::min(row_group_scan_limit, pax_tuple_count);

        while (true) {
            const auto current_row = current_pax_fixed_row_offset(*this, state);
            if (current_row >= local_max_row_group_row) {
                if (row_group_scan_limit > pax_tuple_count && current_row >= pax_tuple_count) {
                    return false;
                }
                return true;
            }

            const auto max_count =
                std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, local_max_row_group_row - current_row);

            if (!check_zonemap_segments(state)) {
                continue;
            }

            vector::indexing_vector_t visible_indexing(result.resource(), max_count);
            const auto visible_count = apply_visibility_filter
                                           ? pax_visibility_indexing(state,
                                                                    current_row,
                                                                    max_count,
                                                                    visible_indexing,
                                                                    transaction_scan)
                                           : max_count;
            if (visible_count == 0) {
                state.vector_index++;
                set_pax_scan_row_offset(*this, state, current_row + max_count);
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                }
                continue;
            }
            const auto visible_mask = visible_count == max_count
                                          ? std::vector<uint8_t>{}
                                          : build_pax_visibility_mask(visible_indexing, visible_count, max_count);

            const auto window_end = current_row + max_count;
            const auto page_windows = collect_pax_page_windows(layout.pages, current_row, window_end);
            const auto result_offset = result.size();
            if (!has_filter) {
                std::vector<uint64_t> prefetch_blocks;
                for (const auto& page_window : page_windows) {
                    const auto& page = *page_window.page;
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        if (!collect_pax_generic_root_column_blocks(page,
                                                                    static_cast<uint32_t>(column.primary_index()),
                                                                    get_column(column).type(),
                                                                    prefetch_blocks)) {
                            return false;
                        }
                    }
                }
                const auto prefetched = prefetch_and_pin_pax_blocks(*this, prefetch_blocks, block_cache);
                if (prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_generic_prefetched_block_count_.fetch_add(prefetched, std::memory_order_relaxed);
                }

                validate_chunk_capacity(result, result_offset + visible_count);
                if (visible_count == max_count) {
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        const auto out_idx = column.primary_index();
                        if (out_idx >= result.data.size()) {
                            return false;
                        }
                        if (is_unprojected_placeholder(result.data[out_idx])) {
                            continue;
                        }
                        if (!result.data[out_idx].data()) {
                            return false;
                        }
                        if (!decode_pax_generic_root_window(*this,
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
                        apply_pax_committed_updates(get_column(column),
                                                    current_row,
                                                    max_count,
                                                    result.data[out_idx],
                                                    result_offset);
                    }
                } else {
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        const auto out_idx = column.primary_index();
                        if (out_idx >= result.data.size()) {
                            return false;
                        }
                        if (is_unprojected_placeholder(result.data[out_idx])) {
                            continue;
                        }
                        if (!result.data[out_idx].data()) {
                            return false;
                        }
                        vector::vector_t temp_values(result.resource(), get_column(column).type(), max_count);
                        if (!decode_pax_generic_root_window(*this,
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
                        apply_pax_committed_updates(get_column(column), current_row, max_count, temp_values);
                        vector::vector_ops::copy(temp_values,
                                                 result.data[out_idx],
                                                 visible_indexing,
                                                 visible_count,
                                                 0,
                                                 result_offset);
                    }
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < visible_count; i++) {
                    const auto row_offset = visible_count == max_count ? i : visible_indexing.get_index(i);
                    row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + row_offset);
                }
                mark_vector_range_valid(result.row_ids, result_offset, visible_count);
                if (!fill_projected_row_id_columns(column_ids, result, result_offset, visible_count)) {
                    return false;
                }

                result.set_cardinality(result_offset + visible_count);
                state.valid_indexing = visible_count == max_count
                                           ? vector::indexing_vector_t(result.resource(), 0, result.capacity())
                                           : visible_indexing;
                state.vector_index++;
                set_pax_scan_row_offset(*this, state, current_row + max_count);
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                    column_state.result_offset += visible_count;
                }
                return true;
            }

            vector::indexing_vector_t indexing(result.resource(), max_count);
            uint64_t approved_count = 0;

            for (const auto& page_window : page_windows) {
                const auto& page = *page_window.page;
                const auto page_filter_result =
                    single_column_conjunction_filter
                        ? (get_column(generic_filter_columns.front()).has_updates()
                               ? filter_propagate_result_t::NO_PRUNING_POSSIBLE
                               : check_pax_generic_page_statistics(page,
                                                                   generic_filter_columns.front(),
                                                                   generic_simple_filters))
                        : check_pax_generic_filter_tree_statistics(
                              page,
                              filter,
                              [&](uint32_t column_index) {
                                  return get_column(column_index).has_updates();
                              });
                if (page_filter_result == filter_propagate_result_t::ALWAYS_FALSE) {
                    if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                        pax_generic_pruned_page_count_.fetch_add(1, std::memory_order_relaxed);
                        pax_generic_skipped_payload_page_count_.fetch_add(1, std::memory_order_relaxed);
                    }
                    continue;
                }

                std::vector<uint64_t> filter_prefetch_blocks;
                for (const auto column_index : generic_filter_columns) {
                    if (!collect_pax_generic_root_column_blocks(page,
                                                                column_index,
                                                                get_column(column_index).type(),
                                                                filter_prefetch_blocks)) {
                        return false;
                    }
                }
                const auto filter_prefetched =
                    prefetch_and_pin_pax_blocks(*this, filter_prefetch_blocks, block_cache);
                if (filter_prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_generic_prefetched_block_count_.fetch_add(filter_prefetched, std::memory_order_relaxed);
                }

                std::vector<pax_generic_decoded_filter_column_t> decoded_filter_columns;
                decoded_filter_columns.reserve(generic_filter_columns.size());
                for (const auto column_index : generic_filter_columns) {
                    auto values = std::make_unique<vector::vector_t>(result.resource(),
                                                                     get_column(column_index).type(),
                                                                     page_window.page_count);
                    if (!decode_pax_generic_root_window(*this,
                                                        layout,
                                                        column_index,
                                                        get_column(column_index).type(),
                                                        page_window.overlap_start,
                                                        page_window.page_count,
                                                        *values,
                                                        0,
                                                        block_cache)) {
                        return false;
                    }
                    apply_pax_committed_updates(get_column(column_index),
                                                page_window.overlap_start,
                                                page_window.page_count,
                                                *values);
                    decoded_filter_columns.push_back({column_index, std::move(values)});
                }

                vector::indexing_vector_t page_indexing(result.resource(), page_window.page_count);
                const auto page_approved_count =
                    single_column_conjunction_filter
                        ? apply_pax_generic_root_filter_list(*decoded_filter_columns.front().values,
                                                             generic_simple_filters,
                                                             page_window.page_count,
                                                             page_indexing)
                    : conjunction_filter
                        ? apply_pax_generic_root_filter_conjunction(generic_simple_filters,
                                                                    decoded_filter_columns,
                                                                    page_window.page_count,
                                                                    page_indexing)
                        : apply_pax_generic_root_filter_tree(filter,
                                                             generic_simple_filters,
                                                             decoded_filter_columns,
                                                             page_window.page_count,
                                                             page_indexing);
                const auto visible_page_approved_count =
                    apply_pax_visibility_mask(visible_mask,
                                              page_window.page_offset_in_window,
                                              page_indexing,
                                              page_approved_count);
                if (visible_page_approved_count == 0) {
                    if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                        pax_generic_skipped_payload_page_count_.fetch_add(1, std::memory_order_relaxed);
                    }
                    continue;
                }

                std::vector<uint64_t> projected_prefetch_blocks;
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        continue;
                    }
                    if (find_pax_generic_decoded_filter_column(decoded_filter_columns,
                                                               static_cast<uint32_t>(column.primary_index()))) {
                        continue;
                    }
                    if (!collect_pax_generic_root_column_blocks(page,
                                                                static_cast<uint32_t>(column.primary_index()),
                                                                get_column(column).type(),
                                                                projected_prefetch_blocks)) {
                        return false;
                    }
                }
                const auto projected_prefetched =
                    prefetch_and_pin_pax_blocks(*this, projected_prefetch_blocks, block_cache);
                if (projected_prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_generic_prefetched_block_count_.fetch_add(projected_prefetched, std::memory_order_relaxed);
                }

                validate_chunk_capacity(result, result_offset + approved_count + visible_page_approved_count);
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        continue;
                    }
                    const auto out_idx = column.primary_index();
                    if (out_idx >= result.data.size()) {
                        return false;
                    }
                    if (is_unprojected_placeholder(result.data[out_idx])) {
                        continue;
                    }
                    if (!result.data[out_idx].data()) {
                        return false;
                    }

                    if (const auto* decoded_values =
                            find_pax_generic_decoded_filter_column(decoded_filter_columns,
                                                                   static_cast<uint32_t>(column.primary_index()))) {
                        vector::vector_ops::copy(*decoded_values,
                                                 result.data[out_idx],
                                                 page_indexing,
                                                 visible_page_approved_count,
                                                 0,
                                                 result_offset + approved_count);
                        continue;
                    }

                    vector::vector_t temp_values(result.resource(), get_column(column).type(), page_window.page_count);
                    if (!decode_pax_generic_root_window(*this,
                                                        layout,
                                                        static_cast<uint32_t>(column.primary_index()),
                                                        get_column(column).type(),
                                                        page_window.overlap_start,
                                                        page_window.page_count,
                                                        temp_values,
                                                        0,
                                                        block_cache)) {
                        return false;
                    }
                    apply_pax_committed_updates(get_column(column),
                                                page_window.overlap_start,
                                                page_window.page_count,
                                                temp_values);
                    vector::vector_ops::copy(temp_values,
                                             result.data[out_idx],
                                             page_indexing,
                                             visible_page_approved_count,
                                             0,
                                             result_offset + approved_count);
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < visible_page_approved_count; i++) {
                    const auto row_offset = page_window.page_offset_in_window + page_indexing.get_index(i);
                    indexing.set_index(approved_count + i, row_offset);
                    row_ids[result_offset + approved_count + i] =
                        start + static_cast<int64_t>(current_row + row_offset);
                }
                mark_vector_range_valid(result.row_ids,
                                        result_offset + approved_count,
                                        visible_page_approved_count);
                if (!fill_projected_row_id_columns(column_ids,
                                                   result,
                                                   result_offset + approved_count,
                                                   visible_page_approved_count)) {
                    return false;
                }

                approved_count += visible_page_approved_count;
            }

            state.vector_index++;
            set_pax_scan_row_offset(*this, state, current_row + max_count);
            for (auto& column_state : state.column_scans) {
                advance_pax_fixed_scan_state(column_state, max_count);
                column_state.result_offset += approved_count;
            }
            if (approved_count == 0) {
                continue;
            }

            result.set_cardinality(result_offset + approved_count);
            state.valid_indexing = indexing;
            return true;
        }
    }

	    bool row_group_t::try_scan_pax_fixed_projected(collection_scan_state& state, vector::data_chunk_t& result) {
	        const bool has_persisted_pax_layout =
	            layout_kind_ == storage::row_group_layout_kind::PAX_FIXED && pax_fixed_layout_.has_value();
	        if (!has_persisted_pax_layout) {
	            return false;
	        }
	        const bool transaction_scan = state.txn.transaction_id != 0 || state.txn.start_time != 0;
		        const bool apply_visibility_filter = requires_pax_version_visibility(transaction_scan);

        const auto& column_ids = state.column_ids();
        if (column_ids.empty()) {
            return false;
        }

        auto* filter = state.filter();
        std::vector<const table_filter_t*> fixed_simple_filters;
        std::vector<uint32_t> fixed_filter_columns;
        if (filter && !collect_pax_fixed_filter_tree(filter, fixed_simple_filters, fixed_filter_columns)) {
            return false;
        }
        const bool has_filter = filter != nullptr;
        const bool conjunction_filter = has_filter && is_pax_fixed_conjunction_filter_tree(filter);
        const bool single_column_conjunction_filter = conjunction_filter && fixed_filter_columns.size() == 1;

        std::vector<uint32_t> required_fixed_columns;
        for (const auto& column_index : fixed_filter_columns) {
            append_unique_column_index(required_fixed_columns, column_index);
        }

        for (const auto& column : column_ids) {
            if (column.has_children()) {
                return false;
            }
            if (column.is_row_id_column()) {
                continue;
            }
            auto column_index = column.primary_index();
            if (column_index >= get_column_count()) {
                return false;
            }
	            auto& column_data = get_column(column_index);
	            if (!is_pax_fixed_projected_type(column_data.type())) {
	                return false;
	            }
	            if (transaction_scan && column_data.has_uncommitted_updates()) {
	                return false;
	            }
	            append_unique_column_index(required_fixed_columns, static_cast<uint32_t>(column_index));
	        }

	        for (const auto column_index : fixed_filter_columns) {
            if (column_index >= get_column_count()) {
                return false;
            }
	            auto& filter_column = get_column(column_index);
	            if (!is_pax_fixed_projected_type(filter_column.type())) {
	                return false;
	            }
	            if (transaction_scan && filter_column.has_uncommitted_updates()) {
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
        const auto pax_tuple_count = pax_fixed_layout_tuple_count(layout);
        const auto row_group_tuple_count = count.load();
        if (expected_row_offset != pax_tuple_count || pax_tuple_count > row_group_tuple_count) {
            return false;
        }
        const auto row_group_scan_limit =
            start > state.max_row ? uint64_t(0)
                                  : static_cast<uint64_t>(std::min(static_cast<int64_t>(row_group_tuple_count),
                                                                   state.max_row - start));
        if (row_group_scan_limit > pax_tuple_count &&
            pax_tuple_count % vector::DEFAULT_VECTOR_CAPACITY != 0) {
            return false;
        }

        for (const auto column_index : required_fixed_columns) {
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
        const auto local_max_row_group_row = std::min(row_group_scan_limit, pax_tuple_count);

        // One reusable decode buffer per distinct column, shared across all pages of this scan.
        // Capacity is the full vector capacity so any page window fits. Validity carries over
        // between uses, so reset it to all-valid on each hand-out before the decoder re-applies it.
        std::unordered_map<uint32_t, std::unique_ptr<vector::vector_t>> decode_buffers;
        auto decode_buffer_for = [&](uint32_t column) -> vector::vector_t& {
            auto it = decode_buffers.find(column);
            if (it == decode_buffers.end()) {
                it = decode_buffers
                         .emplace(column,
                                  std::make_unique<vector::vector_t>(result.resource(),
                                                                     get_column(column).type(),
                                                                     vector::DEFAULT_VECTOR_CAPACITY))
                         .first;
            }
            auto& buffer = *it->second;
            buffer.validity().reset();
            return buffer;
        };

        // Whether a filter column carries committed updates is fixed for the whole scan, so
        // compute it once here rather than calling has_updates() per leaf per page below.
        std::unordered_map<uint32_t, bool> filter_column_has_updates;
        for (const auto column_index : fixed_filter_columns) {
            filter_column_has_updates.emplace(column_index,
                                              get_column(column_index).has_updates());
        }
        auto column_has_updates_cached = [&](uint32_t column_index) -> bool {
            auto it = filter_column_has_updates.find(column_index);
            if (it != filter_column_has_updates.end()) {
                return it->second;
            }
            return get_column(column_index).has_updates();
        };

        while (true) {
            const auto current_row = current_pax_fixed_row_offset(*this, state);
            if (current_row >= local_max_row_group_row) {
                if (row_group_scan_limit > pax_tuple_count && current_row >= pax_tuple_count) {
                    return false;
                }
                return true;
            }

            const auto max_count =
                std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, local_max_row_group_row - current_row);

            if (!check_zonemap_segments(state)) {
                continue;
            }

            vector::indexing_vector_t visible_indexing(result.resource(), max_count);
            const auto visible_count = apply_visibility_filter
                                           ? pax_visibility_indexing(state,
                                                                    current_row,
                                                                    max_count,
                                                                    visible_indexing,
                                                                    transaction_scan)
                                           : max_count;
            if (visible_count == 0) {
                state.vector_index++;
                set_pax_scan_row_offset(*this, state, current_row + max_count);
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                }
                continue;
            }
            const auto visible_mask = visible_count == max_count
                                          ? std::vector<uint8_t>{}
                                          : build_pax_visibility_mask(visible_indexing, visible_count, max_count);

            const auto window_end = current_row + max_count;
            const auto page_windows = collect_pax_page_windows(layout.pages, current_row, window_end);
            const auto result_offset = result.size();

            if (!has_filter) {
                std::vector<uint64_t> prefetch_blocks;
                for (const auto& page_window : page_windows) {
                    const auto& page = *page_window.page;
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        if (!collect_pax_fixed_page_column_blocks(page,
                                                                  static_cast<uint32_t>(column.primary_index()),
                                                                  prefetch_blocks)) {
                            return false;
                        }
                    }
                }
                const auto prefetched = prefetch_and_pin_pax_blocks(*this, prefetch_blocks, block_cache);
                if (prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_fixed_prefetched_block_count_.fetch_add(prefetched, std::memory_order_relaxed);
                }

                validate_chunk_capacity(result, result_offset + visible_count);
                if (visible_count == max_count) {
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        const auto out_idx = column.primary_index();
                        if (out_idx >= result.data.size()) {
                            return false;
                        }
                        if (is_unprojected_placeholder(result.data[out_idx])) {
                            continue;
                        }
                        if (!result.data[out_idx].data()) {
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
                        apply_pax_committed_updates(get_column(column),
                                                    current_row,
                                                    max_count,
                                                    result.data[out_idx],
                                                    result_offset);
                    }
                } else {
                    for (const auto& column : column_ids) {
                        if (column.is_row_id_column()) {
                            continue;
                        }
                        const auto out_idx = column.primary_index();
                        if (out_idx >= result.data.size()) {
                            return false;
                        }
                        if (is_unprojected_placeholder(result.data[out_idx])) {
                            continue;
                        }
                        if (!result.data[out_idx].data()) {
                            return false;
                        }
                        auto& temp_values = decode_buffer_for(static_cast<uint32_t>(column.primary_index()));
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
                        apply_pax_committed_updates(get_column(column), current_row, max_count, temp_values);
                        vector::vector_ops::copy(temp_values,
                                                 result.data[out_idx],
                                                 visible_indexing,
                                                 visible_count,
                                                 0,
                                                 result_offset);
                    }
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < visible_count; i++) {
                    const auto row_offset = visible_count == max_count ? i : visible_indexing.get_index(i);
                    row_ids[result_offset + i] = start + static_cast<int64_t>(current_row + row_offset);
                }
                mark_vector_range_valid(result.row_ids, result_offset, visible_count);
                if (!fill_projected_row_id_columns(column_ids, result, result_offset, visible_count)) {
                    return false;
                }

                result.set_cardinality(result_offset + visible_count);
                state.valid_indexing = visible_count == max_count
                                           ? vector::indexing_vector_t(result.resource(), 0, result.capacity())
                                           : visible_indexing;
                state.vector_index++;
                set_pax_scan_row_offset(*this, state, current_row + max_count);
                for (auto& column_state : state.column_scans) {
                    advance_pax_fixed_scan_state(column_state, max_count);
                    column_state.result_offset += visible_count;
                }
                return true;
            }

            vector::indexing_vector_t indexing(result.resource(), max_count);
            uint64_t approved_count = 0;

            for (const auto& page_window : page_windows) {
                const auto& page = *page_window.page;
                const auto page_filter_result =
                    single_column_conjunction_filter
                        ? (column_has_updates_cached(fixed_filter_columns.front())
                               ? filter_propagate_result_t::NO_PRUNING_POSSIBLE
                               : check_pax_fixed_page_statistics(page,
                                                                 fixed_filter_columns.front(),
                                                                 fixed_simple_filters))
                        : check_pax_fixed_filter_tree_statistics(page, filter, column_has_updates_cached);
                if (page_filter_result == filter_propagate_result_t::ALWAYS_FALSE) {
                    if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                        pax_fixed_pruned_page_count_.fetch_add(1, std::memory_order_relaxed);
                        pax_fixed_skipped_payload_page_count_.fetch_add(1, std::memory_order_relaxed);
                    }
                    continue;
                }

                std::vector<uint64_t> filter_prefetch_blocks;
                for (const auto column_index : fixed_filter_columns) {
                    if (!collect_pax_fixed_page_column_blocks(page, column_index, filter_prefetch_blocks)) {
                        return false;
                    }
                }
                const auto filter_prefetched =
                    prefetch_and_pin_pax_blocks(*this, filter_prefetch_blocks, block_cache);
                if (filter_prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_fixed_prefetched_block_count_.fetch_add(filter_prefetched, std::memory_order_relaxed);
                }

                std::vector<pax_fixed_decoded_filter_column_t> decoded_filter_columns;
                decoded_filter_columns.reserve(fixed_filter_columns.size());
                for (const auto column_index : fixed_filter_columns) {
                    auto& values = decode_buffer_for(column_index);
                    if (!decode_pax_fixed_window(*this,
                                                 layout,
                                                 column_index,
                                                 get_column(column_index).type(),
                                                 page_window.overlap_start,
                                                 page_window.page_count,
                                                 values,
                                                 0,
                                                 block_cache)) {
                        return false;
                    }
                    apply_pax_committed_updates(get_column(column_index),
                                                page_window.overlap_start,
                                                page_window.page_count,
                                                values);
                    decoded_filter_columns.push_back({column_index, &values});
                }

                vector::indexing_vector_t page_indexing(result.resource(), page_window.page_count);
                const auto page_approved_count =
                    single_column_conjunction_filter
                        ? apply_pax_fixed_filter_list(*decoded_filter_columns.front().values,
                                                      fixed_simple_filters,
                                                      page_window.page_count,
                                                      page_indexing)
                    : conjunction_filter
                        ? apply_pax_fixed_filter_conjunction(fixed_simple_filters,
                                                             decoded_filter_columns,
                                                             page_window.page_count,
                                                             page_indexing)
                        : apply_pax_fixed_filter_tree(filter,
                                                      fixed_simple_filters,
                                                      decoded_filter_columns,
                                                      page_window.page_count,
                                                      page_indexing);
                const auto visible_page_approved_count =
                    apply_pax_visibility_mask(visible_mask,
                                              page_window.page_offset_in_window,
                                              page_indexing,
                                              page_approved_count);
                if (visible_page_approved_count == 0) {
                    if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                        pax_fixed_skipped_payload_page_count_.fetch_add(1, std::memory_order_relaxed);
                    }
                    continue;
                }

                std::vector<uint64_t> projected_prefetch_blocks;
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        continue;
                    }
                    if (find_pax_fixed_decoded_filter_column(decoded_filter_columns,
                                                             static_cast<uint32_t>(column.primary_index()))) {
                        continue;
                    }
                    if (!collect_pax_fixed_page_column_blocks(page,
                                                              static_cast<uint32_t>(column.primary_index()),
                                                              projected_prefetch_blocks)) {
                        return false;
                    }
                }
                const auto projected_prefetched =
                    prefetch_and_pin_pax_blocks(*this, projected_prefetch_blocks, block_cache);
                if (projected_prefetched > 0 && scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                    pax_fixed_prefetched_block_count_.fetch_add(projected_prefetched, std::memory_order_relaxed);
                }

                validate_chunk_capacity(result, result_offset + approved_count + visible_page_approved_count);
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        continue;
                    }
                    const auto out_idx = column.primary_index();
                    if (out_idx >= result.data.size()) {
                        return false;
                    }
                    if (is_unprojected_placeholder(result.data[out_idx])) {
                        continue;
                    }
                    if (!result.data[out_idx].data()) {
                        return false;
                    }

                    if (const auto* decoded_values =
                            find_pax_fixed_decoded_filter_column(decoded_filter_columns,
                                                                 static_cast<uint32_t>(column.primary_index()))) {
                        vector::vector_ops::copy(*decoded_values,
                                                 result.data[out_idx],
                                                 page_indexing,
                                                 visible_page_approved_count,
                                                 0,
                                                 result_offset + approved_count);
                        continue;
                    }

                    auto& temp_values = decode_buffer_for(static_cast<uint32_t>(column.primary_index()));
                    if (!decode_pax_fixed_window(*this,
                                                 layout,
                                                 static_cast<uint32_t>(column.primary_index()),
                                                 get_column(column).type(),
                                                 page_window.overlap_start,
                                                 page_window.page_count,
                                                 temp_values,
                                                 0,
                                                 block_cache)) {
                        return false;
                    }
                    apply_pax_committed_updates(get_column(column),
                                                page_window.overlap_start,
                                                page_window.page_count,
                                                temp_values);
                    vector::vector_ops::copy(temp_values,
                                             result.data[out_idx],
                                             page_indexing,
                                             visible_page_approved_count,
                                             0,
                                             result_offset + approved_count);
                }

                result.row_ids.set_vector_type(vector::vector_type::FLAT);
                auto* row_ids = result.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < visible_page_approved_count; i++) {
                    const auto row_offset = page_window.page_offset_in_window + page_indexing.get_index(i);
                    indexing.set_index(approved_count + i, row_offset);
                    row_ids[result_offset + approved_count + i] =
                        start + static_cast<int64_t>(current_row + row_offset);
                }
                mark_vector_range_valid(result.row_ids,
                                        result_offset + approved_count,
                                        visible_page_approved_count);
                if (!fill_projected_row_id_columns(column_ids,
                                                   result,
                                                   result_offset + approved_count,
                                                   visible_page_approved_count)) {
                    return false;
                }

                approved_count += visible_page_approved_count;
            }

            state.vector_index++;
            set_pax_scan_row_offset(*this, state, current_row + max_count);
            for (auto& column_state : state.column_scans) {
                advance_pax_fixed_scan_state(column_state, max_count);
                column_state.result_offset += approved_count;
            }
            if (approved_count == 0) {
                continue;
            }

            result.set_cardinality(result_offset + approved_count);
            state.valid_indexing = indexing;
            return true;
        }
    }

    template<table_scan_type TYPE>
    void row_group_t::templated_scan(collection_scan_state& state, vector::data_chunk_t& result) {
        constexpr bool ALLOW_UPDATES = TYPE != table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES;
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
            // valid_indexing is reused across row groups and an earlier projected scan may have left
            // it undersized. The visibility path below writes up to max_count entries, so ensure
            // capacity first.
            if (state.valid_indexing.capacity() < max_count) {
                state.valid_indexing = vector::indexing_vector_t(result.resource(), vector::DEFAULT_VECTOR_CAPACITY);
            }
            const auto version_vector_idx = current_version_vector_index(*this, state);
            if (TYPE == table_scan_type::REGULAR) {
                // REGULAR scans have no see-all fallback: state.txn must be a real
                // transaction_data, as its snapshot fields drive MVCC visibility.
                // version_vector_idx maps state.vector_index to the row-group-relative
                // (or absolute) version index expected by the version manager.
                count = state.row_group->indexing_vector(state.txn,
                                                         version_vector_idx,
                                                         state.valid_indexing,
                                                         max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else {
                count = max_count;
            }
            const int64_t row_id_base = current_regular_row_id_base(*this, state);
            validate_chunk_capacity(result, result.size() + count);

            if (count == max_count && !filter) {
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    // Write into the output slot corresponding to the storage column index
                    // (for row_id column, write into slot i as the caller expects it there).
                    size_t out_idx = column.is_row_id_column() ? i : column.primary_index();
                    if (column.is_row_id_column()) {
                        assert(result.data[out_idx].type().type() == types::logical_type::BIGINT);
                        result.data[out_idx].sequence(row_id_base, 1, count);
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
                    filter_indexing(collection_->resource(), row_id_base, indexing, filter, approved_tuple_count);
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
                            result_data[indexing_idx] = row_id_base + static_cast<int64_t>(indexing.get_index(indexing_idx));
                        }
                    } else {
                        auto& col_data = get_column(column);
                        vector::vector_t scan_vector(result.resource(), result.data[out_idx].type(), max_count);
                        auto prev_offset = state.column_scans[i].result_offset;
                        state.column_scans[i].result_offset = 0;
                        if (TYPE == table_scan_type::REGULAR) {
                            col_data.scan(state.vector_index, state.column_scans[i], scan_vector, max_count);
                            state.column_scans[i].result_offset = prev_offset;
                            vector::vector_ops::copy(scan_vector,
                                                     result.data[out_idx],
                                                     indexing,
                                                     approved_tuple_count,
                                                     0,
                                                     state.column_scans[i].result_offset);
                        } else {
                            col_data.scan_committed(state.vector_index,
                                                    state.column_scans[i],
                                                    scan_vector,
                                                    ALLOW_UPDATES,
                                                    max_count);
                            state.column_scans[i].result_offset = prev_offset;
                            vector::vector_ops::copy(scan_vector,
                                                     result.data[out_idx],
                                                     indexing,
                                                     approved_tuple_count,
                                                     0,
                                                     state.column_scans[i].result_offset);
                        }
                    }
                }

                assert(approved_tuple_count > 0);
                count = approved_tuple_count;
                state.valid_indexing = indexing;
            }
            auto* row_ids_data = result.row_ids.data<int64_t>();
            const uint64_t write_start = result.size();
            for (uint64_t i = 0; i < count; i++) {
                row_ids_data[write_start + i] = row_id_base + static_cast<int64_t>(state.valid_indexing.get_index(i));
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
            if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                pax_generic_projected_scan_count_.fetch_add(1, std::memory_order_relaxed);
            }
            return;
        }
        if (try_scan_pax_fixed_projected(state, result)) {
            if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
                pax_fixed_projected_scan_count_.fetch_add(1, std::memory_order_relaxed);
            }
            return;
        }
        if (scan_path_counts_enabled_.load(std::memory_order_relaxed)) {
            regular_scan_count_.fetch_add(1, std::memory_order_relaxed);
        }
        templated_scan<table_scan_type::REGULAR>(state, result);
    }

    void row_group_t::reset_scan_path_counts() {
        scan_path_counts_enabled_.store(true, std::memory_order_relaxed);
        pax_generic_projected_scan_count_.store(0, std::memory_order_relaxed);
        pax_generic_pruned_page_count_.store(0, std::memory_order_relaxed);
        pax_generic_prefetched_block_count_.store(0, std::memory_order_relaxed);
        pax_generic_skipped_payload_page_count_.store(0, std::memory_order_relaxed);
        pax_fixed_projected_scan_count_.store(0, std::memory_order_relaxed);
        pax_fixed_pruned_page_count_.store(0, std::memory_order_relaxed);
        pax_fixed_prefetched_block_count_.store(0, std::memory_order_relaxed);
        pax_fixed_skipped_payload_page_count_.store(0, std::memory_order_relaxed);
        regular_scan_count_.store(0, std::memory_order_relaxed);
    }

    row_group_scan_path_counts_t row_group_t::scan_path_counts() const {
        return {pax_generic_projected_scan_count_.load(std::memory_order_relaxed),
                pax_generic_pruned_page_count_.load(std::memory_order_relaxed),
                pax_generic_prefetched_block_count_.load(std::memory_order_relaxed),
                pax_generic_skipped_payload_page_count_.load(std::memory_order_relaxed),
                pax_fixed_projected_scan_count_.load(std::memory_order_relaxed),
                pax_fixed_pruned_page_count_.load(std::memory_order_relaxed),
                pax_fixed_prefetched_block_count_.load(std::memory_order_relaxed),
                pax_fixed_skipped_payload_page_count_.load(std::memory_order_relaxed),
                regular_scan_count_.load(std::memory_order_relaxed)};
    }

	    uint64_t row_group_t::pax_visibility_indexing(const collection_scan_state& state,
	                                                  uint64_t row_offset_in_group,
	                                                  uint64_t max_count,
	                                                  vector::indexing_vector_t& result_indexing,
	                                                  bool transaction_scan) {
        if (version_info() == nullptr) {
            return max_count;
        }

        uint64_t visible_count = 0;
        uint64_t consumed = 0;
        auto absolute_row = static_cast<uint64_t>(start) + row_offset_in_group;
        while (consumed < max_count) {
            const auto vector_idx = absolute_row / vector::DEFAULT_VECTOR_CAPACITY;
            const auto offset_in_vector = absolute_row % vector::DEFAULT_VECTOR_CAPACITY;
            const auto chunk_count =
                std::min<uint64_t>(max_count - consumed, vector::DEFAULT_VECTOR_CAPACITY - offset_in_vector);
            const auto prefix_count = offset_in_vector + chunk_count;

            vector::indexing_vector_t prefix_indexing(result_indexing.resource(), prefix_count);
            // Non-transaction (committed) scans want every committed row: a
            // default-constructed transaction_data is the see-all-committed
            // snapshot (horizon = UINT64_MAX, empty in-flight set) under the
            // snapshot-horizon visibility model.
            const auto prefix_visible_count =
                transaction_scan ? indexing_vector(state.txn, vector_idx, prefix_indexing, prefix_count)
                                 : indexing_vector(transaction_data{}, vector_idx, prefix_indexing, prefix_count);

            if (prefix_visible_count == prefix_count) {
                for (uint64_t i = 0; i < chunk_count; i++) {
                    result_indexing.set_index(visible_count++, consumed + i);
                }
            } else {
                const auto window_end = offset_in_vector + chunk_count;
                for (uint64_t i = 0; i < prefix_visible_count; i++) {
                    const auto idx = prefix_indexing.get_index(i);
                    if (idx < offset_in_vector || idx >= window_end) {
                        continue;
                    }
                    const auto local_idx = consumed + idx - offset_in_vector;
                    result_indexing.set_index(visible_count++, local_idx);
                }
            }

            consumed += chunk_count;
            absolute_row += chunk_count;
        }
	        return visible_count;
	    }

	    bool row_group_t::requires_pax_version_visibility(bool transaction_scan) {
	        auto* vinfo = version_info();
	        if (!vinfo) {
	            return false;
	        }
	        return transaction_scan ? vinfo->has_version_entries() : vinfo->has_visibility_changes();
	    }

    void row_group_t::scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        switch (type) {
            case table_scan_type::COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES:
                templated_scan<table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES>(state, result);
                break;
            case table_scan_type::LATEST_COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            default:
                assert(false && "Unrecognized table scan type");
                std::abort();
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

        auto vector_idx = static_cast<uint64_t>(row_id) / vector::DEFAULT_VECTOR_CAPACITY;
        auto row_in_vector = static_cast<uint64_t>(row_id) % vector::DEFAULT_VECTOR_CAPACITY;
        vector::indexing_vector_t visible_rows(collection().resource(), row_in_vector + 1);

        // A (0, 0) txn is the see-all-committed sentinel (no MVCC snapshot). Under
        // the snapshot-horizon visibility model, a default-constructed transaction_data
        // already means "see every committed row" (horizon = UINT64_MAX, empty
        // in-flight set), so the committed path just uses that see-all snapshot.
        transaction_data visibility_txn = txn;
        if (txn.transaction_id == 0 && txn.start_time == 0) {
            visibility_txn = transaction_data{};
        }
        uint64_t visible_count = indexing_vector(visibility_txn, vector_idx, visible_rows, row_in_vector + 1);
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
        if (count > 0) {
            mark_dirty();
        }
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
        mark_dirty();
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
        if (append_count > 0) {
            mark_dirty();
        }
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
        if (count > 0 && !column_ids.empty()) {
            mark_dirty();
        }
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
        if (updates.size() > 0) {
            mark_dirty();
        }
        auto& col_data = get_column(primary_column_idx);

        col_data.update_column(column_path, updates.data[0], ids, updates.size(), 1);
    }

    uint64_t row_group_t::committed_row_count() {
        auto* vi = version_info_.load();
        if (vi) {
            const auto version_deleted = std::min<uint64_t>(count.load(), vi->committed_deleted_count(count));
            return count - version_deleted;
        }
        return count;
    }

    bool row_group_t::has_persisted_pax_layout() const {
        return persisted_pointer_.has_value() &&
               (layout_kind_ == storage::row_group_layout_kind::PAX_FIXED ||
                layout_kind_ == storage::row_group_layout_kind::PAX_GENERIC);
    }

    bool row_group_t::can_append_mutable_tail() const {
        return !has_persisted_pax_layout();
    }

#if defined(DEV_MODE)
    void row_group_t::debug_set_unloaded_deletes_for_test(bool enabled) {
        if (enabled) {
            if (deletes_pointers_.empty()) {
                deletes_pointers_.emplace_back();
            }
            deletes_is_loaded_ = false;
            return;
        }
        deletes_pointers_.clear();
        deletes_is_loaded_ = true;
    }
#endif

    bool row_group_t::supports_threaded_scan() const {
        auto* version_info = const_cast<row_group_t*>(this)->version_info();
        if (version_info && !version_info->supports_threaded_scan()) {
            return false;
        }
        for (const auto& column : columns_) {
            if (column && column->has_uncommitted_updates()) {
                return false;
            }
        }
        return true;
    }

    bool row_group_t::has_version_above(uint64_t watermark) {
        auto* vi = version_info_.load();
        if (!vi) {
            // No version info — every row is plain committed, visible to all.
            return false;
        }
        return vi->has_version_above(watermark, count);
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
        const auto delete_id = ++current_version_;
        auto deleted = get_or_create_version_info().delete_rows(vector_idx, delete_id, rows, count);
        if (deleted > 0) {
            mark_dirty();
        }
        ++current_version_;
        return deleted;
    }

    uint64_t row_group_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id) {
        const bool is_txn = transaction_id != 0;
        version_delete_state del_state(*this, transaction_id, table, start, is_txn);

        for (uint64_t i = 0; i < count; i++) {
            assert(ids[i] >= 0);
            assert(ids[i] >= start && ids[i] < start + static_cast<int64_t>(this->count));
            del_state.delete_row(ids[i]);
        }
        del_state.flush();
        if (del_state.delete_count > 0) {
            mark_dirty();
        }
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
        // are reflected in see-all-committed scans
        if (commit_id >= current_version_) {
            current_version_ = commit_id + 1;
        }
    }

    void row_group_t::revert_all_deletes(uint64_t txn_id) {
        auto vinfo = version_info();
        if (vinfo) {
            vinfo->revert_all_deletes(txn_id);
        }
        // No current_version_ advance: revert un-marks pending deletes back to
        // NOT_DELETED_ID, restoring visibility. Unlike commit there is no new
        // commit_id to publish, so the version watermark stays where it was.
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
        // Metadata accounting, not a user scan: a UINT64_MAX horizon + empty
        // in_flight set is a see-all snapshot covering every committed row.
        transaction_data td(0, 0);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return indexing_vector(td, index, temp_indexing, count);
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
        auto loaded_info = std::make_shared<row_version_manager_t>(start);
        for (const auto& pointer : deletes_pointers_) {
            if (pointer.compression != compression::compression_type::UNCOMPRESSED) {
                throw std::logic_error("unsupported committed delete snapshot compression");
            }
            if (pointer.segment_size == 0) {
                continue;
            }
            auto source_block = block_manager().register_block(pointer.block_pointer.block_id);
            auto source_handle = block_manager().buffer_manager.pin(source_block);
            auto* source_ptr = checked_pax_block_ptr(source_handle,
                                                     pointer.block_pointer.offset,
                                                     pointer.segment_size,
                                                     block_manager().block_size());
            loaded_info->deserialize_committed_deletes(source_ptr, pointer.segment_size);
        }
        set_version_info(std::move(loaded_info));
        deletes_is_loaded_ = true;
        return version_info_;
    }

    void row_group_t::set_version_info(std::shared_ptr<row_version_manager_t> version) {
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void row_group_t::mark_dirty() {
        is_dirty_ = true;
        persisted_pointer_.reset();
    }

    bool row_group_t::can_reuse_persisted_pointer() {
        if (is_dirty_ || !persisted_pointer_.has_value()) {
            return false;
        }
        if (persisted_pointer_->row_start != static_cast<uint64_t>(start) || persisted_pointer_->tuple_count != count) {
            return false;
        }
        const auto current_policy = block_manager().layout_policy();
        if (persisted_layout_policy_ != current_policy) {
            return false;
        }
        if (current_policy == storage::row_group_layout_policy::COLUMNAR_ONLY &&
            persisted_pointer_->layout_kind != storage::row_group_layout_kind::COLUMNAR) {
            return false;
        }
        return true;
    }

    storage::row_group_pointer_t row_group_t::remember_persisted_pointer(storage::row_group_pointer_t pointer) {
        persisted_layout_policy_ = block_manager().layout_policy();
        deletes_pointers_ = pointer.deletes_pointers;
        deletes_is_loaded_ = true;
        persisted_pointer_ = pointer;
        is_dirty_ = false;
        return pointer;
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
        if (can_reuse_persisted_pointer()) {
            return *persisted_pointer_;
        }

        storage::row_group_pointer_t pointer;
        pointer.row_start = static_cast<uint64_t>(start);
        pointer.tuple_count = count;

        auto col_count = get_column_count();
        pointer.columnar_data_pointers.resize(col_count);
        pointer.columnar_validity_pointers.resize(col_count);

        // Persist a columnar column's validity child alongside its data, otherwise the reopened
        // column reads back all-valid. Only standard columns carry a validity child.
        auto persist_columnar_validity = [&](uint64_t column_index, column_data_t& column) {
            auto* standard_column = dynamic_cast<standard_column_data_t*>(&column);
            if (!standard_column) {
                return;
            }
            auto validity_persistent = standard_column->validity.checkpoint(partial_block_manager);
            pointer.columnar_validity_pointers[column_index] = std::move(validity_persistent.data_pointers);
        };

        auto checkpoint_committed_deletes = [&]() {
            auto* vinfo = version_info();
            if (!vinfo) {
                return;
            }
            auto payload = vinfo->serialize_committed_deletes(count);
            if (payload.empty()) {
                return;
            }
            auto allocation = partial_block_manager.get_block_allocation(payload.size());
            partial_block_manager.write_to_block(allocation.block_id,
                                                 allocation.offset_in_block,
                                                 payload.data(),
                                                 payload.size());

            storage::data_pointer_t pointer_info;
            pointer_info.row_start = static_cast<uint64_t>(start);
            pointer_info.tuple_count = count;
            pointer_info.block_pointer =
                storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
            pointer_info.compression = compression::compression_type::UNCOMPRESSED;
            pointer_info.segment_size = payload.size();
            pointer.deletes_pointers.push_back(pointer_info);
        };

        const bool force_columnar =
            block_manager().layout_policy() == storage::row_group_layout_policy::COLUMNAR_ONLY;
        const bool force_pax = block_manager().layout_policy() == storage::row_group_layout_policy::PAX_ONLY;

        std::vector<uint64_t> pax_generic_columns;
        pax_generic_columns.reserve(col_count);
        std::vector<uint64_t> pax_fixed_columns;
        pax_fixed_columns.reserve(col_count);
        bool pax_generic_requires_v4 = false;
        bool pax_generic_requires_v2 = false;
        bool pax_generic_requires_v3 = false;

        // The columnar checkpoint flushes only a column's own top-level data segments plus its
        // validity child. A nested column's data-bearing children (struct fields, list/array
        // elements) are not persisted, so reject them rather than write a lossy checkpoint.
        auto reject_nested_columnar = [](column_data_t& column) {
            if (dynamic_cast<struct_column_data_t*>(&column) != nullptr ||
                dynamic_cast<list_column_data_t*>(&column) != nullptr ||
                dynamic_cast<array_column_data_t*>(&column) != nullptr) {
                throw std::logic_error("columnar checkpoint cannot persist nested column '" +
                                       column.type().alias() +
                                       "': child columns are not flushed and would be lost on reopen");
            }
        };

        auto checkpoint_columnar_or_throw = [&](uint64_t column_index) {
            auto& column = get_column(column_index);
            if (force_pax) {
                throw std::logic_error("explicit PAX layout cannot persist column '" + column.type().alias() +
                                       "' through the columnar fallback path");
            }
            reject_nested_columnar(column);
            auto persistent = column.checkpoint(partial_block_manager);
            pointer.columnar_data_pointers[column_index] = std::move(persistent.data_pointers);
            persist_columnar_validity(column_index, column);
        };

        for (uint64_t i = 0; i < col_count; i++) {
            auto& column = get_column(i);
            if (force_columnar || components::table::detail::is_explicit_pax_columnar_only_root_type(column.type())) {
                checkpoint_columnar_or_throw(i);
            } else if (is_pax_generic_string_type(column.type())) {
                pax_generic_columns.push_back(i);
                pax_generic_requires_v4 = true;
            } else if (is_pax_generic_struct_type(column.type())) {
                if (!is_supported_pax_generic_column_type(column.type())) {
                    checkpoint_columnar_or_throw(i);
                    continue;
                }
                pax_generic_columns.push_back(i);
                pax_generic_requires_v2 = true;
            } else if (is_pax_generic_collection_type(column.type())) {
                if (!is_supported_pax_generic_column_type(column.type())) {
                    checkpoint_columnar_or_throw(i);
                    continue;
                }
                pax_generic_columns.push_back(i);
                pax_generic_requires_v3 = true;
            } else if (is_pax_fixed_scalar_type(column.type())) {
                pax_fixed_columns.push_back(i);
            } else {
                checkpoint_columnar_or_throw(i);
            }
        }

        if (!pax_generic_columns.empty() && !pax_fixed_columns.empty()) {
            for (auto column_index : pax_fixed_columns) {
                if (std::find(pax_generic_columns.begin(), pax_generic_columns.end(), column_index) ==
                    pax_generic_columns.end()) {
                    pax_generic_columns.push_back(column_index);
                }
            }
            pax_fixed_columns.clear();
            pax_generic_requires_v2 = true;
        }

        if (force_pax && pax_generic_columns.empty() && pax_fixed_columns.empty() && pointer.tuple_count > 0) {
            throw std::logic_error("explicit PAX layout did not find a supported root-column family");
        }

        if (force_pax && pointer.tuple_count > 0) {
            for (uint64_t i = 0; i < col_count; i++) {
                if (!pointer.columnar_data_pointers[i].empty()) {
                    throw std::logic_error("explicit PAX layout wrote unexpected columnar fallback metadata");
                }
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
                checkpoint_columnar_or_throw(i);
            }

            storage::pax_generic_row_group_layout_t pax_layout;
            pax_layout.version =
                pax_generic_requires_v4 ? 4 : (pax_generic_requires_v3 ? 3 : (pax_generic_requires_v2 ? 2 : 1));
            const auto pax_rows_per_page = block_manager().pax_rows_per_page();
            pax_layout.rows_per_page = pax_rows_per_page;

            for (uint64_t row_offset = 0; row_offset < pointer.tuple_count; row_offset += pax_rows_per_page) {
                storage::pax_generic_page_t page;
                page.row_offset_in_group = static_cast<uint32_t>(row_offset);
                page.tuple_count = static_cast<uint32_t>(std::min<uint64_t>(pax_rows_per_page,
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
                        value_slice.statistics = std::move(page_result.statistics);
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
            checkpoint_committed_deletes();
            return remember_persisted_pointer(std::move(pointer));
        }

        if (pax_fixed_columns.empty() || pointer.tuple_count == 0) {
            for (uint64_t i = 0; i < col_count; i++) {
                if (!pointer.columnar_data_pointers[i].empty()) {
                    continue;
                }
                auto& column = get_column(i);
                reject_nested_columnar(column);
                auto persistent = column.checkpoint(partial_block_manager);
                pointer.columnar_data_pointers[i] = std::move(persistent.data_pointers);
                persist_columnar_validity(i, column);
            }
            pointer.layout_kind = storage::row_group_layout_kind::COLUMNAR;
            pax_fixed_layout_.reset();
            pax_generic_layout_.reset();
            layout_kind_ = pointer.layout_kind;
            checkpoint_committed_deletes();
            return remember_persisted_pointer(std::move(pointer));
        }

        storage::pax_fixed_row_group_layout_t pax_layout;
        pax_layout.version = 4;
        const auto pax_rows_per_page = block_manager().pax_rows_per_page();
        pax_layout.rows_per_page = pax_rows_per_page;

        for (uint64_t row_offset = 0; row_offset < pointer.tuple_count; row_offset += pax_rows_per_page) {
            storage::pax_fixed_page_t page;
            page.row_offset_in_group = static_cast<uint32_t>(row_offset);
            page.tuple_count = static_cast<uint32_t>(std::min<uint64_t>(pax_rows_per_page,
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
        checkpoint_committed_deletes();
        return remember_persisted_pointer(std::move(pointer));
    }

    void row_group_t::create_from_pointer(const storage::row_group_pointer_t& pointer) {
        count = pointer.tuple_count;
        layout_kind_ = pointer.layout_kind;
        pax_fixed_layout_ = pointer.pax_fixed_layout;
        pax_generic_layout_ = pointer.pax_generic_layout;
        persisted_pointer_ = pointer;
        persisted_layout_policy_ = block_manager().layout_policy();
        deletes_pointers_ = pointer.deletes_pointers;
        deletes_is_loaded_ = deletes_pointers_.empty();
        is_dirty_ = false;
        auto col_count = get_column_count();
        auto ptrs_count = pointer.columnar_data_pointers.size();
        auto min_count = std::min(col_count, static_cast<uint64_t>(ptrs_count));

        for (uint64_t i = 0; i < min_count; i++) {
            persistent_column_data_t pcd(columns_[i]->resource());
            pcd.data_pointers = pointer.columnar_data_pointers[i];
            // Pass the persisted validity-child pointers as a child column so a columnar column
            // restores its real null mask. Empty for PAX columns (validity comes from the page layout).
            if (i < pointer.columnar_validity_pointers.size() && !pointer.columnar_validity_pointers[i].empty()) {
                auto validity_child = std::make_unique<persistent_column_data_t>(columns_[i]->resource());
                validity_child->data_pointers = pointer.columnar_validity_pointers[i];
                pcd.child_columns.push_back(std::move(validity_child));
            }
            columns_[i]->initialize_column(pcd);
        }

        if (layout_kind_ == storage::row_group_layout_kind::PAX_FIXED && pax_fixed_layout_.has_value()) {
            if (!is_supported_pax_fixed_layout_version(pax_fixed_layout_->version)) {
                throw std::logic_error("unsupported pax_fixed layout version");
            }

            for (uint64_t column_index = 0; column_index < min_count; column_index++) {
                auto* standard_column = dynamic_cast<standard_column_data_t*>(columns_[column_index].get());
                if (!standard_column) {
                    continue;
                }

                auto validity_lock = standard_column->validity.data_.lock();
                for (uint64_t page_index = 0; page_index < pax_fixed_layout_->pages.size(); page_index++) {
                    const auto& page = pax_fixed_layout_->pages[page_index];
                    const auto* slice = find_pax_fixed_slice(page, static_cast<uint32_t>(column_index));
                    if (!slice) {
                        continue;
                    }

                    auto* validity_segment = standard_column->validity.data_.segment_at(validity_lock,
                                                                                        static_cast<int64_t>(page_index));
                    if (!validity_segment) {
                        throw std::logic_error("missing validity segment for pax_fixed load");
                    }

                    auto validity_handle = block_manager().buffer_manager.pin(validity_segment->block);
                    auto* target_ptr = validity_handle.ptr() + validity_segment->block_offset();
                    switch (slice->validity_kind) {
                        case storage::pax_fixed_validity_kind::ALL_VALID:
                            std::memset(target_ptr, 0xFF, validity_segment->segment_size());
                            break;
                        case storage::pax_fixed_validity_kind::ALL_INVALID:
                            std::memset(target_ptr, 0, validity_segment->segment_size());
                            break;
                        case storage::pax_fixed_validity_kind::BITMASK: {
                            if (!slice->validity_data_pointer.has_value()) {
                                throw std::logic_error("missing pax_fixed validity payload");
                            }
                            const uint64_t vsize = slice->validity_data_pointer->segment_size;
                            const uint64_t voffset = slice->validity_data_pointer->block_pointer.offset;
                            // Validate the disk-derived copy length against both the destination
                            // validity buffer and the source block before the memcpy.
                            if (vsize > validity_segment->segment_size() ||
                                voffset > block_manager().block_size() ||
                                voffset + vsize > block_manager().block_size()) {
                                throw std::logic_error("pax_fixed validity payload out of bounds");
                            }
                            auto source_block =
                                block_manager().register_block(slice->validity_data_pointer->block_pointer.block_id);
                            auto source_handle = block_manager().buffer_manager.pin(source_block);
                            auto* source_ptr = source_handle.ptr() + voffset;
                            std::memcpy(target_ptr, source_ptr, vsize);
                            break;
                        }
                        case storage::pax_fixed_validity_kind::RLE:
                        default:
                            throw std::logic_error("invalid pax_fixed validity codec");
                    }
                }
            }
            return;
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

        struct pax_generic_window_t {
            uint64_t row_start;
            uint32_t tuple_count;
        };

        std::vector<pax_generic_window_t> root_windows;
        root_windows.reserve(pax_generic_layout_->pages.size());
        for (const auto& page : pax_generic_layout_->pages) {
            root_windows.push_back({static_cast<uint64_t>(start) + page.row_offset_in_group, page.tuple_count});
        }

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
                const std::vector<uint16_t>& field_path,
                const std::vector<pax_generic_window_t>& windows) -> std::vector<pax_generic_validity_page_info_t> {
            std::vector<pax_generic_validity_page_info_t> infos;
            infos.reserve(windows.size());
            uint64_t window_index = 0;
            for (const auto& page : pax_generic_layout_->pages) {
                const auto* slice =
                    find_pax_generic_slice(page, root_column_index, storage::pax_generic_slice_kind::VALIDITY, field_path);
                if (!slice) {
                    continue;
                }
                if (window_index >= windows.size()) {
                    throw std::logic_error("pax_generic validity window count mismatch");
                }
                infos.push_back(
                    {windows[window_index].row_start, windows[window_index].tuple_count, slice->codec_kind, slice->payload});
                window_index++;
            }
            if (window_index != windows.size()) {
                throw std::logic_error("missing pax_generic validity slices for expected windows");
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
                        const uint64_t vsize = info.payload->main_pointer.segment_size;
                        const uint64_t voffset = info.payload->main_pointer.block_pointer.offset;
                        // Bound the disk-derived copy length against the destination validity buffer
                        // and the source block before the memcpy.
                        if (vsize > validity_segment->segment_size() || voffset > block_manager().block_size() ||
                            voffset + vsize > block_manager().block_size()) {
                            throw std::logic_error("pax_generic validity payload out of bounds");
                        }
                        auto source_block =
                            block_manager().register_block(info.payload->main_pointer.block_pointer.block_id);
                        auto source_handle = block_manager().buffer_manager.pin(source_block);
                        auto* source_ptr = source_handle.ptr() + voffset;
                        std::memcpy(target_ptr, source_ptr, vsize);
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

        std::function<void(column_data_t&,
                           uint32_t,
                           const std::vector<uint16_t>&,
                           bool,
                           const std::vector<pax_generic_window_t>&)>
            load_generic_column;
        load_generic_column = [&](column_data_t& column,
                                  uint32_t root_column_index,
                                  const std::vector<uint16_t>& field_path,
                                  bool is_top_level,
                                  const std::vector<pax_generic_window_t>& windows) {
            if (is_pax_generic_struct_type(column.type())) {
                auto* struct_column = dynamic_cast<struct_column_data_t*>(&column);
                if (!struct_column) {
                    throw std::logic_error("pax_generic struct column is not a struct column");
                }

                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path, windows);
                apply_validity_infos(struct_column->validity, validity_infos, true);
                uint64_t struct_count = 0;
                for (const auto& window : windows) {
                    struct_count += window.tuple_count;
                }
                struct_column->count_ = struct_count;

                for (uint16_t child_index = 0; child_index < struct_column->sub_columns.size(); child_index++) {
                    auto child_path = field_path;
                    child_path.push_back(child_index);
                    load_generic_column(*struct_column->sub_columns[child_index],
                                        root_column_index,
                                        child_path,
                                        false,
                                        windows);
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
                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path, windows);
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

                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path, windows);
                apply_validity_infos(standard_column->validity, validity_infos, false);
                return;
            }

            if (is_pax_generic_collection_type(column.type())) {
                if (column.type().type() == types::logical_type::LIST) {
                    auto* list_column = dynamic_cast<list_column_data_t*>(&column);
                    if (!list_column) {
                        throw std::logic_error("pax_generic list column is not a list column");
                    }

                    auto value_slices =
                        collect_generic_slices(root_column_index, field_path, storage::pax_generic_slice_kind::FIXED_VALUES);
                    if (value_slices.empty()) {
                        throw std::logic_error("missing pax_generic list offset slices");
                    }

                    persistent_column_data_t pcd(list_column->resource());
                    pcd.data_pointers.reserve(value_slices.size());
                    for (const auto* slice : value_slices) {
                        if (slice->codec_kind != storage::pax_generic_codec_kind::FIXED_PLAIN ||
                            !slice->payload.has_value() ||
                            (pax_generic_layout_->version >= 2 && slice->fixed_logical_type != types::logical_type::UBIGINT)) {
                            throw std::logic_error("invalid pax_generic list offset slice payload");
                        }
                        pcd.data_pointers.push_back(slice->payload->main_pointer);
                    }
                    list_column->initialize_column(pcd);

                    auto validity_infos = collect_generic_validity_infos(root_column_index, field_path, windows);
                    apply_validity_infos(list_column->validity, validity_infos, true);

                    std::vector<pax_generic_window_t> child_windows;
                    child_windows.reserve(windows.size());
                    for (const auto& window : windows) {
                        if (window.tuple_count == 0) {
                            continue;
                        }
                        const auto row_start = static_cast<int64_t>(window.row_start);
                        const auto previous_offset =
                            row_start == list_column->start() ? 0 : list_column->list_offset(row_start - 1);
                        const auto current_offset =
                            list_column->list_offset(row_start + static_cast<int64_t>(window.tuple_count) - 1);
                        const auto child_tuple_count = static_cast<uint32_t>(current_offset - previous_offset);
                        if (child_tuple_count == 0) {
                            continue;
                        }
                        child_windows.push_back({static_cast<uint64_t>(list_column->start()) + previous_offset,
                                                 child_tuple_count});
                    }

                    if (!child_windows.empty()) {
                        auto child_path = field_path;
                        child_path.push_back(0);
                        load_generic_column(*list_column->child_column,
                                            root_column_index,
                                            child_path,
                                            false,
                                            child_windows);
                    } else {
                        list_column->child_column->count_ = 0;
                    }
                    return;
                }

                auto* array_column = dynamic_cast<array_column_data_t*>(&column);
                if (!array_column) {
                    throw std::logic_error("pax_generic array column is not an array column");
                }

                auto validity_infos = collect_generic_validity_infos(root_column_index, field_path, windows);
                apply_validity_infos(array_column->validity, validity_infos, true);
                uint64_t array_count = 0;
                for (const auto& window : windows) {
                    array_count += window.tuple_count;
                }
                array_column->count_ = array_count;

                std::vector<pax_generic_window_t> child_windows;
                child_windows.reserve(windows.size());
                const auto array_size = static_cast<uint64_t>(array_column->array_size());
                for (const auto& window : windows) {
                    if (window.tuple_count == 0) {
                        continue;
                    }
                    child_windows.push_back(
                        {static_cast<uint64_t>(array_column->start()) +
                             (window.row_start - static_cast<uint64_t>(array_column->start())) * array_size,
                         static_cast<uint32_t>(static_cast<uint64_t>(window.tuple_count) * array_size)});
                }

                if (!child_windows.empty()) {
                    auto child_path = field_path;
                    child_path.push_back(0);
                    load_generic_column(*array_column->child_column,
                                        root_column_index,
                                        child_path,
                                        false,
                                        child_windows);
                } else {
                    array_column->child_column->count_ = 0;
                }
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
                                    true,
                                    root_windows);
                initialized_roots[column_index] = true;
            }
        }
    }

} // namespace components::table
