#include "data_chunk_binary.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string_view>

#include <components/types/type_binary.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_buffer.hpp>

namespace components::vector {
    // -----------------------------------------------------------------------
    // Little-endian helpers
    // -----------------------------------------------------------------------
    namespace {

        inline void write_le16(char* destination, uint16_t value) { std::memcpy(destination, &value, 2); }
        inline void write_le32(char* destination, uint32_t value) { std::memcpy(destination, &value, 4); }

        inline uint16_t read_le16(const char* source) {
            uint16_t value;
            std::memcpy(&value, source, 2);
            return value;
        }
        inline uint32_t read_le32(const char* source) {
            uint32_t value;
            std::memcpy(&value, source, 4);
            return value;
        }

        // Return the byte-size of one element for a fixed-width physical type.
        // Returns 0 for STRING (variable-width) and for composite types (ARRAY, etc.).
        size_t fixed_type_size(types::physical_type physical_type) {
            switch (physical_type) {
                case types::physical_type::BOOL:
                    return sizeof(bool);
                case types::physical_type::INT8:
                case types::physical_type::UINT8:
                    return 1;
                case types::physical_type::INT16:
                case types::physical_type::UINT16:
                    return 2;
                case types::physical_type::INT32:
                case types::physical_type::UINT32:
                case types::physical_type::FLOAT:
                    return 4;
                case types::physical_type::INT64:
                case types::physical_type::UINT64:
                case types::physical_type::DOUBLE:
                    return 8;
                case types::physical_type::INT128:
                case types::physical_type::UINT128:
                    return 16;
                default:
                    return 0;
            }
        }

        bool is_variable_type(types::physical_type physical_type) {
            return physical_type == types::physical_type::STRING;
        }

        // Empty/sentinel chunk returned on deserialize failure. Caller must check
        // the ok flag and discard the chunk on failure.
        data_chunk_t make_empty_error_chunk(std::pmr::memory_resource* resource) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return data_chunk_t(resource, empty_types, 1);
        }

    } // anonymous namespace

    // -----------------------------------------------------------------------
    // serialize_binary
    // -----------------------------------------------------------------------
    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer) {
        const auto num_columns = static_cast<uint16_t>(chunk.column_count());
        const auto num_rows = static_cast<uint32_t>(chunk.size());

        // ----- Build null mask (row-major, 1 bit per cell, bit=1 means valid) -----
        const uint64_t total_cells = static_cast<uint64_t>(num_columns) * num_rows;
        const uint32_t null_mask_bytes = (total_cells > 0) ? static_cast<uint32_t>((total_cells + 7) / 8) : 0;

        bool has_nulls = false;
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            if (!column.validity().all_valid()) {
                has_nulls = true;
                break;
            }
        }

        const uint32_t actual_mask_bytes = has_nulls ? null_mask_bytes : 0;

        // ----- Pre-compute total size to minimise reallocations -----
        // header: 2 (num_columns) + 4 (num_rows) + 4 (null_mask_size) + actual_mask_bytes
        size_t total = 2 + 4 + 4 + actual_mask_bytes;

        // Per-column: type_header + 4 (data_size) + data_size
        std::vector<uint32_t> column_data_sizes(num_columns);
        std::vector<uint32_t> column_type_header_sizes(num_columns);

        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            auto physical_type = column.type().to_physical_type();

            if (is_variable_type(physical_type)) {
                uint32_t offsets_size = (num_rows + 1) * 4;
                uint32_t string_data_size = 0;
                const auto* views = reinterpret_cast<const std::string_view*>(column.data());
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    string_data_size += static_cast<uint32_t>(views[row_index].size());
                }
                column_data_sizes[column_index] = offsets_size + string_data_size;
            } else {
                size_t element_size = fixed_type_size(physical_type);
                column_data_sizes[column_index] = static_cast<uint32_t>(element_size * num_rows);
            }

            column_type_header_sizes[column_index] = types::type_binary_size(column.type());
            total += column_type_header_sizes[column_index] + 4 + column_data_sizes[column_index];
        }

        const size_t base = buffer.size();
        buffer.resize(base + total);
        char* output = buffer.data() + base;

        // ----- Write header -----
        write_le16(output, num_columns);
        output += 2;
        write_le32(output, num_rows);
        output += 4;
        write_le32(output, actual_mask_bytes);
        output += 4;

        // ----- Write null mask -----
        if (has_nulls) {
            std::memset(output, 0, actual_mask_bytes);
            for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
                const auto& column = chunk.data[column_index];
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint64_t bit_index = static_cast<uint64_t>(row_index) * num_columns + column_index;
                    if (column.validity().all_valid() || column.validity().row_is_valid(row_index)) {
                        output[bit_index / 8] |= static_cast<char>(1u << (bit_index % 8));
                    }
                }
            }
            output += actual_mask_bytes;
        }

        // ----- Write columns -----
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            auto physical_type = column.type().to_physical_type();

            // Write type header (logical_type + alias + extension)
            [[maybe_unused]] const char* type_header_begin = output;
            output = types::type_binary_write(output, column.type());
            assert(static_cast<uint32_t>(output - type_header_begin) == column_type_header_sizes[column_index] &&
                   "type header writer disagrees with compute_type_header_size");

            // Write data_size
            write_le32(output, column_data_sizes[column_index]);
            output += 4;

            // Write data
            if (is_variable_type(physical_type)) {
                const auto* views = reinterpret_cast<const std::string_view*>(column.data());
                uint32_t running_offset = 0;
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    write_le32(output, running_offset);
                    output += 4;
                    running_offset += static_cast<uint32_t>(views[row_index].size());
                }
                write_le32(output, running_offset);
                output += 4;
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    std::memcpy(output, views[row_index].data(), views[row_index].size());
                    output += views[row_index].size();
                }
            } else {
                std::memcpy(output, column.data(), column_data_sizes[column_index]);
                output += column_data_sizes[column_index];
            }
        }

        assert(static_cast<size_t>(output - buffer.data()) - base == total);
    }

    // -----------------------------------------------------------------------
    // deserialize_binary
    // -----------------------------------------------------------------------
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource, bool& ok) {
        ok = true;
        if (len < 10) {
            ok = false;
            return make_empty_error_chunk(resource);
        }

        const char* pointer = data;
        const char* end = data + len;

        uint16_t num_columns = read_le16(pointer);
        pointer += 2;
        uint32_t num_rows = read_le32(pointer);
        pointer += 4;
        uint32_t null_mask_size = read_le32(pointer);
        pointer += 4;

        const char* null_mask = nullptr;
        if (null_mask_size > 0) {
            if (pointer + null_mask_size > end) {
                ok = false;
                return make_empty_error_chunk(resource);
            }
            null_mask = pointer;
            pointer += null_mask_size;
        }

        // First pass: read column types (peek ahead).
        std::pmr::vector<types::complex_logical_type> column_types(resource);
        column_types.reserve(num_columns);

        {
            const char* scan = pointer;
            for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
                // Read type header
                auto column_type = types::type_binary_read(scan, end, resource);
                if (column_type.has_error()) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                column_types.push_back(std::move(column_type.value()));

                // Skip data_size + data
                if (scan + 4 > end) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                uint32_t data_size = read_le32(scan);
                scan += 4;
                if (scan + data_size > end) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                scan += data_size;
            }
        }

        data_chunk_t chunk(resource, column_types, num_rows);
        chunk.set_cardinality(num_rows);

        // Second pass: populate column data.
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            // Skip type header (already parsed in first pass)
            auto skipped = types::type_binary_read(pointer, end, resource);
            if (skipped.has_error()) {
                ok = false;
                return make_empty_error_chunk(resource);
            }

            uint32_t data_size = read_le32(pointer);
            pointer += 4;

            auto& column = chunk.data[column_index];
            auto physical_type = column_types[column_index].to_physical_type();

            if (is_variable_type(physical_type)) {
                if (data_size < (num_rows + 1) * 4) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                const char* offsets_pointer = pointer;
                const char* string_data = pointer + (num_rows + 1) * 4;

                auto* views = reinterpret_cast<std::string_view*>(column.data());
                auto string_buffer = std::make_shared<string_vector_buffer_t>(resource);

                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint32_t offset_begin = read_le32(offsets_pointer + row_index * 4);
                    uint32_t offset_end = read_le32(offsets_pointer + (row_index + 1) * 4);
                    uint32_t string_length = offset_end - offset_begin;

                    if (string_length > 0) {
                        void* heap_pointer = string_buffer->insert(
                            const_cast<void*>(static_cast<const void*>(string_data + offset_begin)),
                            string_length);
                        views[row_index] = std::string_view(reinterpret_cast<const char*>(heap_pointer), string_length);
                    } else {
                        views[row_index] = std::string_view();
                    }
                }

                column.set_auxiliary(std::move(string_buffer));
            } else {
                std::memcpy(column.data(), pointer, data_size);
            }
            pointer += data_size;

            // Apply null mask for this column.
            if (null_mask) {
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint64_t bit_index = static_cast<uint64_t>(row_index) * num_columns + column_index;
                    bool valid = (static_cast<unsigned char>(null_mask[bit_index / 8]) >> (bit_index % 8)) & 1u;
                    if (!valid) {
                        column.validity().set_invalid(row_index);
                    }
                }
            }
        }

        return chunk;
    }
} // namespace components::vector
