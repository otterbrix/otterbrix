#include "data_chunk_binary.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string_view>

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

        // Compute the size of the type header for a single column.
        // Format: [logical_type:1][alias_length:2][alias:N][extension_type:1][extension_data:0-5]
        uint32_t compute_type_header_size(const types::complex_logical_type& column_type) {
            uint32_t header_size = 1 + 2; // logical_type + alias_length
            if (column_type.has_alias()) {
                header_size += static_cast<uint32_t>(column_type.alias().size());
            }
            header_size += 1; // extension_type byte
            auto* extension = column_type.extension();
            if (extension) {
                switch (extension->type()) {
                    case types::logical_type_extension::extension_type::ARRAY:
                        header_size += 5; // inner_logical_type(1) + array_size(4)
                        break;
                    case types::logical_type_extension::extension_type::DECIMAL:
                        header_size += 2; // width(1) + scale(1)
                        break;
                    default:
                        break;
                }
            }
            return header_size;
        }

        // Write the type header for a single column. Returns pointer past written data.
        char* write_type_header(char* output, const types::complex_logical_type& column_type) {
            // Logical type
            *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(column_type.type());
            output += 1;

            // Alias
            if (column_type.has_alias()) {
                auto alias_length = static_cast<uint16_t>(column_type.alias().size());
                write_le16(output, alias_length);
                output += 2;
                std::memcpy(output, column_type.alias().data(), alias_length);
                output += alias_length;
            } else {
                write_le16(output, 0);
                output += 2;
            }

            // Extension
            auto* extension = column_type.extension();
            if (!extension) {
                *reinterpret_cast<uint8_t*>(output) = 0; // no extension
                output += 1;
            } else {
                switch (extension->type()) {
                    case types::logical_type_extension::extension_type::ARRAY: {
                        *reinterpret_cast<uint8_t*>(output) = 1;
                        output += 1;
                        auto* array_extension =
                            static_cast<const types::array_logical_type_extension*>(extension);
                        *reinterpret_cast<uint8_t*>(output) =
                            static_cast<uint8_t>(array_extension->internal_type().type());
                        output += 1;
                        write_le32(output, static_cast<uint32_t>(array_extension->size()));
                        output += 4;
                        break;
                    }
                    case types::logical_type_extension::extension_type::DECIMAL: {
                        *reinterpret_cast<uint8_t*>(output) = 2;
                        output += 1;
                        auto* decimal_extension =
                            static_cast<const types::decimal_logical_type_extension*>(extension);
                        *reinterpret_cast<uint8_t*>(output) = decimal_extension->width();
                        output += 1;
                        *reinterpret_cast<uint8_t*>(output) = decimal_extension->scale();
                        output += 1;
                        break;
                    }
                    default:
                        *reinterpret_cast<uint8_t*>(output) = 0; // unknown extension → none
                        output += 1;
                        break;
                }
            }

            return output;
        }

        // Read the type header for a single column. Advances scan pointer.
        types::complex_logical_type read_type_header(const char*& scan, const char* end) {
            if (scan + 4 > end) {
                throw std::runtime_error("data_chunk_binary: type header overflows buffer");
            }

            // Logical type
            auto logical_type_value = static_cast<types::logical_type>(*reinterpret_cast<const uint8_t*>(scan));
            scan += 1;

            // Alias
            uint16_t alias_length = read_le16(scan);
            scan += 2;
            std::string alias;
            if (alias_length > 0) {
                if (scan + alias_length > end) {
                    throw std::runtime_error("data_chunk_binary: alias overflows buffer");
                }
                alias.assign(scan, alias_length);
                scan += alias_length;
            }

            // Extension type
            if (scan >= end) {
                throw std::runtime_error("data_chunk_binary: extension type overflows buffer");
            }
            uint8_t extension_type = *reinterpret_cast<const uint8_t*>(scan);
            scan += 1;

            switch (extension_type) {
                case 1: { // ARRAY
                    if (scan + 5 > end) {
                        throw std::runtime_error("data_chunk_binary: array extension overflows buffer");
                    }
                    auto inner_logical_type =
                        static_cast<types::logical_type>(*reinterpret_cast<const uint8_t*>(scan));
                    scan += 1;
                    uint32_t array_size = read_le32(scan);
                    scan += 4;
                    return types::complex_logical_type::create_array(
                        inner_logical_type, static_cast<size_t>(array_size), std::move(alias));
                }
                case 2: { // DECIMAL
                    if (scan + 2 > end) {
                        throw std::runtime_error("data_chunk_binary: decimal extension overflows buffer");
                    }
                    uint8_t width = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    uint8_t scale = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    return types::complex_logical_type::create_decimal(width, scale, std::move(alias));
                }
                default: // no extension
                    return types::complex_logical_type(logical_type_value, std::move(alias));
            }
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

            column_type_header_sizes[column_index] = compute_type_header_size(column.type());
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
            output = write_type_header(output, column.type());

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

        assert(static_cast<size_t>(output - buffer.data() - base) == total);
    }

    // -----------------------------------------------------------------------
    // deserialize_binary
    // -----------------------------------------------------------------------
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource) {
        if (len < 10) {
            throw std::runtime_error("data_chunk_binary: buffer too small for header");
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
                throw std::runtime_error("data_chunk_binary: null mask overflows buffer");
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
                auto column_type = read_type_header(scan, end);
                column_types.push_back(std::move(column_type));

                // Skip data_size + data
                if (scan + 4 > end) {
                    throw std::runtime_error("data_chunk_binary: column data_size overflows buffer");
                }
                uint32_t data_size = read_le32(scan);
                scan += 4;
                if (scan + data_size > end) {
                    throw std::runtime_error("data_chunk_binary: column data overflows buffer");
                }
                scan += data_size;
            }
        }

        data_chunk_t chunk(resource, column_types, num_rows);
        chunk.set_cardinality(num_rows);

        // Second pass: populate column data.
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            // Skip type header (already parsed in first pass)
            read_type_header(pointer, end);

            uint32_t data_size = read_le32(pointer);
            pointer += 4;

            auto& column = chunk.data[column_index];
            auto physical_type = column_types[column_index].to_physical_type();

            if (is_variable_type(physical_type)) {
                if (data_size < (num_rows + 1) * 4) {
                    throw std::runtime_error("data_chunk_binary: string offsets overflow");
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
                        views[row_index] =
                            std::string_view(reinterpret_cast<const char*>(heap_pointer), string_length);
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
