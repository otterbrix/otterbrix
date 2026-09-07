#include "data_chunk_binary.hpp"

#include <cstring>
#include <limits>
#include <string_view>

#include <components/types/type_spec_codec.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_buffer.hpp>

namespace components::vector {

    namespace {

        inline void write_le16(char* destination, uint16_t value) { std::memcpy(destination, &value, 2); }
        inline void write_le32(char* destination, uint32_t value) { std::memcpy(destination, &value, 4); }
        inline void write_le64(char* destination, uint64_t value) { std::memcpy(destination, &value, 8); }

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
        inline uint64_t read_le64(const char* source) {
            uint64_t value;
            std::memcpy(&value, source, 8);
            return value;
        }

        // A nested payload's size isn't known ahead of the walk producing it, so the writer grows and back-patches.
        inline void append_le16(services::wal::buffer_t& buffer, uint16_t value) {
            const size_t at = buffer.size();
            buffer.resize(at + 2);
            write_le16(buffer.data() + at, value);
        }
        inline void append_le32(services::wal::buffer_t& buffer, uint32_t value) {
            const size_t at = buffer.size();
            buffer.resize(at + 4);
            write_le32(buffer.data() + at, value);
        }
        inline void append_le64(services::wal::buffer_t& buffer, uint64_t value) {
            const size_t at = buffer.size();
            buffer.resize(at + 8);
            write_le64(buffer.data() + at, value);
        }
        inline void append_bytes(services::wal::buffer_t& buffer, const void* source, size_t length) {
            if (length == 0) {
                return;
            }
            const size_t at = buffer.size();
            buffer.resize(at + length);
            std::memcpy(buffer.data() + at, source, length);
        }

        // 0 marks a variable-width or composite type (STRING, ARRAY, etc.).
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

        // 0 means the extension is missing: a type claiming ARRAY but unable to say how wide it is.
        uint64_t array_stride(const types::complex_logical_type& type) {
            const auto* extension = type.extension_as<types::array_logical_type_extension>();
            return extension ? extension->size() : 0;
        }

        // Column type header = [spec_size:u32][spec bytes]; spec_size 0 marks a refused encode.
        void encode_type_spec_or_poison(const types::complex_logical_type& column_type,
                                        std::pmr::vector<std::byte>& spec) {
            spec.clear();
            auto encoded = types::encode_type_spec(column_type, spec);
            if (encoded.has_error()) {
                spec.clear(); // poison marker: spec_size 0 → loud decode failure
            }
        }

        types::complex_logical_type
        read_type_header(const char*& scan, const char* end, std::pmr::memory_resource* resource, bool& ok) {
            if (scan + 4 > end) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }
            uint32_t spec_size = read_le32(scan);
            scan += 4;
            if (spec_size == 0 || scan + spec_size > end) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }
            auto decoded =
                types::decode_type_spec(resource, reinterpret_cast<const std::byte*>(scan), spec_size);
            scan += spec_size;
            if (decoded.has_error()) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }
            return std::move(decoded.value());
        }

        data_chunk_t make_empty_error_chunk(std::pmr::memory_resource* resource) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return data_chunk_t(resource, empty_types, 1);
        }

        // Nested payload order is [validity, ...children]; only levels below the top get a mask of their own here.

        void append_validity_block(const vector_t& vector, uint64_t count, services::wal::buffer_t& buffer) {
            if (count == 0 || vector.validity().all_valid()) {
                append_le32(buffer, 0); // 0 bytes of mask = every element valid
                return;
            }
            const auto mask_bytes = static_cast<uint32_t>((count + 7) / 8);
            append_le32(buffer, mask_bytes);
            const size_t at = buffer.size();
            buffer.resize(at + mask_bytes);
            char* output = buffer.data() + at;
            std::memset(output, 0, mask_bytes);
            for (uint64_t index = 0; index < count; ++index) {
                if (vector.validity().row_is_valid(index)) {
                    output[index / 8] |= static_cast<char>(1u << (index % 8));
                }
            }
        }

        bool read_validity_block(vector_t& vector, uint64_t count, const char*& scan, const char* end) {
            if (static_cast<uint64_t>(end - scan) < 4) {
                return false;
            }
            const uint32_t mask_bytes = read_le32(scan);
            scan += 4;
            if (mask_bytes == 0) {
                return true;
            }
            if (mask_bytes != (count + 7) / 8 || static_cast<uint64_t>(end - scan) < mask_bytes) {
                return false;
            }
            for (uint64_t index = 0; index < count; ++index) {
                const bool valid = (static_cast<unsigned char>(scan[index / 8]) >> (index % 8)) & 1u;
                if (!valid) {
                    vector.validity().set_invalid(index);
                }
            }
            scan += mask_bytes;
            return true;
        }

        uint64_t list_child_count(const vector_t& vector, uint64_t count) {
            uint64_t child_count = vector.size();
            const auto* entries = reinterpret_cast<const types::list_entry_t*>(vector.data());
            for (uint64_t row = 0; row < count; ++row) {
                const uint64_t entry_end = entries[row].offset + entries[row].length;
                if (entry_end > child_count) {
                    child_count = entry_end;
                }
            }
            return child_count;
        }

        // False means no rule for this payload; the caller poisons the column rather than writing a short one.
        bool append_vector_payload(const vector_t& vector, uint64_t count, services::wal::buffer_t& buffer) {
            const auto physical_type = vector.type().to_physical_type();

            if (is_variable_type(physical_type)) {
                const auto* views = reinterpret_cast<const std::string_view*>(vector.data());
                uint32_t running_offset = 0;
                for (uint64_t index = 0; index < count; ++index) {
                    append_le32(buffer, running_offset);
                    running_offset += static_cast<uint32_t>(views[index].size());
                }
                append_le32(buffer, running_offset);
                for (uint64_t index = 0; index < count; ++index) {
                    append_bytes(buffer, views[index].data(), views[index].size());
                }
                return true;
            }

            switch (physical_type) {
                case types::physical_type::NA:
                    return true;
                case types::physical_type::STRUCT: {
                    const auto& fields = vector.entries();
                    for (const auto& field : fields) {
                        append_validity_block(*field, count, buffer);
                        if (!append_vector_payload(*field, count, buffer)) {
                            return false;
                        }
                    }
                    return true;
                }
                case types::physical_type::ARRAY: {
                    const uint64_t stride = array_stride(vector.type());
                    if (stride == 0) {
                        return false;
                    }
                    const uint64_t child_count = count * stride;
                    const auto& child = vector.entry();
                    append_validity_block(child, child_count, buffer);
                    return append_vector_payload(child, child_count, buffer);
                }
                case types::physical_type::LIST: {
                    const auto* entries = reinterpret_cast<const types::list_entry_t*>(vector.data());
                    for (uint64_t row = 0; row < count; ++row) {
                        append_le64(buffer, entries[row].offset);
                        append_le64(buffer, entries[row].length);
                    }
                    const uint64_t child_count = list_child_count(vector, count);
                    append_le64(buffer, child_count);
                    const auto& child = vector.entry();
                    append_validity_block(child, child_count, buffer);
                    return append_vector_payload(child, child_count, buffer);
                }
                default:
                    break;
            }

            const size_t element_size = fixed_type_size(physical_type);
            if (element_size == 0) {
                // BIT / UNKNOWN / INVALID have no payload rule; only 0 rows can be written.
                return count == 0;
            }
            append_bytes(buffer, vector.data(), element_size * count);
            return true;
        }

        bool read_vector_payload(vector_t& vector,
                                 uint64_t count,
                                 const char*& scan,
                                 const char* end,
                                 std::pmr::memory_resource* resource) {
            const auto physical_type = vector.type().to_physical_type();

            if (is_variable_type(physical_type)) {
                const uint64_t offsets_bytes = (count + 1) * 4u;
                if (static_cast<uint64_t>(end - scan) < offsets_bytes) {
                    return false;
                }
                const char* offsets = scan;
                const uint32_t total_bytes = read_le32(offsets + count * 4u);
                const char* string_data = offsets + offsets_bytes;
                if (static_cast<uint64_t>(end - string_data) < total_bytes) {
                    return false;
                }

                auto* views = reinterpret_cast<std::string_view*>(vector.data());
                auto string_buffer = std::make_shared<string_vector_buffer_t>(resource);
                for (uint64_t index = 0; index < count; ++index) {
                    const uint32_t offset_begin = read_le32(offsets + index * 4);
                    const uint32_t offset_end = read_le32(offsets + (index + 1) * 4);
                    if (offset_end < offset_begin || offset_end > total_bytes) {
                        return false;
                    }
                    const uint32_t string_length = offset_end - offset_begin;
                    if (string_length > 0) {
                        void* heap_pointer = string_buffer->insert(
                            const_cast<void*>(static_cast<const void*>(string_data + offset_begin)),
                            string_length);
                        views[index] = std::string_view(reinterpret_cast<const char*>(heap_pointer), string_length);
                    } else {
                        views[index] = std::string_view();
                    }
                }
                vector.set_auxiliary(std::move(string_buffer));
                scan = string_data + total_bytes;
                return true;
            }

            switch (physical_type) {
                case types::physical_type::NA:
                    return true;
                case types::physical_type::STRUCT: {
                    auto& fields = vector.entries();
                    for (auto& field : fields) {
                        if (!read_validity_block(*field, count, scan, end) ||
                            !read_vector_payload(*field, count, scan, end, resource)) {
                            return false;
                        }
                    }
                    return true;
                }
                case types::physical_type::ARRAY: {
                    const uint64_t stride = array_stride(vector.type());
                    if (stride == 0) {
                        return false;
                    }
                    const uint64_t child_count = count * stride;
                    auto& child = vector.entry();
                    return read_validity_block(child, child_count, scan, end) &&
                           read_vector_payload(child, child_count, scan, end, resource);
                }
                case types::physical_type::LIST: {
                    const uint64_t entries_bytes = count * 16u;
                    if (static_cast<uint64_t>(end - scan) < entries_bytes + 8) {
                        return false;
                    }
                    auto* entries = reinterpret_cast<types::list_entry_t*>(vector.data());
                    for (uint64_t row = 0; row < count; ++row) {
                        entries[row].offset = read_le64(scan);
                        scan += 8;
                        entries[row].length = read_le64(scan);
                        scan += 8;
                    }
                    const uint64_t child_count = read_le64(scan);
                    scan += 8;
                    // A count the remaining bytes can't possibly back is refused before it drives an allocation.
                    if (child_count > static_cast<uint64_t>(end - scan)) {
                        return false;
                    }
                    vector.reserve(child_count);
                    vector.set_list_size(child_count);
                    auto& child = vector.entry();
                    return read_validity_block(child, child_count, scan, end) &&
                           read_vector_payload(child, child_count, scan, end, resource);
                }
                default:
                    break;
            }

            const size_t element_size = fixed_type_size(physical_type);
            if (element_size == 0) {
                return count == 0;
            }
            const uint64_t bytes = element_size * count;
            if (static_cast<uint64_t>(end - scan) < bytes) {
                return false;
            }
            if (bytes > 0) {
                std::memcpy(vector.data(), scan, bytes);
                scan += bytes;
            }
            return true;
        }

    } // anonymous namespace

    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer) {
        const auto num_columns = static_cast<uint16_t>(chunk.column_count());
        const auto num_rows = static_cast<uint32_t>(chunk.size());

        // Null mask is row-major, 1 bit per cell, bit=1 means valid.
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

        append_le16(buffer, num_columns);
        append_le32(buffer, num_rows);
        append_le32(buffer, actual_mask_bytes);

        if (has_nulls) {
            const size_t at = buffer.size();
            buffer.resize(at + actual_mask_bytes);
            char* output = buffer.data() + at;
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
        }

        // Per column: [spec_size:u32][spec][data_size:u32][payload].
        std::pmr::vector<std::byte> spec(chunk.resource());
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            const size_t column_start = buffer.size();

            encode_type_spec_or_poison(column.type(), spec);
            append_le32(buffer, static_cast<uint32_t>(spec.size()));
            append_bytes(buffer, spec.data(), spec.size());

            const size_t length_position = buffer.size();
            append_le32(buffer, 0); // data_size, back-patched once the payload is written
            const size_t data_start = buffer.size();

            const bool payload_written = append_vector_payload(column, num_rows, buffer);
            const size_t payload_size = buffer.size() - data_start;

            if (!payload_written || spec.empty() || payload_size > std::numeric_limits<uint32_t>::max()) {
                // Poison the whole column (spec_size 0) so the reader refuses it outright.
                buffer.resize(column_start);
                append_le32(buffer, 0);
                append_le32(buffer, 0);
                continue;
            }
            write_le32(buffer.data() + length_position, static_cast<uint32_t>(payload_size));
        }
    }

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
            // Indexed by row * num_columns + column bits, so a mask shorter than the chunk is refused here.
            const uint64_t required_bits = static_cast<uint64_t>(num_rows) * num_columns;
            const uint64_t required_bytes = (required_bits + 7) / 8;
            if (static_cast<uint64_t>(null_mask_size) < required_bytes) {
                ok = false;
                return make_empty_error_chunk(resource);
            }
            null_mask = pointer;
            pointer += null_mask_size;
        }

        // Interleaved as [type header][data_size][data] per column; a first walk collects types and
        // offsets since data_chunk_t's ctor needs the whole column-type vector up front.
        std::pmr::vector<types::complex_logical_type> column_types(resource);
        column_types.reserve(num_columns);
        std::pmr::vector<uint64_t> column_data_offsets(resource); // from `data`, to the column's DATA
        std::pmr::vector<uint32_t> column_data_lengths(resource);
        column_data_offsets.reserve(num_columns);
        column_data_lengths.reserve(num_columns);

        {
            const char* scan = pointer;
            for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
                auto column_type = read_type_header(scan, end, resource, ok);
                if (!ok) {
                    return make_empty_error_chunk(resource);
                }
                column_types.push_back(std::move(column_type));

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
                column_data_offsets.push_back(static_cast<uint64_t>(scan - data));
                column_data_lengths.push_back(data_size);
                scan += data_size;
            }
        }

        data_chunk_t chunk(resource, column_types, num_rows);
        chunk.set_cardinality(num_rows);

        // The payload reader must land exactly on that column's own end; short or overrun is a format violation.
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const char* column_data = data + column_data_offsets[column_index];
            const char* column_end = column_data + column_data_lengths[column_index];

            auto& column = chunk.data[column_index];

            const char* scan = column_data;
            if (!read_vector_payload(column, num_rows, scan, column_end, resource) || scan != column_end) {
                ok = false;
                return make_empty_error_chunk(resource);
            }

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
