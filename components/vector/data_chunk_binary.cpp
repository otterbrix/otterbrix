#include "data_chunk_binary.hpp"

#include <cstring>
#include <limits>
#include <string_view>

#include <components/types/type_spec_codec.hpp>
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

        // Appending helpers. The payload of a nested column is not a size anyone can compute
        // ahead of the walk that produces it (a list's child span is data, not schema), so the
        // writer grows the buffer as it goes and back-patches the one length prefix that needs
        // it. The buffer is the WAL's reused encode buffer, so after warm-up the growth costs
        // nothing.
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

        // Elements per row of an ARRAY. 0 means the extension is missing, i.e. a type that
        // claims to be an ARRAY and cannot say how wide it is — refused rather than guessed.
        uint64_t array_stride(const types::complex_logical_type& type) {
            const auto* extension = type.extension_as<types::array_logical_type_extension>();
            return extension ? extension->size() : 0;
        }

        // Column type header = [spec_size:u32][spec bytes], where the spec is the CANONICAL
        // type-spec encoding (types::encode_type_spec) — the same codec the table checkpoint
        // uses. The previous hand-rolled header enumerated extensions one by one and every
        // missing leg was a STARTUP CRASH: LIST was missing once (T5), then STRUCT — the
        // writer emitted "no extension", replay rebuilt a bare STRUCT, and building the
        // replay chunk walked the absent struct extension's field types through a garbage
        // pointer (flaky SIGSEGV in read_all_records). The canonical spec round-trips every
        // persistable type — struct fields, decimal width/scale, nested children, aliases —
        // recursively, so a new type can never again decode into a crash-shaped half-type.
        // A spec_size of 0 marks a type the canonical codec REFUSED to encode; the reader
        // treats it as corruption (ok=false), never as "assume some type" (rule 6).
        //
        // Encode `column_type`'s spec into `spec` (cleared first). Empty result = refusal.
        void encode_type_spec_or_poison(const types::complex_logical_type& column_type,
                                        std::pmr::vector<std::byte>& spec) {
            spec.clear();
            auto encoded = types::encode_type_spec(column_type, spec);
            if (encoded.has_error()) {
                spec.clear(); // poison marker: spec_size 0 → loud decode failure
            }
        }

        // Read one [spec_size:u32][spec bytes] type header and decode it with the
        // canonical spec codec. Advances scan past the header. A truncated buffer, a
        // zero spec_size (the writer's refusal poison) or a spec the codec rejects sets
        // ok=false and returns an INVALID-typed placeholder (caller must check ok).
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

        // Empty/sentinel chunk returned on deserialize failure. Caller must check
        // the ok flag and discard the chunk on failure.
        data_chunk_t make_empty_error_chunk(std::pmr::memory_resource* resource) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return data_chunk_t(resource, empty_types, 1);
        }

        // -----------------------------------------------------------------
        // NESTED COLUMN PAYLOAD — the recursive half of the codec.
        //
        // Until this existed the per-column data block was sized by fixed_type_size(), which
        // answers 0 for LIST, STRUCT and ARRAY. Writer and reader AGREED on that zero: the
        // writer emitted `data_size = 0` and no bytes, the reader memcpy'd 0 bytes into a
        // correctly-SHAPED nested column, and every element was the zero the constructor had
        // left behind. Nothing failed and nothing was logged. Rows that a checkpoint had made
        // durable were unaffected — they come back through the .otbx column tree, which has
        // always been recursive — so the loss showed up only on rows recovered FROM THE
        // JOURNAL, which is the one path a clean shutdown never exercises.
        //
        // Both directions derive the shape from the column TYPE, which the header ahead of the
        // payload already carries verbatim through the canonical spec codec. So there is no
        // tag byte to keep in sync, and a container inside a container is nothing but this
        // function re-entered — second-level nesting (list of structs, struct holding a list,
        // array of arrays) needs no case of its own.
        //
        // Child order mirrors the .otbx checkpoint's, [validity, ...children], so the two
        // durable paths describe a nested column the same way round:
        //
        //   STRUCT : per field   [validity][payload]        (also TIME_TZ, INTERVAL, UNION)
        //   ARRAY  : [validity][payload] over count*stride child elements
        //   LIST   : [count x (offset:u64)(length:u64)][child_count:u64][validity][payload]
        //            (also MAP, which is physically a list of key/value structs)
        //   STRING : [(count+1) x offset:u32][concatenated bytes]
        //   fixed  : count * element_size raw bytes
        //   NA     : nothing — a NULL-typed column has no payload by construction
        //
        // The TOP-LEVEL column's own validity is NOT written here: it stays in the chunk's
        // interleaved null mask, where it already was and where the existing cases pin it.
        // Only the levels BELOW the column need a mask of their own, and they get one at every
        // level, because a nested column carries validity at each of them.
        // -----------------------------------------------------------------

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

        // The list child's length is the WRITTEN SPAN, not the row count: lengths are ragged
        // and the child buffer is append-only, so it is normally longer than the column is
        // tall. Taking the max of the buffer's own size and every entry's end keeps a vector
        // whose bookkeeping disagrees with its entries from truncating real elements.
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

        // Returns false when the column carries a payload this codec has no rule for. The
        // caller turns that into a POISONED column the reader refuses outright (rule 6) —
        // writing a short payload instead would recreate the very defect this exists to close.
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
                // No payload rule for this physical type (BIT / UNKNOWN / INVALID). Nothing to
                // write when there are no rows; otherwise refuse loudly.
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
                    // The child is about to be ALLOCATED to this length, so a count the record
                    // cannot possibly back with bytes is refused before it is believed. Every
                    // element of every persistable child type costs at least one byte.
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

        // ----- Write header -----
        append_le16(buffer, num_columns);
        append_le32(buffer, num_rows);
        append_le32(buffer, actual_mask_bytes);

        // ----- Write null mask -----
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

        // ----- Write columns: [spec_size:u32][spec][data_size:u32][payload] -----
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
                // POISON the whole column — spec_size 0 — which the reader refuses outright.
                // Rule 6: a column this codec cannot carry must break the record LOUDLY. The
                // alternative, emitting a short payload, is exactly the defect being closed
                // here: a decode that reports success and hands replay a column of zeroes.
                buffer.resize(column_start);
                append_le32(buffer, 0);
                append_le32(buffer, 0);
                continue;
            }
            write_le32(buffer.data() + length_position, static_cast<uint32_t>(payload_size));
        }
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

        // The buffer INTERLEAVES the columns — [type header][data_size][data] each — and
        // data_chunk_t wants every column type up front (its ctor takes the whole type
        // vector; columns cannot be appended afterwards). So the types have to be collected
        // before the chunk exists, which means walking the buffer once before filling it.
        //
        // That first walk already sees where each column's data begins and how long it is,
        // so it RECORDS both. The fill loop below then addresses each column directly and
        // never looks at a type header again — decoding one only to throw it away would cost
        // a pmr-allocated complex_logical_type (children, alias, extension) per column per
        // chunk, on the WAL replay path.
        //
        // Every offset handed to the fill loop is bounds-checked HERE, against `end`, so the
        // second loop indexes an already-validated range rather than re-validating it.
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

        // Fill pass: address each column's data by the offset the walk above recorded. The
        // payload reader is bounded by that column's OWN end, and is required to land exactly
        // on it — a payload that stops short or overruns is a format violation, not a column
        // to be filled in as far as it goes.
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const char* column_data = data + column_data_offsets[column_index];
            const char* column_end = column_data + column_data_lengths[column_index];

            auto& column = chunk.data[column_index];

            const char* scan = column_data;
            if (!read_vector_payload(column, num_rows, scan, column_end, resource) || scan != column_end) {
                ok = false;
                return make_empty_error_chunk(resource);
            }

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
