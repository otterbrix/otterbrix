#include "wal_binary.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <tuple>

#include <absl/crc/crc32c.h>

#include <components/vector/data_chunk.hpp>
#include <components/vector/data_chunk_binary.hpp>

namespace services::wal {

    // Little-endian helpers: memcpy is a no-op on x86/ARM-LE, but correct (and optimal) everywhere.
    namespace {

        inline void write_le32(char* dst, uint32_t v) { std::memcpy(dst, &v, 4); }
        inline void write_le64(char* dst, uint64_t v) { std::memcpy(dst, &v, 8); }

        inline uint32_t read_le32(const char* src) {
            uint32_t v;
            std::memcpy(&v, src, 4);
            return v;
        }
        inline uint64_t read_le64(const char* src) {
            uint64_t v;
            std::memcpy(&v, src, 8);
            return v;
        }

        crc32_t compute_crc(const char* data, size_t len) {
            auto crc = absl::ComputeCrc32c(absl::string_view(data, len));
            return static_cast<crc32_t>(crc);
        }

        // Layout: [chunk_count:4][chunk_size:4][chunk]*; the whole batch is one record, so recovery sees all or none.
        core::error_t serialize_chunk_batch(buffer_t& payload_buf,
                                            const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
            const size_t count_pos = payload_buf.size();
            payload_buf.resize(count_pos + 4);
            write_le32(payload_buf.data() + count_pos, static_cast<uint32_t>(chunks.size()));
            for (const auto& chunk : chunks) {
                const size_t len_pos = payload_buf.size();
                payload_buf.resize(len_pos + 4); // size placeholder
                const size_t data_start = payload_buf.size();
                RETURN_IF_ERROR(components::vector::serialize_binary(chunk, payload_buf));
                const auto chunk_size = static_cast<uint32_t>(payload_buf.size() - data_start);
                write_le32(payload_buf.data() + len_pos, chunk_size);
            }
            return core::error_t::no_error();
        }

        // Layout: [attoid_count:4][attoid:4]*.
        void serialize_attoids(buffer_t& payload_buf, const column_attoids_t& attoids) {
            size_t start = payload_buf.size();
            payload_buf.resize(start + 4 + attoids.size() * 4);
            char* ptr = payload_buf.data() + start;
            write_le32(ptr, static_cast<uint32_t>(attoids.size()));
            ptr += 4;
            for (const auto attoid : attoids) {
                write_le32(ptr, static_cast<uint32_t>(attoid));
                ptr += 4;
            }
        }

        // Returns the bytes consumed
        uint32_t deserialize_attoids(const char* payload, uint32_t payload_size, column_attoids_t* attoids) {
            if (payload_size < 4) {
                return 0;
            }
            uint32_t count = read_le32(payload);
            if (count > (payload_size - 4) / 4) {
                return 0;
            }
            attoids->resize(count);
            for (uint32_t index = 0; index < count; ++index) {
                (*attoids)[index] = components::catalog::oid_t{read_le32(payload + 4 + index * 4)};
            }
            return 4 + count * 4;
        }

        // Inverse of serialize_chunk_batch, producing the vector form replay consumes.
        std::pmr::vector<components::vector::data_chunk_t> deserialize_chunk_batch(const char* payload,
                                                                                   uint32_t payload_size,
                                                                                   std::pmr::memory_resource* resource,
                                                                                   bool& ok) {
            ok = true;
            std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
            if (payload_size < 4) {
                ok = false;
                return chunks;
            }
            const char* ptr = payload;
            uint32_t chunk_count = read_le32(ptr);
            ptr += 4;
            uint32_t remaining = payload_size - 4;

            chunks.reserve(chunk_count);
            for (uint32_t ci = 0; ci < chunk_count; ++ci) {
                if (remaining < 4) {
                    ok = false;
                    chunks.clear();
                    return chunks;
                }
                uint32_t chunk_size = read_le32(ptr);
                ptr += 4;
                remaining -= 4;
                if (remaining < chunk_size) {
                    ok = false;
                    chunks.clear();
                    return chunks;
                }
                bool chunk_ok = false;
                auto chunk = components::vector::deserialize_binary(ptr, chunk_size, resource, chunk_ok);
                if (!chunk_ok) {
                    ok = false;
                    chunks.clear();
                    return chunks;
                }
                chunks.emplace_back(std::move(chunk));
                ptr += chunk_size;
                remaining -= chunk_size;
            }
            return chunks;
        }

        // DML header: [size:4][last_crc32:4][wal_id:8][txn_id:8][record_type:1][table_oid:4][sub_id:8]
        // [row_start:8][row_count:8][payload_size:4][payload][crc32:4].

        static constexpr size_t DML_FIXED_HEADER = 4    // last_crc32
                                                   + 8  // wal_id
                                                   + 8  // txn_id
                                                   + 1  // record_type
                                                   + 4  // table_oid
                                                   + 8  // sub_id
                                                   + 8  // row_start
                                                   + 8  // row_count
                                                   + 4; // payload_size

        crc32_t write_dml_record(buffer_t& buffer,
                                 crc32_t last_crc32,
                                 id_t wal_id,
                                 uint64_t txn_id,
                                 uint64_t sub_id,
                                 wal_record_type rtype,
                                 components::catalog::oid_t table_oid,
                                 uint64_t row_start,
                                 uint64_t row_count,
                                 const char* payload,
                                 uint32_t payload_size) {
            // "size" = bytes from last_crc32 up to end-of-payload (before trailing crc32)
            const uint32_t size_field = static_cast<uint32_t>(DML_FIXED_HEADER + payload_size);

            const size_t total = 4 /*size*/ + size_field + 4 /*crc32*/;
            const size_t base = buffer.size();
            buffer.resize(base + total);
            char* out = buffer.data() + base;

            write_le32(out, size_field);
            out += 4;

            char* crc_start = out;

            write_le32(out, last_crc32);
            out += 4;
            write_le64(out, wal_id);
            out += 8;
            write_le64(out, txn_id);
            out += 8;
            *reinterpret_cast<uint8_t*>(out) = static_cast<uint8_t>(rtype);
            out += 1;
            write_le32(out, static_cast<uint32_t>(table_oid));
            out += 4;
            write_le64(out, sub_id);
            out += 8;
            write_le64(out, row_start);
            out += 8;
            write_le64(out, row_count);
            out += 8;
            write_le32(out, payload_size);
            out += 4;

            if (payload_size > 0) {
                std::memcpy(out, payload, payload_size);
                out += payload_size;
            }

            assert(static_cast<size_t>(out - crc_start) == size_field);

            crc32_t crc = compute_crc(crc_start, size_field);
            write_le32(out, crc);

            return crc;
        }

        // The payload one record can carry: its size field counts the fixed header too.
        constexpr std::size_t MAX_RECORD_PAYLOAD = std::numeric_limits<uint32_t>::max() - DML_FIXED_HEADER;
        // [part_index:4][part_count:4] ahead of a part's payload.
        constexpr std::size_t PART_PREFIX_BYTES = 8;

        std::atomic<std::size_t> g_part_payload_override{0};

        void append_part_prefix(buffer_t& payload, std::size_t part, std::size_t part_count) {
            const size_t at = payload.size();
            payload.resize(at + PART_PREFIX_BYTES);
            write_le32(payload.data() + at, static_cast<uint32_t>(part));
            write_le32(payload.data() + at + 4, static_cast<uint32_t>(part_count));
        }

        // serialize_chunk_batch's framing over the part's already serialized chunks.
        void append_part_chunks(buffer_t& payload, const wal_operation_t& operation, const wal_part_t& part) {
            const size_t count_pos = payload.size();
            payload.resize(count_pos + 4);
            write_le32(payload.data() + count_pos, static_cast<uint32_t>(part.chunk_count));
            for (std::size_t index = part.first_chunk; index < part.first_chunk + part.chunk_count; ++index) {
                const auto& chunk = operation.chunks[index];
                const size_t len_pos = payload.size();
                payload.resize(len_pos + 4);
                write_le32(payload.data() + len_pos, static_cast<uint32_t>(chunk.size()));
                payload.append(chunk);
            }
        }

    } // anonymous namespace

    std::size_t wal_part_payload_bytes() noexcept {
        const auto override_bytes = g_part_payload_override.load(std::memory_order_relaxed);
        return override_bytes != 0 ? override_bytes : WAL_PART_PAYLOAD_BYTES;
    }

#ifdef DEV_MODE
    void dev_set_wal_part_payload_bytes(std::size_t bytes) noexcept {
        g_part_payload_override.store(bytes, std::memory_order_relaxed);
    }
#endif

    core::result_wrapper_t<wal_operation_t>
    cut_into_parts(const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                   std::size_t attoid_count,
                   bool carries_row_ids,
                   std::size_t part_bytes,
                   std::pmr::memory_resource* resource) {
        wal_operation_t operation{std::pmr::vector<buffer_t>(resource), std::pmr::vector<wal_part_t>(resource)};
        operation.chunks.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            buffer_t serialized(resource);
            RETURN_IF_ERROR(components::vector::serialize_binary(chunk, serialized));
            operation.chunks.push_back(std::move(serialized));
        }

        // prefix, attoids, chunk count, and an UPDATE's row-id length
        const std::size_t fixed = PART_PREFIX_BYTES + 4 + 4 * attoid_count + 4 + (carries_row_ids ? 4 : 0);
        wal_part_t current{};
        std::size_t current_bytes = fixed;
        uint64_t row = 0;
        for (std::size_t index = 0; index < chunks.size(); ++index) {
            const uint64_t rows = chunks[index].size();
            const std::size_t cost = 4 + operation.chunks[index].size() + (carries_row_ids ? 8 * rows : 0);
            if (current.chunk_count > 0 && current_bytes + cost > part_bytes) {
                operation.parts.push_back(current);
                current = wal_part_t{index, 0, row, 0};
                current_bytes = fixed;
            }
            if (current.chunk_count == 0) {
                current.first_chunk = index;
                current.first_row = row;
            }
            if (current_bytes + cost > MAX_RECORD_PAYLOAD) {
                return core::error_t(core::error_code_t::program_limit_exceeded,
                                     std::pmr::string{"WAL: chunk " + std::to_string(index) + " of the operation (" +
                                                          std::to_string(operation.chunks[index].size()) +
                                                          " bytes) is larger than one record holds; nothing "
                                                          "was journaled",
                                                      resource});
            }
            ++current.chunk_count;
            current.row_count += rows;
            current_bytes += cost;
            row += rows;
        }
        operation.parts.push_back(current);
        return operation;
    }

    crc32_t encode_insert_part(buffer_t& buffer,
                               crc32_t last_crc32,
                               id_t wal_id,
                               uint64_t txn_id,
                               uint64_t sub_id,
                               components::catalog::oid_t table_oid,
                               const column_attoids_t& attoids,
                               const wal_operation_t& operation,
                               std::size_t part,
                               uint64_t row_start,
                               uint64_t row_count) {
        const auto& slice = operation.parts[part];
        const bool whole = operation.parts.size() == 1;
        // Payload: [part prefix, parts only][attoids][chunk_batch].
        buffer_t payload_buf(buffer.get_allocator());
        if (!whole) {
            append_part_prefix(payload_buf, part, operation.parts.size());
        }
        serialize_attoids(payload_buf, attoids);
        append_part_chunks(payload_buf, operation, slice);
        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                sub_id,
                                whole ? wal_record_type::PHYSICAL_INSERT : wal_record_type::PHYSICAL_INSERT_PART,
                                table_oid,
                                whole ? row_start : row_start + slice.first_row,
                                whole ? row_count : slice.row_count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    crc32_t encode_update_part(buffer_t& buffer,
                               crc32_t last_crc32,
                               id_t wal_id,
                               uint64_t txn_id,
                               uint64_t sub_id,
                               components::catalog::oid_t table_oid,
                               const int64_t* row_ids,
                               const column_attoids_t& attoids,
                               const wal_operation_t& operation,
                               std::size_t part,
                               uint64_t count) {
        const auto& slice = operation.parts[part];
        const bool whole = operation.parts.size() == 1;
        const uint64_t id_count = whole ? count : slice.row_count;
        const int64_t* ids = whole ? row_ids : row_ids + slice.first_row;
        // Payload: [part prefix, parts only][row_ids_size:4][row_ids][attoids][chunk_batch].
        buffer_t payload_buf(buffer.get_allocator());
        if (!whole) {
            append_part_prefix(payload_buf, part, operation.parts.size());
        }
        const auto row_ids_bytes = static_cast<uint32_t>(id_count * sizeof(int64_t));
        const size_t at = payload_buf.size();
        payload_buf.resize(at + 4 + row_ids_bytes);
        write_le32(payload_buf.data() + at, row_ids_bytes);
        if (row_ids_bytes > 0) {
            std::memcpy(payload_buf.data() + at + 4, ids, row_ids_bytes);
        }
        serialize_attoids(payload_buf, attoids);
        append_part_chunks(payload_buf, operation, slice);
        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                sub_id,
                                whole ? wal_record_type::PHYSICAL_UPDATE : wal_record_type::PHYSICAL_UPDATE_PART,
                                table_oid,
                                0,
                                id_count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    bool attoids_match_chunks(const column_attoids_t& attoids,
                              const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
        for (const auto& chunk : chunks) {
            if (chunk.column_count() != attoids.size()) {
                return false;
            }
        }
        return true;
    }

    core::result_wrapper_t<crc32_t> encode_insert(buffer_t& buffer,
                                                  std::pmr::memory_resource* /*resource*/,
                                                  crc32_t last_crc32,
                                                  id_t wal_id,
                                                  uint64_t txn_id,
                                                  uint64_t sub_id,
                                                  components::catalog::oid_t table_oid,
                                                  const column_attoids_t& attoids,
                                                  const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                                  uint64_t row_start,
                                                  uint64_t row_count) {
        VALUE_OR_RETURN(auto operation,
                        cut_into_parts(chunks,
                                       attoids.size(),
                                       false,
                                       std::numeric_limits<std::size_t>::max(),
                                       buffer.get_allocator().resource()));
        return encode_insert_part(buffer,
                                  last_crc32,
                                  wal_id,
                                  txn_id,
                                  sub_id,
                                  table_oid,
                                  attoids,
                                  operation,
                                  0,
                                  row_start,
                                  row_count);
    }

    // Schema-growth record: payload is a 0-row data_chunk whose columns are the new ones; row_count = new-column count.
    core::result_wrapper_t<crc32_t> encode_add_column(buffer_t& buffer,
                                                      crc32_t last_crc32,
                                                      id_t wal_id,
                                                      uint64_t txn_id,
                                                      uint64_t sub_id,
                                                      components::catalog::oid_t table_oid,
                                                      const column_attoids_t& attoids,
                                                      const components::vector::data_chunk_t& schema_chunk,
                                                      uint64_t first_position) {
        // Payload: [attoids][chunk_batch].
        buffer_t payload_buf(buffer.get_allocator());
        serialize_attoids(payload_buf, attoids);
        // Framed as a one-element chunk batch, not bare serialize_binary, so the leading-count
        // read in deserialize_chunk_batch doesn't misfire.
        std::pmr::vector<components::vector::data_chunk_t> schema_batch{payload_buf.get_allocator().resource()};
        components::vector::data_chunk_t schema_copy(schema_chunk.resource(),
                                                     schema_chunk.types(),
                                                     std::max<uint64_t>(schema_chunk.size(), 1));
        schema_chunk.copy(schema_copy, 0);
        schema_copy.set_cardinality(schema_chunk.size());
        schema_batch.emplace_back(std::move(schema_copy));
        RETURN_IF_ERROR(serialize_chunk_batch(payload_buf, schema_batch));

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                sub_id,
                                wal_record_type::PHYSICAL_ADD_COLUMN,
                                table_oid,
                                first_position,
                                schema_chunk.column_count(),
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    crc32_t encode_delete(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          uint64_t sub_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          uint64_t count) {
        // Payload = raw int64_t array.
        const auto payload_size = static_cast<uint32_t>(count * sizeof(int64_t));

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                sub_id,
                                wal_record_type::PHYSICAL_DELETE,
                                table_oid,
                                0,
                                count,
                                reinterpret_cast<const char*>(row_ids),
                                payload_size);
    }

    core::result_wrapper_t<crc32_t> encode_update(buffer_t& buffer,
                                                  std::pmr::memory_resource* /*resource*/,
                                                  crc32_t last_crc32,
                                                  id_t wal_id,
                                                  uint64_t txn_id,
                                                  uint64_t sub_id,
                                                  components::catalog::oid_t table_oid,
                                                  const int64_t* row_ids,
                                                  const column_attoids_t& attoids,
                                                  const std::pmr::vector<components::vector::data_chunk_t>& new_chunks,
                                                  uint64_t count) {
        VALUE_OR_RETURN(auto operation,
                        cut_into_parts(new_chunks,
                                       attoids.size(),
                                       true,
                                       std::numeric_limits<std::size_t>::max(),
                                       buffer.get_allocator().resource()));
        return encode_update_part(buffer,
                                  last_crc32,
                                  wal_id,
                                  txn_id,
                                  sub_id,
                                  table_oid,
                                  row_ids,
                                  attoids,
                                  operation,
                                  0,
                                  count);
    }

    // Marker (37 bytes): [size:4][last_crc32:4][wal_id:8][txn_id:8][record_type:1][commit_id/from_sub_id:8][crc32:4].
    crc32_t write_marker_record(buffer_t& buffer,
                                crc32_t last_crc32,
                                id_t wal_id,
                                uint64_t txn_id,
                                wal_record_type rtype,
                                uint64_t value) {
        static constexpr uint32_t MARKER_BODY_SIZE = 4 + 8 + 8 + 1 + 8;
        static constexpr size_t MARKER_TOTAL = 4 + MARKER_BODY_SIZE + 4;

        const size_t base = buffer.size();
        buffer.resize(base + MARKER_TOTAL);
        char* out = buffer.data() + base;

        write_le32(out, MARKER_BODY_SIZE);
        out += 4;

        char* crc_start = out;

        write_le32(out, last_crc32);
        out += 4;
        write_le64(out, wal_id);
        out += 8;
        write_le64(out, txn_id);
        out += 8;
        *reinterpret_cast<uint8_t*>(out) = static_cast<uint8_t>(rtype);
        out += 1;
        write_le64(out, value);
        out += 8;

        assert(static_cast<size_t>(out - crc_start) == MARKER_BODY_SIZE);

        crc32_t crc = compute_crc(crc_start, MARKER_BODY_SIZE);
        write_le32(out, crc);

        return crc;
    }

    crc32_t encode_commit(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t commit_id) {
        return write_marker_record(buffer, last_crc32, wal_id, txn_id, wal_record_type::COMMIT, commit_id);
    }

    crc32_t
    encode_rollback_to(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t from_sub_id) {
        return write_marker_record(buffer, last_crc32, wal_id, txn_id, wal_record_type::ROLLBACK_TO, from_sub_id);
    }

    record_t decode_record(const buffer_t& buffer, std::pmr::memory_resource* resource) {
        return decode_record(buffer.data(), buffer.size(), resource);
    }

    record_t decode_record(const char* data, size_t len, std::pmr::memory_resource* resource) {
        record_t rec{resource};
        rec.is_corrupt = false;

        // Minimum valid record is a COMMIT at 37 bytes.
        if (len < 37) {
            rec.is_corrupt = true;
            rec.size = 0;
            return rec;
        }

        const char* ptr = data;

        uint32_t body_size = read_le32(ptr);
        ptr += 4;

        if (4 + body_size + 4 > len) {
            rec.is_corrupt = true;
            rec.size = 0;
            return rec;
        }

        rec.size = static_cast<size_tt>(4 + body_size + 4);

        const char* body_start = ptr;
        crc32_t expected_crc = read_le32(body_start + body_size);
        crc32_t actual_crc = compute_crc(body_start, body_size);

        if (expected_crc != actual_crc) {
            rec.is_corrupt = true;
            rec.crc32 = expected_crc;
            return rec;
        }
        rec.crc32 = actual_crc;

        rec.last_crc32 = read_le32(ptr);
        ptr += 4;
        rec.id = read_le64(ptr);
        ptr += 8;
        rec.transaction_id = read_le64(ptr);
        ptr += 8;
        rec.record_type = static_cast<wal_record_type>(*reinterpret_cast<const uint8_t*>(ptr));
        ptr += 1;

        // commit_id stays 0 for DML records; replay back-fills it from the matching COMMIT.
        if (rec.record_type == wal_record_type::COMMIT) {
            rec.commit_id = read_le64(ptr);
            ptr += 8;
            return rec;
        }
        if (rec.record_type == wal_record_type::ROLLBACK_TO) {
            rec.sub_id = read_le64(ptr);
            ptr += 8;
            return rec;
        }

        if (static_cast<size_t>(body_size) < DML_FIXED_HEADER) {
            rec.is_corrupt = true;
            return rec;
        }

        rec.table_oid = components::catalog::oid_t{read_le32(ptr)};
        ptr += 4;
        rec.sub_id = read_le64(ptr);
        ptr += 8;
        rec.physical_row_start = read_le64(ptr);
        ptr += 8;
        rec.physical_row_count = read_le64(ptr);
        ptr += 8;
        uint32_t payload_size = read_le32(ptr);
        ptr += 4;

        if (static_cast<size_t>(DML_FIXED_HEADER + payload_size) != body_size) {
            rec.is_corrupt = true;
            return rec;
        }

        const char* payload = ptr;

        if (rec.is_part()) {
            if (payload_size < PART_PREFIX_BYTES) {
                rec.is_corrupt = true;
                return rec;
            }
            rec.part_index = read_le32(payload);
            rec.part_count = read_le32(payload + 4);
            if (rec.part_count < 2 || rec.part_index >= rec.part_count) {
                rec.is_corrupt = true;
                return rec;
            }
            payload += PART_PREFIX_BYTES;
            payload_size -= static_cast<uint32_t>(PART_PREFIX_BYTES);
        }

        switch (whole_record_type(rec.record_type)) {
            case wal_record_type::PHYSICAL_INSERT: {
                uint32_t attoid_bytes = deserialize_attoids(payload, payload_size, &rec.physical_attoids);
                if (attoid_bytes == 0) {
                    rec.is_corrupt = true;
                    return rec;
                }
                if (payload_size > attoid_bytes) {
                    bool ok = false;
                    rec.physical_data =
                        deserialize_chunk_batch(payload + attoid_bytes, payload_size - attoid_bytes, resource, ok);
                    if (!ok || !attoids_match_chunks(rec.physical_attoids, rec.physical_data)) {
                        rec.is_corrupt = true;
                        return rec;
                    }
                }
                break;
            }
            // [attoids][chunk batch]: one chunk whose columns are the new ones.
            case wal_record_type::PHYSICAL_ADD_COLUMN: {
                uint32_t attoid_bytes = deserialize_attoids(payload, payload_size, &rec.physical_attoids);
                if (attoid_bytes == 0) {
                    rec.is_corrupt = true;
                    return rec;
                }
                if (payload_size > attoid_bytes) {
                    bool ok = false;
                    rec.physical_data =
                        deserialize_chunk_batch(payload + attoid_bytes, payload_size - attoid_bytes, resource, ok);
                    if (!ok || !attoids_match_chunks(rec.physical_attoids, rec.physical_data)) {
                        rec.is_corrupt = true;
                        return rec;
                    }
                }
                break;
            }
            case wal_record_type::PHYSICAL_DELETE: {
                // Row-id payloads are WHOLE row ids; sizing as size/8 overran by up to 7 bytes on a ragged length.
                if (payload_size % sizeof(int64_t) != 0) {
                    rec.is_corrupt = true;
                    return rec;
                }
                uint64_t count = payload_size / sizeof(int64_t);
                rec.physical_row_ids.resize(count);
                std::memcpy(rec.physical_row_ids.data(), payload, payload_size);
                break;
            }
            case wal_record_type::PHYSICAL_UPDATE: {
                if (payload_size < 4) {
                    rec.is_corrupt = true;
                    return rec;
                }
                uint32_t row_ids_bytes = read_le32(payload);
                const char* row_ids_data = payload + 4;
                // Wrap-safe: `4 + row_ids_bytes` could overflow 32-bit arithmetic near UINT32_MAX
                // and let a bogus length through; same whole-row-ids rule as PHYSICAL_DELETE.
                if (row_ids_bytes > payload_size - 4 || row_ids_bytes % sizeof(int64_t) != 0) {
                    rec.is_corrupt = true;
                    return rec;
                }
                uint64_t id_count = row_ids_bytes / sizeof(int64_t);
                rec.physical_row_ids.resize(id_count);
                std::memcpy(rec.physical_row_ids.data(), row_ids_data, row_ids_bytes);

                const char* attoid_data = row_ids_data + row_ids_bytes;
                uint32_t after_row_ids = payload_size - 4 - row_ids_bytes;
                uint32_t attoid_bytes = deserialize_attoids(attoid_data, after_row_ids, &rec.physical_attoids);
                if (attoid_bytes == 0) {
                    rec.is_corrupt = true;
                    return rec;
                }
                const char* chunk_data = attoid_data + attoid_bytes;
                uint32_t chunk_batch_size = after_row_ids - attoid_bytes;
                if (chunk_batch_size > 0) {
                    bool ok = false;
                    rec.physical_data = deserialize_chunk_batch(chunk_data, chunk_batch_size, resource, ok);
                    if (!ok || !attoids_match_chunks(rec.physical_attoids, rec.physical_data)) {
                        rec.is_corrupt = true;
                        return rec;
                    }
                }
                break;
            }
            default:
                rec.is_corrupt = true;
                break;
        }

        return rec;
    }

    combined_records_t combine_record_parts(std::vector<record_t>&& records) {
        using group_key_t = std::tuple<id_t, uint64_t, uint32_t, uint8_t>;
        const auto key_of = [](const record_t& record) {
            return group_key_t{record.id,
                               record.transaction_id,
                               static_cast<uint32_t>(record.table_oid),
                               static_cast<uint8_t>(record.record_type)};
        };
        std::map<group_key_t, std::vector<std::size_t>> groups;
        for (std::size_t index = 0; index < records.size(); ++index) {
            if (records[index].is_part()) {
                groups[key_of(records[index])].push_back(index);
            }
        }

        // Whole: as many parts as each names, every index once, all valid and of one shape.
        const auto whole = [&records](std::vector<std::size_t>& members) {
            const auto& first = records[members.front()];
            if (members.size() != first.part_count) {
                return false;
            }
            std::sort(members.begin(), members.end(), [&records](std::size_t left, std::size_t right) {
                return records[left].part_index < records[right].part_index;
            });
            for (std::size_t position = 0; position < members.size(); ++position) {
                const auto& part = records[members[position]];
                if (!part.is_valid() || part.part_index != position || part.part_count != first.part_count ||
                    part.sub_id != first.sub_id || part.physical_attoids != first.physical_attoids) {
                    return false;
                }
            }
            return true;
        };

        combined_records_t combined;
        combined.records.reserve(records.size());
        for (std::size_t index = 0; index < records.size(); ++index) {
            if (!records[index].is_part()) {
                combined.records.push_back(std::move(records[index]));
                continue;
            }
            auto group = groups.find(key_of(records[index]));
            if (group == groups.end()) {
                continue; // already joined, or dropped, at its first part
            }
            auto members = std::move(group->second);
            groups.erase(group);
            if (!whole(members)) {
                ++combined.dropped_groups;
                continue;
            }
            record_t joined = std::move(records[members.front()]);
            for (std::size_t position = 1; position < members.size(); ++position) {
                auto& part = records[members[position]];
                for (auto& chunk : part.physical_data) {
                    joined.physical_data.push_back(std::move(chunk));
                }
                joined.physical_row_ids.insert(joined.physical_row_ids.end(),
                                               part.physical_row_ids.begin(),
                                               part.physical_row_ids.end());
                joined.physical_row_count += part.physical_row_count;
            }
            joined.record_type = whole_record_type(joined.record_type);
            joined.part_index = 0;
            joined.part_count = 1;
            combined.records.push_back(std::move(joined));
        }
        return combined;
    }

    crc32_t extract_crc(const buffer_t& buffer) { return extract_crc(buffer.data(), buffer.size()); }

    crc32_t extract_crc(const char* data, size_t len) {
        if (len < 4) {
            return 0;
        }
        return read_le32(data + len - 4);
    }

} // namespace services::wal
