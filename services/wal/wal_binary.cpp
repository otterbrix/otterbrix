#include "wal_binary.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

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
        void serialize_chunk_batch(buffer_t& payload_buf,
                                   const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
            const size_t count_pos = payload_buf.size();
            payload_buf.resize(count_pos + 4);
            write_le32(payload_buf.data() + count_pos, static_cast<uint32_t>(chunks.size()));
            for (const auto& chunk : chunks) {
                const size_t len_pos = payload_buf.size();
                payload_buf.resize(len_pos + 4); // size placeholder
                const size_t data_start = payload_buf.size();
                components::vector::serialize_binary(chunk, payload_buf);
                const auto chunk_size = static_cast<uint32_t>(payload_buf.size() - data_start);
                write_le32(payload_buf.data() + len_pos, chunk_size);
            }
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

        // DML header: [size:4][last_crc32:4][wal_id:8][txn_id:8][record_type:1][table_oid:4]
        // [row_start:8][row_count:8][payload_size:4][payload][crc32:4].

        static constexpr size_t DML_FIXED_HEADER = 4    // last_crc32
                                                   + 8  // wal_id
                                                   + 8  // txn_id
                                                   + 1  // record_type
                                                   + 4  // table_oid
                                                   + 8  // row_start
                                                   + 8  // row_count
                                                   + 4; // payload_size

        crc32_t write_dml_record(buffer_t& buffer,
                                 crc32_t last_crc32,
                                 id_t wal_id,
                                 uint64_t txn_id,
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

    } // anonymous namespace

    crc32_t encode_insert(buffer_t& buffer,
                          std::pmr::memory_resource* /*resource*/,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                          uint64_t row_start,
                          uint64_t row_count) {
        buffer_t payload_buf(buffer.get_allocator());
        serialize_chunk_batch(payload_buf, chunks);

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                wal_record_type::PHYSICAL_INSERT,
                                table_oid,
                                row_start,
                                row_count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    // Schema-growth record: payload is a 0-row data_chunk whose columns are the new ones; row_count = new-column count.
    crc32_t encode_add_column(buffer_t& buffer,
                              crc32_t last_crc32,
                              id_t wal_id,
                              uint64_t txn_id,
                              components::catalog::oid_t table_oid,
                              const components::vector::data_chunk_t& schema_chunk,
                              uint64_t column_count) {
        buffer_t payload_buf(buffer.get_allocator());
        // Framed as a one-element chunk batch, not bare serialize_binary, so the leading-count
        // read in deserialize_chunk_batch doesn't misfire.
        std::pmr::vector<components::vector::data_chunk_t> schema_batch{payload_buf.get_allocator().resource()};
        components::vector::data_chunk_t schema_copy(schema_chunk.resource(),
                                                     schema_chunk.types(),
                                                     std::max<uint64_t>(schema_chunk.size(), 1));
        schema_chunk.copy(schema_copy, 0);
        schema_copy.set_cardinality(schema_chunk.size());
        schema_batch.emplace_back(std::move(schema_copy));
        serialize_chunk_batch(payload_buf, schema_batch);

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                wal_record_type::PHYSICAL_ADD_COLUMN,
                                table_oid,
                                0,
                                column_count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    crc32_t encode_delete(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          uint64_t count) {
        // Payload = raw int64_t array.
        const auto payload_size = static_cast<uint32_t>(count * sizeof(int64_t));

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                wal_record_type::PHYSICAL_DELETE,
                                table_oid,
                                0,
                                count,
                                reinterpret_cast<const char*>(row_ids),
                                payload_size);
    }

    crc32_t encode_update(buffer_t& buffer,
                          std::pmr::memory_resource* /*resource*/,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          const std::pmr::vector<components::vector::data_chunk_t>& new_chunks,
                          uint64_t count) {
        // Payload: [row_ids_size:4][row_ids][chunk_batch: serialize_chunk_batch's own framing].

        buffer_t payload_buf(buffer.get_allocator());

        const auto row_ids_bytes = static_cast<uint32_t>(count * sizeof(int64_t));

        payload_buf.resize(4 + row_ids_bytes);
        char* p = payload_buf.data();
        write_le32(p, row_ids_bytes);
        p += 4;
        std::memcpy(p, row_ids, row_ids_bytes);

        serialize_chunk_batch(payload_buf, new_chunks);

        return write_dml_record(buffer,
                                last_crc32,
                                wal_id,
                                txn_id,
                                wal_record_type::PHYSICAL_UPDATE,
                                table_oid,
                                0,
                                count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    // COMMIT (37 bytes): [size:4][last_crc32:4][wal_id:8][txn_id:8][record_type:1=COMMIT][commit_id:8][crc32:4].
    crc32_t encode_commit(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t commit_id) {
        // commit_id sits after the type byte so its offset matches DML records for decode.
        static constexpr uint32_t COMMIT_BODY_SIZE = 4 + 8 + 8 + 1 + 8;
        static constexpr size_t COMMIT_TOTAL = 4 + COMMIT_BODY_SIZE + 4;

        const size_t base = buffer.size();
        buffer.resize(base + COMMIT_TOTAL);
        char* out = buffer.data() + base;

        write_le32(out, COMMIT_BODY_SIZE);
        out += 4;

        char* crc_start = out;

        write_le32(out, last_crc32);
        out += 4;
        write_le64(out, wal_id);
        out += 8;
        write_le64(out, txn_id);
        out += 8;
        *reinterpret_cast<uint8_t*>(out) = static_cast<uint8_t>(wal_record_type::COMMIT);
        out += 1;
        write_le64(out, commit_id);
        out += 8;

        assert(static_cast<size_t>(out - crc_start) == COMMIT_BODY_SIZE);

        crc32_t crc = compute_crc(crc_start, COMMIT_BODY_SIZE);
        write_le32(out, crc);

        return crc;
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

        if (static_cast<size_t>(body_size) < DML_FIXED_HEADER) {
            rec.is_corrupt = true;
            return rec;
        }

        rec.table_oid = static_cast<components::catalog::oid_t>(read_le32(ptr));
        ptr += 4;
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

        switch (rec.record_type) {
            case wal_record_type::PHYSICAL_INSERT:
            // PHYSICAL_ADD_COLUMN shares INSERT's payload shape: a serialized data_chunk (0-row for schema records).
            case wal_record_type::PHYSICAL_ADD_COLUMN: {
                if (payload_size > 0) {
                    bool ok = false;
                    rec.physical_data = deserialize_chunk_batch(payload, payload_size, resource, ok);
                    if (!ok) {
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

                const char* chunk_data = row_ids_data + row_ids_bytes;
                uint32_t chunk_batch_size = payload_size - 4 - row_ids_bytes;
                if (chunk_batch_size > 0) {
                    bool ok = false;
                    rec.physical_data = deserialize_chunk_batch(chunk_data, chunk_batch_size, resource, ok);
                    if (!ok) {
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

    crc32_t extract_crc(const buffer_t& buffer) { return extract_crc(buffer.data(), buffer.size()); }

    crc32_t extract_crc(const char* data, size_t len) {
        if (len < 4) {
            return 0;
        }
        return read_le32(data + len - 4);
    }

} // namespace services::wal
