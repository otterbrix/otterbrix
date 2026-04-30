#include "wal_binary.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

#include <absl/crc/crc32c.h>

#include <components/vector/data_chunk.hpp>
#include <components/vector/data_chunk_binary.hpp>

namespace services::wal {

    // -----------------------------------------------------------------------
    // Little-endian helpers (no-op on x86/ARM-LE, but correct everywhere).
    // Using memcpy lets the compiler emit the optimal load/store on any ISA.
    // -----------------------------------------------------------------------
    namespace {

        inline void write_le16(char* dst, uint16_t v) { std::memcpy(dst, &v, 2); }
        inline void write_le32(char* dst, uint32_t v) { std::memcpy(dst, &v, 4); }
        inline void write_le64(char* dst, uint64_t v) { std::memcpy(dst, &v, 8); }

        inline uint16_t read_le16(const char* src) {
            uint16_t v;
            std::memcpy(&v, src, 2);
            return v;
        }
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

        // CRC32-C over an arbitrary byte range.
        crc32_t compute_crc(const char* data, size_t len) {
            auto crc = absl::ComputeCrc32c(absl::string_view(data, len));
            return static_cast<crc32_t>(crc);
        }

        // -----------------------------------------------------------------
        // DML header (common to INSERT/DELETE/UPDATE):
        //
        //   [size:4]         <- payload_size (bytes between size and trailing crc32)
        //   [last_crc32:4]
        //   [wal_id:8]
        //   [txn_id:8]
        //   [record_type:1]
        //   [db_len:2]
        //   [coll_len:2]
        //   [row_start:8]
        //   [row_count:8]
        //   [payload_size:4]
        //   [database: db_len]
        //   [collection: coll_len]
        //   [payload: payload_size bytes]
        //   [crc32:4]
        //
        // "size" counts everything from last_crc32 through payload (inclusive),
        // i.e. everything the CRC covers.
        // -----------------------------------------------------------------

        static constexpr size_t DML_FIXED_HEADER =
            4  // last_crc32
            + 8  // wal_id
            + 8  // txn_id
            + 1  // record_type
            + 2  // db_len
            + 2  // coll_len
            + 8  // row_start
            + 8  // row_count
            + 4; // payload_size
            // = 45

        // Write a complete DML record. Returns the CRC of that record.
        crc32_t write_dml_record(buffer_t& buffer,
                                 crc32_t last_crc32,
                                 id_t wal_id,
                                 uint64_t txn_id,
                                 wal_record_type rtype,
                                 const std::string& database,
                                 const std::string& collection,
                                 uint64_t row_start,
                                 uint64_t row_count,
                                 const char* payload,
                                 uint32_t payload_size) {
            const auto db_len = static_cast<uint16_t>(database.size());
            const auto coll_len = static_cast<uint16_t>(collection.size());

            // "size" = bytes from last_crc32 up to end-of-payload (before trailing crc32)
            const uint32_t size_field = static_cast<uint32_t>(
                DML_FIXED_HEADER + db_len + coll_len + payload_size);

            const size_t total = 4 /*size*/ + size_field + 4 /*crc32*/;
            const size_t base = buffer.size();
            buffer.resize(base + total);
            char* out = buffer.data() + base;

            // size
            write_le32(out, size_field);
            out += 4;

            // --- CRC-covered region starts here ---
            char* crc_start = out;

            write_le32(out, last_crc32);
            out += 4;
            write_le64(out, wal_id);
            out += 8;
            write_le64(out, txn_id);
            out += 8;
            *reinterpret_cast<uint8_t*>(out) = static_cast<uint8_t>(rtype);
            out += 1;
            write_le16(out, db_len);
            out += 2;
            write_le16(out, coll_len);
            out += 2;
            write_le64(out, row_start);
            out += 8;
            write_le64(out, row_count);
            out += 8;
            write_le32(out, payload_size);
            out += 4;

            // variable-length strings
            std::memcpy(out, database.data(), db_len);
            out += db_len;
            std::memcpy(out, collection.data(), coll_len);
            out += coll_len;

            // payload
            if (payload_size > 0) {
                std::memcpy(out, payload, payload_size);
                out += payload_size;
            }

            assert(static_cast<size_t>(out - crc_start) == size_field);

            // compute CRC over the body
            crc32_t crc = compute_crc(crc_start, size_field);
            write_le32(out, crc);

            return crc;
        }

    } // anonymous namespace

    // -----------------------------------------------------------------------
    // encode_insert
    // -----------------------------------------------------------------------
    crc32_t encode_insert(buffer_t& buffer,
                          std::pmr::memory_resource* /*resource*/,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          const std::string& database,
                          const std::string& collection,
                          const components::vector::data_chunk_t& data_chunk,
                          uint64_t row_start,
                          uint64_t row_count) {
        // Serialize the data_chunk into a temporary buffer.
        buffer_t payload_buf(buffer.get_allocator());
        components::vector::serialize_binary(data_chunk, payload_buf);

        return write_dml_record(buffer, last_crc32, wal_id, txn_id,
                                wal_record_type::PHYSICAL_INSERT,
                                database, collection,
                                row_start, row_count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    // -----------------------------------------------------------------------
    // encode_delete
    // -----------------------------------------------------------------------
    crc32_t encode_delete(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          const std::string& database,
                          const std::string& collection,
                          const int64_t* row_ids,
                          uint64_t count) {
        // Payload = raw int64_t array.
        const auto payload_size = static_cast<uint32_t>(count * sizeof(int64_t));

        return write_dml_record(buffer, last_crc32, wal_id, txn_id,
                                wal_record_type::PHYSICAL_DELETE,
                                database, collection,
                                0, count,
                                reinterpret_cast<const char*>(row_ids),
                                payload_size);
    }

    // -----------------------------------------------------------------------
    // encode_update
    // -----------------------------------------------------------------------
    crc32_t encode_update(buffer_t& buffer,
                          std::pmr::memory_resource* /*resource*/,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          const std::string& database,
                          const std::string& collection,
                          const int64_t* row_ids,
                          const components::vector::data_chunk_t& new_data,
                          uint64_t count) {
        // Payload layout for UPDATE:
        //   [row_ids_size : 4 LE]          // byte count of the row-ids block
        //   [row_ids      : row_ids_size]
        //   [data_chunk   : remainder]

        buffer_t payload_buf(buffer.get_allocator());

        const auto row_ids_bytes = static_cast<uint32_t>(count * sizeof(int64_t));

        // Reserve for the 4-byte length prefix + row_ids.
        payload_buf.resize(4 + row_ids_bytes);
        char* p = payload_buf.data();
        write_le32(p, row_ids_bytes);
        p += 4;
        std::memcpy(p, row_ids, row_ids_bytes);

        // Append serialised data_chunk.
        components::vector::serialize_binary(new_data, payload_buf);

        return write_dml_record(buffer, last_crc32, wal_id, txn_id,
                                wal_record_type::PHYSICAL_UPDATE,
                                database, collection,
                                0, count,
                                payload_buf.data(),
                                static_cast<uint32_t>(payload_buf.size()));
    }

    // -----------------------------------------------------------------------
    // encode_commit
    //
    // Compact layout (29 bytes total):
    //   [size:4]  = 21
    //   [last_crc32:4]
    //   [wal_id:8]
    //   [txn_id:8]
    //   [record_type:1]   = COMMIT (1)
    //   [crc32:4]
    // -----------------------------------------------------------------------
    crc32_t encode_commit(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id) {
        static constexpr uint32_t COMMIT_BODY_SIZE = 4 + 8 + 8 + 1; // = 21
        static constexpr size_t COMMIT_TOTAL = 4 + COMMIT_BODY_SIZE + 4; // = 29

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

        assert(static_cast<size_t>(out - crc_start) == COMMIT_BODY_SIZE);

        crc32_t crc = compute_crc(crc_start, COMMIT_BODY_SIZE);
        write_le32(out, crc);

        return crc;
    }

    // -----------------------------------------------------------------------
    // decode_record
    // -----------------------------------------------------------------------
    record_t decode_record(const buffer_t& buffer, std::pmr::memory_resource* resource) {
        return decode_record(buffer.data(), buffer.size(), resource);
    }

    record_t decode_record(const char* data, size_t len, std::pmr::memory_resource* resource) {
        record_t rec;
        rec.is_corrupt = false;

        // Minimum valid record is a COMMIT at 29 bytes.
        if (len < 29) {
            rec.is_corrupt = true;
            rec.size = 0;
            return rec;
        }

        const char* ptr = data;

        // --- size ---
        uint32_t body_size = read_le32(ptr);
        ptr += 4;

        if (4 + body_size + 4 > len) {
            rec.is_corrupt = true;
            rec.size = 0;
            return rec;
        }

        rec.size = static_cast<size_tt>(4 + body_size + 4);

        // CRC check: body starts at ptr, has body_size bytes.
        const char* body_start = ptr;
        crc32_t expected_crc = read_le32(body_start + body_size);
        crc32_t actual_crc = compute_crc(body_start, body_size);

        if (expected_crc != actual_crc) {
            rec.is_corrupt = true;
            rec.crc32 = expected_crc;
            return rec;
        }
        rec.crc32 = actual_crc;

        // --- Common fields ---
        rec.last_crc32 = read_le32(ptr);
        ptr += 4;
        rec.id = read_le64(ptr);
        ptr += 8;
        rec.transaction_id = read_le64(ptr);
        ptr += 8;
        rec.record_type = static_cast<wal_record_type>(*reinterpret_cast<const uint8_t*>(ptr));
        ptr += 1;

        // COMMIT record has no further fields.
        if (rec.record_type == wal_record_type::COMMIT) {
            return rec;
        }

        // --- DML fields ---
        if (static_cast<size_t>(body_size) < DML_FIXED_HEADER) {
            rec.is_corrupt = true;
            return rec;
        }

        uint16_t db_len = read_le16(ptr);
        ptr += 2;
        uint16_t coll_len = read_le16(ptr);
        ptr += 2;
        rec.physical_row_start = read_le64(ptr);
        ptr += 8;
        rec.physical_row_count = read_le64(ptr);
        ptr += 8;
        uint32_t payload_size = read_le32(ptr);
        ptr += 4;

        // Bounds check on variable-length data.
        if (static_cast<size_t>(DML_FIXED_HEADER + db_len + coll_len + payload_size) != body_size) {
            rec.is_corrupt = true;
            return rec;
        }

        rec.collection_name.database.assign(ptr, db_len);
        ptr += db_len;
        rec.collection_name.collection.assign(ptr, coll_len);
        ptr += coll_len;

        const char* payload = ptr;

        // --- Type-specific payload decoding ---
        switch (rec.record_type) {
            case wal_record_type::PHYSICAL_INSERT: {
                if (payload_size > 0) {
                    rec.physical_data = std::make_unique<components::vector::data_chunk_t>(
                        components::vector::deserialize_binary(payload, payload_size, resource));
                }
                break;
            }
            case wal_record_type::PHYSICAL_DELETE: {
                uint64_t count = payload_size / sizeof(int64_t);
                rec.physical_row_ids = std::pmr::vector<int64_t>(resource);
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
                if (4 + row_ids_bytes > payload_size) {
                    rec.is_corrupt = true;
                    return rec;
                }
                uint64_t id_count = row_ids_bytes / sizeof(int64_t);
                rec.physical_row_ids = std::pmr::vector<int64_t>(resource);
                rec.physical_row_ids.resize(id_count);
                std::memcpy(rec.physical_row_ids.data(), row_ids_data, row_ids_bytes);

                const char* chunk_data = row_ids_data + row_ids_bytes;
                uint32_t chunk_size = payload_size - 4 - row_ids_bytes;
                if (chunk_size > 0) {
                    rec.physical_data = std::make_unique<components::vector::data_chunk_t>(
                        components::vector::deserialize_binary(chunk_data, chunk_size, resource));
                }
                break;
            }
            default:
                rec.is_corrupt = true;
                break;
        }

        return rec;
    }

    // -----------------------------------------------------------------------
    // extract_crc
    // -----------------------------------------------------------------------
    crc32_t extract_crc(const buffer_t& buffer) {
        return extract_crc(buffer.data(), buffer.size());
    }

    crc32_t extract_crc(const char* data, size_t len) {
        if (len < 4) {
            return 0;
        }
        return read_le32(data + len - 4);
    }

} // namespace services::wal
