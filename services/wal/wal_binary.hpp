#pragma once

#include <memory_resource>
#include <string>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

namespace components::vector {
    class data_chunk_t;
} // namespace components::vector

namespace services::wal {

    // -----------------------------------------------------------------------
    // Encode functions
    //
    // Each appends a complete binary WAL record to \p buffer and returns
    // the CRC32 of the freshly written record (which becomes the
    // "last_crc32" for the next record in the chain).
    //
    // Records carry table_oid (4 bytes) instead of (database, collection)
    // strings.
    // -----------------------------------------------------------------------

    // The whole chunk batch is written into ONE record (count-prefixed, each chunk
    // length-prefixed) so a torn write can never leave a partially-applied batch.
    crc32_t encode_insert(buffer_t& buffer,
                          std::pmr::memory_resource* resource,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                          uint64_t row_start,
                          uint64_t row_count);

    crc32_t encode_delete(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          uint64_t count);

    // Numbering-epoch boundary for `table_oid` (see wal_record_type::PHYSICAL_COMPACT).
    // Payload-free: carries only the table_oid; `compact_watermark` is stored in
    // physical_row_start for forensics and is not load-bearing on replay. txn_id is 0
    // (a system write that re-derives over already-committed state).
    crc32_t encode_compact(buffer_t& buffer,
                           crc32_t last_crc32,
                           id_t wal_id,
                           uint64_t txn_id,
                           components::catalog::oid_t table_oid,
                           uint64_t compact_watermark);

    // Schema-growth record: payload is a 0-row data_chunk whose columns ARE the
    // new columns (alias-tagged types). Written BEFORE the dependent PHYSICAL_INSERT.
    crc32_t encode_add_column(buffer_t& buffer,
                              crc32_t last_crc32,
                              id_t wal_id,
                              uint64_t txn_id,
                              components::catalog::oid_t table_oid,
                              const components::vector::data_chunk_t& schema_chunk,
                              uint64_t column_count);

    // `row_start` is where the new row images LANDED. An MVCC update deletes the old
    // rows and appends the new ones at the end of the table, so the new rows do not sit
    // at `row_ids` -- those are the OLD locations, which the record carries only so that
    // replay knows what to tombstone. Consumers that need to address the new rows (the
    // CREATE INDEX WAL catch-up) map the g-th row of the record to `row_start + g`.
    //
    // `record_type` selects the replay contract: PHYSICAL_UPDATE (the MVCC default) or
    // PHYSICAL_UPDATE_INPLACE. Both share this payload layout.
    crc32_t encode_update(buffer_t& buffer,
                          std::pmr::memory_resource* resource,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          const std::pmr::vector<components::vector::data_chunk_t>& new_chunks,
                          uint64_t row_start,
                          uint64_t count,
                          wal_record_type record_type = wal_record_type::PHYSICAL_UPDATE);

    // commit_id (from transaction_manager_t::commit()) is appended to COMMIT
    // records so replay can rebuild published_horizon_.
    crc32_t encode_commit(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t commit_id);

    // -----------------------------------------------------------------------
    // Decode
    // -----------------------------------------------------------------------

    /// Parse a single WAL record from a buffer.
    record_t decode_record(const buffer_t& buffer, std::pmr::memory_resource* resource);

    /// Parse a single WAL record from raw memory.
    record_t decode_record(const char* data, size_t len, std::pmr::memory_resource* resource);

    // -----------------------------------------------------------------------
    // CRC helpers
    // -----------------------------------------------------------------------

    /// Extract the CRC that is stored in the last 4 bytes of an encoded record.
    crc32_t extract_crc(const buffer_t& buffer);
    crc32_t extract_crc(const char* data, size_t len);

} // namespace services::wal
