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

    // Schema-growth record: payload is a 0-row data_chunk whose columns ARE the
    // new columns (alias-tagged types). Written BEFORE the dependent PHYSICAL_INSERT.
    crc32_t encode_add_column(buffer_t& buffer,
                              crc32_t last_crc32,
                              id_t wal_id,
                              uint64_t txn_id,
                              components::catalog::oid_t table_oid,
                              const components::vector::data_chunk_t& schema_chunk,
                              uint64_t column_count);

    crc32_t encode_update(buffer_t& buffer,
                          std::pmr::memory_resource* resource,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          const std::pmr::vector<components::vector::data_chunk_t>& new_chunks,
                          uint64_t count);

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
