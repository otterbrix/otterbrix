#pragma once

#include <cstddef>
#include <memory_resource>
#include <string>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <core/result_wrapper.hpp>
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

    // The shape every physical record must have: one attoid per column of every chunk. The reader refuses a
    // record that breaks it, so the writers refuse it first.
    bool attoids_match_chunks(const column_attoids_t& attoids,
                              const std::pmr::vector<components::vector::data_chunk_t>& chunks);

    // The whole chunk batch as ONE record (count-prefixed, each chunk length-prefixed); a batch one record
    // cannot hold is refused (the writers cut it into parts instead, below). A chunk the codec cannot carry
    // refuses the record (serialize_binary), and \p buffer is left as it was; likewise for encode_add_column
    // and encode_update.
    [[nodiscard]] core::result_wrapper_t<crc32_t>
    encode_insert(buffer_t& buffer,
                  std::pmr::memory_resource* resource,
                  crc32_t last_crc32,
                  id_t wal_id,
                  uint64_t txn_id,
                  uint64_t sub_id,
                  components::catalog::oid_t table_oid,
                  const column_attoids_t& attoids,
                  const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                  uint64_t row_start,
                  uint64_t row_count);

    crc32_t encode_delete(buffer_t& buffer,
                          crc32_t last_crc32,
                          id_t wal_id,
                          uint64_t txn_id,
                          uint64_t sub_id,
                          components::catalog::oid_t table_oid,
                          const int64_t* row_ids,
                          uint64_t count);

    // Schema-growth record: payload is a data_chunk whose columns ARE the new columns (alias-tagged
    // types), with one row holding each column's default. Written BEFORE the dependent PHYSICAL_INSERT.
    [[nodiscard]] core::result_wrapper_t<crc32_t>
    encode_add_column(buffer_t& buffer,
                      crc32_t last_crc32,
                      id_t wal_id,
                      uint64_t txn_id,
                      uint64_t sub_id,
                      components::catalog::oid_t table_oid,
                      const column_attoids_t& attoids,
                      const components::vector::data_chunk_t& schema_chunk,
                      uint64_t first_position);

    [[nodiscard]] core::result_wrapper_t<crc32_t>
    encode_update(buffer_t& buffer,
                  std::pmr::memory_resource* resource,
                  crc32_t last_crc32,
                  id_t wal_id,
                  uint64_t txn_id,
                  uint64_t sub_id,
                  components::catalog::oid_t table_oid,
                  const int64_t* row_ids,
                  const column_attoids_t& attoids,
                  const std::pmr::vector<components::vector::data_chunk_t>& new_chunks,
                  uint64_t count);

    // -----------------------------------------------------------------------
    // Operations larger than one record
    //
    // An INSERT / UPDATE whose payload passes the part budget is written as a group of *_PART records
    // under its one wal id, each [part_index:4][part_count:4] + the whole type's payload for its chunks
    // (an UPDATE part carries those chunks' row ids). An operation that fits is one whole record, as before.
    // -----------------------------------------------------------------------

    // The payload one part carries at most; a chunk larger than that is a part of its own.
    inline constexpr std::size_t WAL_PART_PAYLOAD_BYTES = std::size_t{64} << 20;

    [[nodiscard]] std::size_t wal_part_payload_bytes() noexcept;
#ifdef DEV_MODE
    // Test seam: 0 restores WAL_PART_PAYLOAD_BYTES.
    void dev_set_wal_part_payload_bytes(std::size_t bytes) noexcept;
#endif

    struct wal_part_t {
        std::size_t first_chunk{0};
        std::size_t chunk_count{0};
        uint64_t first_row{0};
        uint64_t row_count{0};
    };

    // The operation's chunks, serialized once, and the parts they are cut into.
    struct wal_operation_t {
        std::pmr::vector<buffer_t> chunks;
        std::pmr::vector<wal_part_t> parts;
    };

    // Refuses a chunk the codec cannot carry (serialize_binary) and a chunk no record can hold.
    [[nodiscard]] core::result_wrapper_t<wal_operation_t>
    cut_into_parts(const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                   std::size_t attoid_count,
                   bool carries_row_ids,
                   std::size_t part_bytes,
                   std::pmr::memory_resource* resource);

    // One part of \p operation; with a single part it is the whole record, carrying \p row_count.
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
                               uint64_t row_count);

    // One part of \p operation; \p row_ids are the whole operation's, one per row of its chunks in order.
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
                               uint64_t count);

    struct combined_records_t {
        std::vector<record_t> records;
        std::size_t dropped_groups{0};
    };

    // The one place parts are joined, shared by both readers ahead of filter_committed_records: a whole group
    // becomes one record of its whole type, in place of its first part. A group missing a part, or whose parts
    // disagree, is a write that did not finish: it is dropped, as if never journaled, and counted.
    [[nodiscard]] combined_records_t combine_record_parts(std::vector<record_t>&& records);

    // commit_id (from transaction_manager_t::commit()) is appended to COMMIT
    // records so replay can rebuild published_horizon_.
    crc32_t encode_commit(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t commit_id);

    crc32_t
    encode_rollback_to(buffer_t& buffer, crc32_t last_crc32, id_t wal_id, uint64_t txn_id, uint64_t from_sub_id);

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
