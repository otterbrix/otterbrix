#pragma once

#include "base.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

namespace services::wal {

    enum class wal_record_type : uint8_t
    {
        COMMIT = 1,
        PHYSICAL_INSERT = 10,
        PHYSICAL_DELETE = 11,
        // MVCC update: the live path tombstones the old rows and APPENDS the new ones
        // at the end of the table. row_ids are therefore the OLD locations (what replay
        // must tombstone) and physical_row_start is where the new rows landed. Replay has
        // to reproduce that, or the replayed row-id space diverges from the live one and
        // every later row-id-keyed record lands on the wrong slot.
        PHYSICAL_UPDATE = 12,
        // Dynamic-schema growth for IN_MEMORY / computed tables. Written BEFORE the
        // PHYSICAL_INSERT that depends on the new columns so WAL-first replay re-applies
        // the schema change first. Payload is a 0-row data_chunk whose columns ARE the
        // new columns (alias-tagged types). Idempotent on replay (already-present
        // columns are skipped).
        PHYSICAL_ADD_COLUMN = 13,
        // In-place row rewrite, by row_id, with a full-width chunk. The row does NOT move
        // and no row-ids are consumed, so physical_row_start is meaningless here.
        //
        // Written only by the pg_attribute commit-id backfill, which patches an existing
        // catalog row's added_at / dropped_at fields. That row must keep its identity: an
        // MVCC replay would tombstone it and re-append it, leaving two live pg_attribute
        // rows for one attribute -- a dropped column would come back, or appear twice.
        // Same payload layout as PHYSICAL_UPDATE.
        PHYSICAL_UPDATE_INPLACE = 14,
    };

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        uint64_t transaction_id{0};
        // MVCC commit_id from txn_manager_->commit(); lets snapshot-aware
        // replay restore published_horizon_ and the in_flight set. 0 on
        // non-COMMIT records.
        uint64_t commit_id{0};
        wal_record_type record_type{wal_record_type::COMMIT};

        // Physical WAL fields. physical_data holds the record's payload as a batch of
        // ≤DEFAULT_VECTOR_CAPACITY chunks (empty for DELETE / no-payload records).
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        std::pmr::vector<components::vector::data_chunk_t> physical_data{std::pmr::get_default_resource()};
        std::pmr::vector<int64_t> physical_row_ids{std::pmr::get_default_resource()};
        uint64_t physical_row_start{0};
        uint64_t physical_row_count{0};
        core::date::timezone_offset_t session_tz{};

        // Error tracking
        bool is_corrupt{false};

        bool is_valid() const { return size > 0 && !is_corrupt; }
        bool is_commit_marker() const { return record_type == wal_record_type::COMMIT; }
        bool is_physical() const {
            return record_type == wal_record_type::PHYSICAL_INSERT || record_type == wal_record_type::PHYSICAL_DELETE ||
                   record_type == wal_record_type::PHYSICAL_UPDATE ||
                   record_type == wal_record_type::PHYSICAL_UPDATE_INPLACE ||
                   record_type == wal_record_type::PHYSICAL_ADD_COLUMN;
        }
    };

} // namespace services::wal
