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
        PHYSICAL_UPDATE = 12,
        // Dynamic-schema growth for computed (relkind='g') tables and for the columns an
        // ALTER TABLE ADD COLUMN materializes on first INSERT. Written BEFORE the
        // PHYSICAL_INSERT that depends on the new columns so WAL-first replay re-applies
        // the schema change first. Payload is a 0-row data_chunk whose columns ARE the
        // new columns (alias-tagged types). Idempotent on replay (already-present
        // columns are skipped).
        PHYSICAL_ADD_COLUMN = 13,
    };

    struct record_t final {
        // THE RESOURCE ARRIVES AT CONSTRUCTION, AND THERE IS NO WAY TO SKIP IT.
        //
        // physical_data / physical_row_ids used to carry default member initialisers naming
        // std::pmr::get_default_resource() — rule 14's forbidden resource, written out. That was
        // not merely a spelling problem: a pmr move-assignment does NOT adopt the source's
        // allocator (propagate_on_container_move_assignment is false), so decode_record's
        // `rec.physical_data = deserialize_chunk_batch(..., resource, ...)` allocated with the
        // TARGET's allocator and put every replayed WAL payload on the process-global arena, no
        // matter which resource the caller named. Assigning a correctly-placed payload in could
        // not repair it; only constructing the record on the right resource can, which is why
        // the default constructor is gone rather than merely discouraged.
        //
        // The header scalars are zeroed here for a second reason: decode_record fills last_crc32
        // and id only AFTER the CRC check, so a record refused for a bad CRC left them
        // INDETERMINATE, and every reader that logs or compares a corrupt record's id read
        // uninitialised memory. Zero is the value a corrupt record reports.
        explicit record_t(std::pmr::memory_resource* resource)
            : physical_data(resource)
            , physical_row_ids(resource) {}

        size_tt size{0};
        crc32_t crc32{0};
        crc32_t last_crc32{0};
        id_t id{0};
        uint64_t transaction_id{0};
        // MVCC commit_id from txn_manager_->commit(); lets snapshot-aware
        // replay restore published_horizon_ and the in_flight set. 0 on
        // non-COMMIT records.
        uint64_t commit_id{0};
        wal_record_type record_type{wal_record_type::COMMIT};

        // Physical WAL fields. physical_data holds the record's payload as a batch of
        // ≤DEFAULT_VECTOR_CAPACITY chunks (empty for DELETE / no-payload records).
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        std::pmr::vector<components::vector::data_chunk_t> physical_data;
        std::pmr::vector<int64_t> physical_row_ids;
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
                   record_type == wal_record_type::PHYSICAL_ADD_COLUMN;
        }
    };

} // namespace services::wal
