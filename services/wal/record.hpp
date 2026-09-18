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
        // Written BEFORE the dependent PHYSICAL_INSERT so WAL-first replay applies the schema
        // change first; payload is a 0-row data_chunk whose columns ARE the new ones, idempotent on replay.
        PHYSICAL_ADD_COLUMN = 13,
    };

    struct record_t final {
        // Resource must arrive at construction: pmr move-assignment doesn't adopt the source's
        // allocator, so assigning it after construction left decode_record allocating on the
        // process-global arena; last_crc32/id are likewise zeroed until the CRC check succeeds.
        explicit record_t(std::pmr::memory_resource* resource)
            : physical_data(resource)
            , physical_row_ids(resource) {}

        size_tt size{0};
        crc32_t crc32{0};
        crc32_t last_crc32{0};
        id_t id{0};
        uint64_t transaction_id{0};
        // From txn_manager_->commit(); replay uses it to restore published_horizon_/in_flight; 0 on non-COMMIT records.
        uint64_t commit_id{0};
        wal_record_type record_type{wal_record_type::COMMIT};

        // physical_data batches the payload as ≤DEFAULT_VECTOR_CAPACITY chunks; empty for DELETE/no-payload records.
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        std::pmr::vector<components::vector::data_chunk_t> physical_data;
        std::pmr::vector<int64_t> physical_row_ids;
        uint64_t physical_row_start{0};
        uint64_t physical_row_count{0};
        core::date::timezone_offset_t session_tz{};

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
