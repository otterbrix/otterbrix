#pragma once

#include "dto.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

namespace services::wal {

    enum class wal_record_type : uint8_t {
        DATA = 0,
        COMMIT = 1,
        PHYSICAL_INSERT = 10,
        PHYSICAL_DELETE = 11,
        PHYSICAL_UPDATE = 12,
    };

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        uint64_t transaction_id{0};
        wal_record_type record_type{wal_record_type::DATA};
        components::logical_plan::node_ptr data;
        components::logical_plan::parameter_node_ptr params;

        // Physical WAL fields
        collection_full_name_t collection_name;
        std::unique_ptr<components::vector::data_chunk_t> physical_data;
        std::pmr::vector<int64_t> physical_row_ids{std::pmr::get_default_resource()};
        uint64_t physical_row_start{0};
        uint64_t physical_row_count{0};

        // Error tracking
        bool is_corrupt{false};

        bool is_valid() const { return size > 0 && !is_corrupt; }
        bool is_commit_marker() const { return record_type == wal_record_type::COMMIT; }
        bool is_physical() const {
            return record_type == wal_record_type::PHYSICAL_INSERT
                || record_type == wal_record_type::PHYSICAL_DELETE
                || record_type == wal_record_type::PHYSICAL_UPDATE;
        }
    };

} // namespace services::wal
