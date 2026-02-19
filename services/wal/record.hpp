#pragma once

#include "dto.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace services::wal {

    enum class wal_record_type : uint8_t {
        DATA = 0,
        COMMIT = 1,
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

        bool is_valid() const { return size > 0; }
        bool is_commit_marker() const { return record_type == wal_record_type::COMMIT; }
    };

} // namespace services::wal
