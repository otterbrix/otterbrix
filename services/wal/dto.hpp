#pragma once

#include "base.hpp"

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <msgpack.hpp>

namespace services::wal {

    using buffer_t = std::pmr::string;
    using components::logical_plan::node_type;

    using size_tt = std::uint32_t;
    using crc32_t = std::uint32_t;

    struct wal_entry_t final {
        size_tt size_{};
        components::logical_plan::node_ptr entry_ = nullptr;
        components::logical_plan::parameter_node_ptr params_ = nullptr;
        crc32_t last_crc32_{};
        id_t id_{};
        crc32_t crc32_{};
    };

    crc32_t pack(buffer_t& storage, char* data, size_t size);
    buffer_t read_payload(buffer_t& input, size_tt index_start, size_tt index_stop);
    crc32_t read_crc32(buffer_t& input, size_tt index_start);
    size_tt read_size_impl(buffer_t& input, size_tt index_start);

    crc32_t pack(buffer_t& storage,
                 crc32_t last_crc32,
                 id_t id,
                 const components::logical_plan::node_ptr& data,
                 const components::logical_plan::parameter_node_ptr& params);

    void unpack(buffer_t& storage, wal_entry_t& entry);

    id_t unpack_wal_id(buffer_t& storage);

} //namespace services::wal
