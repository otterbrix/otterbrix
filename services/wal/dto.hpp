#pragma once

#include "base.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>
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
        uint64_t transaction_id_{0};
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
                 const components::logical_plan::parameter_node_ptr& params,
                 uint64_t transaction_id = 0);

    crc32_t pack_commit_marker(buffer_t& storage, crc32_t last_crc32, id_t id, uint64_t transaction_id);

    // Physical WAL record pack functions
    crc32_t pack_physical_insert(buffer_t& storage,
                                 std::pmr::memory_resource* resource,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const components::vector::data_chunk_t& data_chunk,
                                 uint64_t row_start,
                                 uint64_t row_count);

    crc32_t pack_physical_delete(buffer_t& storage,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const std::pmr::vector<int64_t>& row_ids,
                                 uint64_t count);

    crc32_t pack_physical_update(buffer_t& storage,
                                 std::pmr::memory_resource* resource,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const std::pmr::vector<int64_t>& row_ids,
                                 const components::vector::data_chunk_t& new_data,
                                 uint64_t count);

    void unpack(buffer_t& storage, wal_entry_t& entry);

    id_t unpack_wal_id(buffer_t& storage);

} //namespace services::wal
