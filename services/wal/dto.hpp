#pragma once

#include "base.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/vector/data_chunk.hpp>
#include <msgpack.hpp>

namespace services::wal {

    using buffer_t = std::pmr::string;

    using size_tt = std::uint32_t;
    using crc32_t = std::uint32_t;

    crc32_t pack(buffer_t& storage, char* data, size_t size);

    crc32_t pack_commit_marker(buffer_t& storage, crc32_t last_crc32, id_t id, uint64_t transaction_id);

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

    id_t unpack_wal_id(buffer_t& storage);

} //namespace services::wal
