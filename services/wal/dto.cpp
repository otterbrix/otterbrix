#include "dto.hpp"
#include "record.hpp"

#include <components/serialization/serializer.hpp>

#include <absl/crc/crc32c.h>
#include <chrono>
#include <msgpack.hpp>
#include <unistd.h>

namespace services::wal {

    using buffer_element_t = char;

    void append_crc32(buffer_t& storage, crc32_t crc32) {
        storage.push_back(buffer_element_t(crc32 >> 24 & 0xff));
        storage.push_back(buffer_element_t(crc32 >> 16 & 0xff));
        storage.push_back(buffer_element_t(crc32 >> 8 & 0xff));
        storage.push_back(buffer_element_t(crc32 & 0xff));
    }

    void append_size(buffer_t& storage, size_tt size) {
        storage.push_back(buffer_element_t((size >> 24) & 0xff));
        storage.push_back(buffer_element_t((size >> 16) & 0xff));
        storage.push_back(buffer_element_t((size >> 8) & 0xff));
        storage.push_back(buffer_element_t(size & 0xff));
    }

    void append_payload(buffer_t& storage, char* ptr, size_t size) {
        storage.reserve(storage.size() + size);
        std::copy(ptr, ptr + size, std::back_inserter(storage));
    }

    crc32_t pack(buffer_t& storage, char* input, size_t data_size) {
        auto last_crc32_ = absl::ComputeCrc32c({input, data_size});
        append_size(storage, size_tt(data_size));
        append_payload(storage, input, data_size);
        append_crc32(storage, static_cast<uint32_t>(last_crc32_));
        return static_cast<uint32_t>(last_crc32_);
    }

    crc32_t pack_commit_marker(buffer_t& storage, crc32_t last_crc32, id_t id, uint64_t transaction_id) {
        msgpack::sbuffer sbuf;
        msgpack::packer<msgpack::sbuffer> pk(&sbuf);
        pk.pack_array(3);
        pk.pack(static_cast<uint64_t>(last_crc32));
        pk.pack(static_cast<uint64_t>(id));
        pk.pack(transaction_id);
        return pack(storage, sbuf.data(), sbuf.size());
    }

    id_t unpack_wal_id(buffer_t& storage) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        return o.via.array.ptr[1].as<id_t>();
    }

    crc32_t pack_physical_insert(buffer_t& storage,
                                 std::pmr::memory_resource* resource,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const components::vector::data_chunk_t& data_chunk,
                                 uint64_t row_start,
                                 uint64_t row_count) {
        components::serializer::msgpack_serializer_t serializer(resource);
        serializer.start_array(9);
        serializer.append(static_cast<uint64_t>(last_crc32));
        serializer.append(static_cast<uint64_t>(id));
        serializer.append(txn_id);
        serializer.append(static_cast<uint64_t>(wal_record_type::PHYSICAL_INSERT));
        serializer.append(database);
        serializer.append(collection);
        data_chunk.serialize(&serializer);
        serializer.append(row_start);
        serializer.append(row_count);
        serializer.end_array();
        auto buffer = serializer.result();
        return pack(storage, buffer.data(), buffer.size());
    }

    crc32_t pack_physical_delete(buffer_t& storage,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const std::pmr::vector<int64_t>& row_ids,
                                 uint64_t count) {
        msgpack::sbuffer sbuf;
        msgpack::packer<msgpack::sbuffer> pk(&sbuf);
        pk.pack_array(8);
        pk.pack(static_cast<uint64_t>(last_crc32));
        pk.pack(static_cast<uint64_t>(id));
        pk.pack(txn_id);
        pk.pack(static_cast<uint64_t>(wal_record_type::PHYSICAL_DELETE));
        pk.pack(database);
        pk.pack(collection);
        pk.pack_array(static_cast<uint32_t>(row_ids.size()));
        for (auto rid : row_ids) {
            pk.pack(rid);
        }
        pk.pack(count);
        return pack(storage, sbuf.data(), sbuf.size());
    }

    crc32_t pack_physical_update(buffer_t& storage,
                                 std::pmr::memory_resource* resource,
                                 crc32_t last_crc32,
                                 id_t id,
                                 uint64_t txn_id,
                                 const std::string& database,
                                 const std::string& collection,
                                 const std::pmr::vector<int64_t>& row_ids,
                                 const components::vector::data_chunk_t& new_data,
                                 uint64_t count) {
        components::serializer::msgpack_serializer_t serializer(resource);
        serializer.start_array(9);
        serializer.append(static_cast<uint64_t>(last_crc32));
        serializer.append(static_cast<uint64_t>(id));
        serializer.append(txn_id);
        serializer.append(static_cast<uint64_t>(wal_record_type::PHYSICAL_UPDATE));
        serializer.append(database);
        serializer.append(collection);
        serializer.start_array(row_ids.size());
        for (auto rid : row_ids) {
            serializer.append(static_cast<int64_t>(rid));
        }
        serializer.end_array();
        new_data.serialize(&serializer);
        serializer.append(count);
        serializer.end_array();
        auto buffer = serializer.result();
        return pack(storage, buffer.data(), buffer.size());
    }

} //namespace services::wal
