#include "dto.hpp"

#include <components/serialization/deserializer.hpp>
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

    crc32_t read_crc32(buffer_t& input, size_tt index_start) {
        crc32_t crc32_tmp = 0;
        crc32_tmp = 0xff000000 & (uint32_t(input[index_start]) << 24);
        crc32_tmp |= 0x00ff0000 & (uint32_t(input[index_start + 1]) << 16);
        crc32_tmp |= 0x0000ff00 & (uint32_t(input[index_start + 2]) << 8);
        crc32_tmp |= 0x000000ff & (uint32_t(input[index_start + 3]));
        return crc32_tmp;
    }

    buffer_t read_payload(buffer_t& input, size_tt index_start, size_tt index_stop) {
        buffer_t buffer(input.begin() + static_cast<std::ptrdiff_t>(index_start),
                        input.begin() + static_cast<std::ptrdiff_t>(index_stop));
        return buffer;
    }


    size_tt read_size_impl(buffer_t& input, size_tt index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff000000 & (size_tt(uint8_t(input[index_start])) << 24);
        size_tmp |= 0x00ff0000 & (size_tt(uint8_t(input[index_start + 1])) << 16);
        size_tmp |= 0x0000ff00 & (size_tt(uint8_t(input[index_start + 2])) << 8);
        size_tmp |= 0x000000ff & (size_tt(uint8_t(input[index_start + 3])));
        return size_tmp;
    }

    crc32_t pack(buffer_t& storage, char* input, size_t data_size) {
        auto last_crc32_ = absl::ComputeCrc32c({input, data_size});
        append_size(storage, size_tt(data_size));
        append_payload(storage, input, data_size);
        append_crc32(storage, static_cast<uint32_t>(last_crc32_));
        return static_cast<uint32_t>(last_crc32_);
    }

    crc32_t pack(buffer_t& storage,
                 crc32_t last_crc32,
                 id_t id,
                 const components::logical_plan::node_ptr& data,
                 const components::logical_plan::parameter_node_ptr& params) {
        components::serializer::msgpack_serializer_t serializer(data->resource());
        serializer.start_array(4);
        serializer.append(static_cast<uint64_t>(last_crc32));
        serializer.append(static_cast<uint64_t>(id));
        data->serialize(&serializer);
        params->serialize(&serializer);
        serializer.end_array();
        auto buffer = serializer.result();

        return pack(storage, buffer.data(), buffer.size());
    }

    void unpack(buffer_t& storage, wal_entry_t& entry) {
        components::serializer::msgpack_deserializer_t deserializer(storage);

        entry.last_crc32_ = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
        entry.id_ = deserializer.deserialize_uint64(1);

        deserializer.advance_array(2);
        entry.entry_ = components::logical_plan::node_t::deserialize(&deserializer);
        deserializer.pop_array();
        deserializer.advance_array(3);
        entry.params_ = components::logical_plan::parameter_node_t::deserialize(&deserializer);
        deserializer.pop_array();
    }

    id_t unpack_wal_id(buffer_t& storage) {
        msgpack::unpacked msg;
        msgpack::unpack(msg, storage.data(), storage.size());
        const auto& o = msg.get();
        return o.via.array.ptr[1].as<id_t>();
    }

} //namespace services::wal
