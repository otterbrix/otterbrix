#include "serializer.hpp"

#include <components/expressions/key.hpp>

namespace components::serializer {

    msgpack_serializer_t::msgpack_serializer_t(std::pmr::memory_resource* resource)
        : result_(std::pmr::string(resource))
        , packer_(result_) {}

    std::pmr::string msgpack_serializer_t::result() const { return result_.str(); }

    void msgpack_serializer_t::append(const std::pmr::vector<expressions::key_t>& keys) {
        start_array(keys.size());
        for (const auto& k : keys) {
            append(k);
        }
        end_array();
    }

    void msgpack_serializer_t::append(const std::pmr::vector<core::parameter_id_t>& params) {
        start_array(params.size());
        for (const auto& id : params) {
            append(id);
        }
        end_array();
    }

    void msgpack_serializer_t::append(const collection_full_name_t& collection) {
        start_array(2);
        append(collection.database);
        append(collection.collection);
        end_array();
    }

    void msgpack_serializer_t::start_array(size_t size) { packer_.pack_array(size); }

    void msgpack_serializer_t::end_array() {
        // nothing to do here
    }

    void msgpack_serializer_t::append_null() { packer_.pack_nil(); }

    void msgpack_serializer_t::append(bool val) { packer_.pack(val); }

    void msgpack_serializer_t::append(int64_t val) { packer_.pack(val); }

    void msgpack_serializer_t::append(uint64_t val) { packer_.pack(val); }

    void msgpack_serializer_t::append(double val) { packer_.pack(val); }

    void msgpack_serializer_t::append(const absl::int128& val) {
        start_array(2);
        packer_.pack(absl::Int128High64(val));
        packer_.pack(absl::Int128Low64(val));
        end_array();
    }
    void msgpack_serializer_t::append(const absl::uint128& val) {
        start_array(2);
        packer_.pack(absl::Uint128High64(val));
        packer_.pack(absl::Uint128Low64(val));
        end_array();
    }

    void msgpack_serializer_t::append(core::parameter_id_t val) { packer_.pack(val.t); }

    void msgpack_serializer_t::append(const std::string& str) { packer_.pack(str); }

    void msgpack_serializer_t::append(const expressions::key_t& key_val) {
        start_array(2);
        packer_.pack(key_val.as_string());
        append_enum(key_val.side());
        end_array();
    }

} // namespace components::serializer