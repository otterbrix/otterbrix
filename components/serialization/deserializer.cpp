#include "deserializer.hpp"

#include "logical_plan/node_limit.hpp"

namespace components::serializer {

    msgpack_deserializer_t::msgpack_deserializer_t(const std::pmr::string& input)
        : input_(input) {
        msg = msgpack::unpack(input_.data(), input_.size());
        root_arr_ = msg.get().via.array;
        working_tree_.emplace(&root_arr_);
    }

    std::pmr::vector<expressions::key_t> msgpack_deserializer_t::deserialize_keys(size_t index) {
        std::pmr::vector<expressions::key_t> res(resource());
        advance_array(index);
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_key(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<document_ptr> msgpack_deserializer_t::deserialize_documents(size_t index) {
        advance_array(index);
        std::pmr::vector<document_ptr> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_document(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<core::parameter_id_t> msgpack_deserializer_t::deserialize_param_ids(size_t index) {
        advance_array(index);
        std::pmr::vector<core::parameter_id_t> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_param_id(i));
        }
        pop_array();
        return res;
    }

    std::pair<core::parameter_id_t, document::value_t>
    msgpack_deserializer_t::deserialize_param_pair(document::impl::base_document* tape, size_t index) {
        advance_array(index);
        std::pair res = {deserialize_param_id(0), deserialize_value(tape, 1)};
        pop_array();
        return res;
    }

    size_t msgpack_deserializer_t::root_array_size() const { return root_arr_.size; }

    size_t msgpack_deserializer_t::current_array_size() const { return working_tree_.top()->size; }

    void msgpack_deserializer_t::advance_array(size_t index) {
        working_tree_.emplace(&working_tree_.top()->ptr[index].via.array);
    }

    void msgpack_deserializer_t::pop_array() { working_tree_.pop(); }

    serialization_type msgpack_deserializer_t::current_type() { return deserialize_enum<serialization_type>(0); }

    bool msgpack_deserializer_t::deserialize_bool(size_t index) { return working_tree_.top()->ptr[index].via.boolean; }

    int64_t msgpack_deserializer_t::deserialize_int64(size_t index) { return working_tree_.top()->ptr[index].via.i64; }

    uint64_t msgpack_deserializer_t::deserialize_uint64(size_t index) {
        return working_tree_.top()->ptr[index].via.u64;
    }

    double msgpack_deserializer_t::deserialize_double(size_t index) { return working_tree_.top()->ptr[index].via.f64; }

    absl::int128 msgpack_deserializer_t::deserialize_int128(size_t index) {
        advance_array(index);
        auto high = working_tree_.top()->ptr[0].via.i64;
        auto low = working_tree_.top()->ptr[1].via.u64;
        auto res = absl::MakeInt128(high, low);
        pop_array();
        return res;
    }

    absl::uint128 msgpack_deserializer_t::deserialize_uint128(size_t index) {
        advance_array(index);
        auto high = working_tree_.top()->ptr[0].via.u64;
        auto low = working_tree_.top()->ptr[1].via.u64;
        auto res = absl::MakeUint128(high, low);
        pop_array();
        return res;
    }

    core::parameter_id_t msgpack_deserializer_t::deserialize_param_id(size_t index) {
        return static_cast<core::parameter_id_t>(working_tree_.top()->ptr[index].via.u64);
    }

    expressions::key_t msgpack_deserializer_t::deserialize_key(size_t index) {
        if (working_tree_.top()->ptr[index].type == msgpack::type::POSITIVE_INTEGER) {
            return expressions::key_t{static_cast<int32_t>(working_tree_.top()->ptr[index].via.i64)};
        } else if (working_tree_.top()->ptr[index].type == msgpack::type::NEGATIVE_INTEGER) {
            return expressions::key_t{static_cast<uint32_t>(working_tree_.top()->ptr[index].via.u64)};
        } else if (working_tree_.top()->ptr[index].type == msgpack::type::STR) {
            return expressions::key_t{working_tree_.top()->ptr[index].via.str.ptr,
                                      working_tree_.top()->ptr[index].via.str.size};
        }
        return {};
    }

    std::string msgpack_deserializer_t::deserialize_string(size_t index) {
        return {working_tree_.top()->ptr[index].via.str.ptr, working_tree_.top()->ptr[index].via.str.size};
    }

    document::value_t msgpack_deserializer_t::deserialize_value(document::impl::base_document* tape, size_t index) {
        auto obj = working_tree_.top()->ptr[index];
        switch (obj.type) {
            case msgpack::type::NIL:
                return document::value_t(tape, nullptr);
            case msgpack::type::BOOLEAN:
                return document::value_t(tape, obj.via.boolean);
            case msgpack::type::POSITIVE_INTEGER:
                return document::value_t(tape, obj.via.u64);
            case msgpack::type::NEGATIVE_INTEGER:
                return document::value_t(tape, obj.via.i64);
            case msgpack::type::FLOAT32:
            case msgpack::type::FLOAT64:
                return document::value_t(tape, obj.via.f64);
            case msgpack::type::STR:
                return document::value_t(tape, std::string_view(obj.via.str.ptr, obj.via.str.size));
            default:
                return document::value_t();
        }
    }

    document_ptr msgpack_deserializer_t::deserialize_document(size_t index) {
        return document::msgpack_decoder_t::to_document(working_tree_.top()->ptr[index], resource());
    }

    collection_full_name_t msgpack_deserializer_t::deserialize_collection(size_t index) {
        return {{working_tree_.top()->ptr[index].via.array.ptr[0].via.str.ptr,
                 working_tree_.top()->ptr[index].via.array.ptr[0].via.str.size},
                {working_tree_.top()->ptr[index].via.array.ptr[1].via.str.ptr,
                 working_tree_.top()->ptr[index].via.array.ptr[1].via.str.size}};
    }

} // namespace components::serializer