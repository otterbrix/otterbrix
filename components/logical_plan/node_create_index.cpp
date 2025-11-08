#include "node_create_index.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_index_t::node_create_index_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name,
                                             index_type type)
        : node_t(resource, node_type::create_index_t, collection)
        , name_(name)
        , index_type_(type) {}

    const std::string& node_create_index_t::name() const noexcept { return name_; }

    index_type node_create_index_t::type() const noexcept { return index_type_; }

    keys_base_storage_t& node_create_index_t::keys() noexcept { return keys_; }

    node_ptr node_create_index_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<index_type>(1);
        auto collection = deserializer->deserialize_collection(2);
        auto name = deserializer->deserialize_string(3);
        auto keys = deserializer->deserialize_keys(4);
        auto res = make_node_create_index(deserializer->resource(), collection, name, type);
        res->keys() = keys;
        return res;
    }

    hash_t node_create_index_t::hash_impl() const { return 0; }

    inline std::string name_index_type(index_type type) {
        switch (type) {
            case index_type::single:
                return "single";
            case index_type::composite:
                return "composite";
            case index_type::multikey:
                return "multikey";
            case index_type::hashed:
                return "hashed";
            case index_type::wildcard:
                return "wildcard";
            case index_type::no_valid:
                return "no_valid";
        }
        return "default";
    }

    std::string node_create_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_index: " << database_name() << "." << collection_name() << " name:" << name() << "[ ";
        for (const auto& key : keys_) {
            stream << key.as_string() << ' ';
        }
        stream << "] type:" << name_index_type(index_type_);
        return stream.str();
    }

    void node_create_index_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(5);
        serializer->append_enum(serializer::serialization_type::logical_node_create_index);
        serializer->append_enum(index_type_);
        serializer->append(collection_);
        serializer->append(name_);
        serializer->append(keys_);
        serializer->end_array();
    }

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& collection,
                                                 const std::string& name,
                                                 index_type type) {
        return {new node_create_index_t{resource, collection, name, type}};
    }

    node_create_index_ptr to_node_create_index(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 5) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto name = msg_object.via.array.ptr[2].as<std::string>();
        auto type = static_cast<components::logical_plan::index_type>(msg_object.via.array.ptr[3].as<uint8_t>());
        auto data = msg_object.via.array.ptr[4].as<std::vector<std::string>>();
        auto keys = components::logical_plan::keys_base_storage_t(data.begin(), data.end());
        auto node = make_node_create_index(resource, {database, collection}, name, type);
        node->keys() = keys;
        return node;
    }

} // namespace components::logical_plan
