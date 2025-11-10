#include "node_drop_index.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_drop_index_t::node_drop_index_t(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const std::string& name)
        : node_t(resource, node_type::drop_index_t, collection)
        , name_(name) {}

    const std::string& node_drop_index_t::name() const noexcept { return name_; }

    node_ptr node_drop_index_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);
        auto name = deserializer->deserialize_string(2);
        return make_node_drop_index(deserializer->resource(), collection, name);
    }

    hash_t node_drop_index_t::hash_impl() const { return 0; }

    std::string node_drop_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_index: " << database_name() << "." << collection_name() << " name:" << name();
        return stream.str();
    }

    void node_drop_index_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_drop_index);
        serializer->append(collection_);
        serializer->append(name_);
        serializer->end_array();
    }

    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name) {
        return {new node_drop_index_t{resource, collection, name}};
    }

} // namespace components::logical_plan
