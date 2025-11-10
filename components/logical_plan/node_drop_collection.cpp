#include "node_drop_collection.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_drop_collection_t::node_drop_collection_t(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& collection)
        : node_t(resource, node_type::drop_collection_t, collection) {}

    node_ptr node_drop_collection_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return make_node_drop_collection(deserializer->resource(), deserializer->deserialize_collection(1));
    }

    hash_t node_drop_collection_t::hash_impl() const { return 0; }

    std::string node_drop_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_drop_collection_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_drop_collection);
        serializer->append(collection_);
        serializer->end_array();
    }

    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection) {
        return {new node_drop_collection_t{resource, collection}};
    }

} // namespace components::logical_plan
