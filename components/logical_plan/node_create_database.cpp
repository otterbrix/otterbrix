#include "node_create_database.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_database_t::node_create_database_t(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& collection)
        : node_t(resource, node_type::create_database_t, collection) {}

    node_create_database_ptr node_create_database_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return make_node_create_database(deserializer->resource(), deserializer->deserialize_collection(1));
    }

    hash_t node_create_database_t::hash_impl() const { return 0; }

    std::string node_create_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_database: " << database_name();
        return stream.str();
    }

    void node_create_database_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_create_database);
        serializer->append(collection_);
        serializer->end_array();
    }

    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection) {
        return {new node_create_database_t{resource, collection}};
    }

} // namespace components::logical_plan
