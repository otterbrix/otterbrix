#include "node_create_collection.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       std::pmr::vector<types::complex_logical_type> schema)
        : node_t(resource, node_type::create_collection_t, collection)
        , schema_(std::move(schema)) {}

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       std::pmr::vector<types::complex_logical_type> schema,
                                                       std::vector<table::column_definition_t> column_definitions,
                                                       std::vector<table::table_constraint_t> constraints)
        : node_t(resource, node_type::create_collection_t, collection)
        , schema_(std::move(schema))
        , column_definitions_(std::move(column_definitions))
        , constraints_(std::move(constraints)) {}

    node_create_collection_ptr node_create_collection_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return make_node_create_collection(deserializer->resource(), deserializer->deserialize_collection(1));
    }

    std::pmr::vector<types::complex_logical_type>& node_create_collection_t::schema() { return schema_; }

    const std::pmr::vector<types::complex_logical_type>& node_create_collection_t::schema() const { return schema_; }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_create_collection_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_create_collection);
        serializer->append(collection_);
        serializer->end_array();
    }

    const std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() const {
        return column_definitions_;
    }

    const std::vector<table::table_constraint_t>& node_create_collection_t::constraints() const {
        return constraints_;
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema) {
        return {new node_create_collection_t{resource, collection, std::move(schema)}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints) {
        return {new node_create_collection_t{resource, collection, std::move(schema),
                                             std::move(column_definitions), std::move(constraints)}};
    }

} // namespace components::logical_plan
