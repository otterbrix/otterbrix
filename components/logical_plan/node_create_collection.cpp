#include "node_create_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       bool disk_storage)
        : node_t(resource, node_type::create_collection_t, collection)
        , disk_storage_(disk_storage) {}

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       std::pmr::vector<types::complex_logical_type> schema,
                                                       bool disk_storage)
        : node_t(resource, node_type::create_collection_t, collection)
        , disk_storage_(disk_storage) {
        column_definitions_.reserve(schema.size());
        for (auto& type : schema) {
            std::string name = type.alias();
            column_definitions_.emplace_back(std::move(name), std::move(type));
        }
    }

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       std::vector<table::column_definition_t> column_definitions,
                                                       std::vector<table::table_constraint_t> constraints,
                                                       bool disk_storage)
        : node_t(resource, node_type::create_collection_t, collection)
        , column_definitions_(std::move(column_definitions))
        , constraints_(std::move(constraints))
        , disk_storage_(disk_storage) {}

    std::pmr::vector<types::complex_logical_type> node_create_collection_t::schema() const {
        std::pmr::vector<types::complex_logical_type> result(resource());
        result.reserve(column_definitions_.size());
        for (const auto& col : column_definitions_) {
            result.push_back(col.type());
        }
        return result;
    }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() {
        return column_definitions_;
    }

    const std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() const {
        return column_definitions_;
    }

    const std::vector<table::table_constraint_t>& node_create_collection_t::constraints() const { return constraints_; }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection) {
        return {new node_create_collection_t{resource, collection}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema) {
        return {new node_create_collection_t{resource, collection, std::move(schema)}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage) {
        return {new node_create_collection_t{resource,
                                             collection,
                                             std::move(column_definitions),
                                             std::move(constraints),
                                             disk_storage}};
    }

} // namespace components::logical_plan
