#include "node_create_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       std::string dbname,
                                                       std::string relname,
                                                       std::string schemaname,
                                                       std::string uuid,
                                                       bool disk_storage)
        : node_t(resource, node_type::create_collection_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , schemaname_(std::move(schemaname))
        , uuid_(std::move(uuid))
        , disk_storage_(disk_storage) {}

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       std::string dbname,
                                                       std::string relname,
                                                       std::vector<table::column_definition_t> column_definitions,
                                                       std::vector<table::table_constraint_t> constraints,
                                                       std::string schemaname,
                                                       std::string uuid,
                                                       bool disk_storage)
        : node_t(resource, node_type::create_collection_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , schemaname_(std::move(schemaname))
        , uuid_(std::move(uuid))
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
        stream << "$create_collection: " << dbname_ << "." << relname_;
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
                                                           std::string dbname,
                                                           std::string relname,
                                                           std::string schemaname,
                                                           std::string uuid) {
        return {new node_create_collection_t{resource,
                                             std::move(dbname),
                                             std::move(relname),
                                             std::move(schemaname),
                                             std::move(uuid)}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage,
                                                           std::string schemaname,
                                                           std::string uuid) {
        return {new node_create_collection_t{resource,
                                             std::move(dbname),
                                             std::move(relname),
                                             std::move(column_definitions),
                                             std::move(constraints),
                                             std::move(schemaname),
                                             std::move(uuid),
                                             disk_storage}};
    }

} // namespace components::logical_plan
