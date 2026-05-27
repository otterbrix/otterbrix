#include "node_create_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       core::relname_t relname,
                                                       bool disk_storage,
                                                       bool if_not_exists)
        : node_t(resource, node_type::create_collection_t)
        , relname_(std::move(static_cast<std::string&>(relname)))
        , disk_storage_(disk_storage)
        , if_not_exists_(if_not_exists) {}

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       core::relname_t relname,
                                                       std::vector<table::column_definition_t> column_definitions,
                                                       std::vector<table::table_constraint_t> constraints,
                                                       bool disk_storage,
                                                       bool if_not_exists)
        : node_t(resource, node_type::create_collection_t)
        , relname_(std::move(static_cast<std::string&>(relname)))
        , column_definitions_(std::move(column_definitions))
        , constraints_(std::move(constraints))
        , disk_storage_(disk_storage)
        , if_not_exists_(if_not_exists) {}

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
        stream << "$create_collection: " << relname_;
        return stream.str();
    }

    std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() {
        return column_definitions_;
    }

    const std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() const {
        return column_definitions_;
    }

    const std::vector<table::table_constraint_t>& node_create_collection_t::constraints() const { return constraints_; }

    node_create_collection_ptr
    make_node_create_collection(std::pmr::memory_resource* resource, core::relname_t relname, bool if_not_exists) {
        return {new node_create_collection_t{resource, std::move(relname), false, if_not_exists}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           core::relname_t relname,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage,
                                                           bool if_not_exists) {
        return {new node_create_collection_t{resource,
                                             std::move(relname),
                                             std::move(column_definitions),
                                             std::move(constraints),
                                             disk_storage,
                                             if_not_exists}};
    }

} // namespace components::logical_plan
