#include "node_create_database.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_database_t::node_create_database_t(std::pmr::memory_resource* resource,
                                                   core::dbname_t dbname,
                                                   bool if_not_exists)
        : node_t(resource, node_type::create_database_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , if_not_exists_(if_not_exists) {}

    hash_t node_create_database_t::hash_impl() const { return 0; }

    std::string node_create_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_database" << (if_not_exists_ ? "_if_not_exists" : "") << ": " << dbname_;
        return stream.str();
    }

    node_create_database_ptr
    make_node_create_database(std::pmr::memory_resource* resource, core::dbname_t dbname, bool if_not_exists) {
        collection_full_name_t collection;
        collection.database = static_cast<const std::string&>(dbname);
        auto node = node_create_database_ptr{new node_create_database_t{resource, std::move(dbname), if_not_exists}};
        node->set_collection_full_name(std::move(collection));
        return node;
    }

    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       bool if_not_exists) {
        auto node = make_node_create_database(resource, core::dbname_t{collection.database}, if_not_exists);
        node->set_collection_full_name(collection);
        return node;
    }

} // namespace components::logical_plan
