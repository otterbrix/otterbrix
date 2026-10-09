#include "node_create_database.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_database_t::node_create_database_t(std::pmr::memory_resource* resource,
                                                   core::dbname_t dbname,
                                                   bool if_not_exists)
        : node_t(resource, node_type::create_database_t, qualified_name_t{std::move(dbname), core::relname_t{}})
        , if_not_exists_(if_not_exists) {}

    hash_t node_create_database_t::hash_impl() const { return 0; }

    std::string node_create_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_database" << (if_not_exists_ ? "_if_not_exists" : "") << ": " << target_.database;
        return stream.str();
    }

    node_create_database_ptr
    make_node_create_database(std::pmr::memory_resource* resource, core::dbname_t dbname, bool if_not_exists) {
        return {new node_create_database_t{resource, std::move(dbname), if_not_exists}};
    }

} // namespace components::logical_plan
