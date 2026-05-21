#include "node_create_database.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_database_t::node_create_database_t(std::pmr::memory_resource* resource, core::dbname_t dbname)
        : node_t(resource, node_type::create_database_t)
        , dbname_(std::move(static_cast<std::string&>(dbname))) {}

    hash_t node_create_database_t::hash_impl() const { return 0; }

    std::string node_create_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_database: " << dbname_;
        return stream.str();
    }

    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource, core::dbname_t dbname) {
        return {new node_create_database_t{resource, std::move(dbname)}};
    }

} // namespace components::logical_plan
