#include "node_catalog_resolve_database.hpp"

#include <sstream>
#include <utility>

namespace components::logical_plan {

    node_catalog_resolve_database_t::node_catalog_resolve_database_t(std::pmr::memory_resource* resource,
                                                                     core::dbname_t dbname)
        : node_t(resource, node_type::catalog_resolve_database_t)
        , dbname_(std::move(static_cast<std::string&>(dbname))) {}

    hash_t node_catalog_resolve_database_t::hash_impl() const { return 0; }

    std::string node_catalog_resolve_database_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$catalog_resolve_database: " << dbname_ << " (oid=" << database_oid_ << ")";
        return stream.str();
    }

    node_catalog_resolve_database_ptr make_node_catalog_resolve_database(std::pmr::memory_resource* resource,
                                                                         core::dbname_t dbname) {
        return {new node_catalog_resolve_database_t{resource, std::move(dbname)}};
    }

} // namespace components::logical_plan
