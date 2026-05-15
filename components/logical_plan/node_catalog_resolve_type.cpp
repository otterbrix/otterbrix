#include "node_catalog_resolve_type.hpp"

#include <sstream>

namespace components::logical_plan {

    node_catalog_resolve_type_t::node_catalog_resolve_type_t(std::pmr::memory_resource* resource,
                                                             core::dbname_t dbname,
                                                             core::typename_t type_name)
        : node_t(resource, node_type::catalog_resolve_type_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , type_name_(std::move(static_cast<std::string&>(type_name))) {}

    hash_t node_catalog_resolve_type_t::hash_impl() const { return 0; }

    std::string node_catalog_resolve_type_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$catalog_resolve_type: dbname: " << dbname_ << ", type_name: " << type_name_
               << ", type_oid: " << type_oid_;
        return stream.str();
    }

    node_catalog_resolve_type_ptr make_node_catalog_resolve_type(std::pmr::memory_resource* resource,
                                                                 core::dbname_t dbname,
                                                                 core::typename_t type_name) {
        return {new node_catalog_resolve_type_t{resource, std::move(dbname), std::move(type_name)}};
    }

} // namespace components::logical_plan
