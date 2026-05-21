#include "node_catalog_resolve_table.hpp"

#include <sstream>
#include <utility>

namespace components::logical_plan {

    node_catalog_resolve_table_t::node_catalog_resolve_table_t(std::pmr::memory_resource* resource,
                                                               core::dbname_t dbname,
                                                               core::relname_t relname)
        : node_t(resource, node_type::catalog_resolve_table_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname))) {}

    hash_t node_catalog_resolve_table_t::hash_impl() const { return 0; }

    std::string node_catalog_resolve_table_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$catalog_resolve_table: " << dbname_ << "." << relname_;
        if (namespace_oid_ != components::catalog::INVALID_OID) {
            stream << " (ns_oid=" << namespace_oid_ << ")";
        }
        return stream.str();
    }

    node_catalog_resolve_table_ptr make_node_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                                   core::dbname_t dbname,
                                                                   core::relname_t relname) {
        return {new node_catalog_resolve_table_t{resource, std::move(dbname), std::move(relname)}};
    }

} // namespace components::logical_plan
