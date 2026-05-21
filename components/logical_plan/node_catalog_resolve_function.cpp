#include "node_catalog_resolve_function.hpp"

#include <sstream>
#include <utility>

namespace components::logical_plan {

    node_catalog_resolve_function_t::node_catalog_resolve_function_t(std::pmr::memory_resource* resource,
                                                                     core::dbname_t dbname,
                                                                     core::function_name_t function_name)
        : node_t(resource, node_type::catalog_resolve_function_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , function_name_(std::move(static_cast<std::string&>(function_name))) {}

    hash_t node_catalog_resolve_function_t::hash_impl() const { return 0; }

    std::string node_catalog_resolve_function_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$catalog_resolve_function: " << dbname_ << "." << function_name_;
        if (function_oid_ != components::catalog::INVALID_OID) {
            stream << " -> oid=" << function_oid_;
        }
        return stream.str();
    }

    node_catalog_resolve_function_ptr make_node_catalog_resolve_function(std::pmr::memory_resource* resource,
                                                                         core::dbname_t dbname,
                                                                         core::function_name_t function_name) {
        return {new node_catalog_resolve_function_t{resource, std::move(dbname), std::move(function_name)}};
    }

} // namespace components::logical_plan
