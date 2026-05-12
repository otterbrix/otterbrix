#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    // CATALOG_RESOLVE_FUNCTION — leaf node that names a function to be resolved
    // in a given namespace (database). The operator lookup-walks pg_proc via the
    // catalog pipeline and stamps the resulting pg_proc.oid back onto the node
    // via set_function_oid(). Phase 13 T8: gives the planner a uniform way to
    // late-bind function references without inlining catalog_view_t reads.
    class node_catalog_resolve_function_t final : public node_t {
    public:
        explicit node_catalog_resolve_function_t(std::pmr::memory_resource* resource,
                                                 std::string dbname,
                                                 std::string function_name);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& function_name() const noexcept { return function_name_; }
        components::catalog::oid_t function_oid() const noexcept { return function_oid_; }
        void set_function_oid(components::catalog::oid_t oid) noexcept { function_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string function_name_;
        components::catalog::oid_t function_oid_{components::catalog::INVALID_OID};
    };

    using node_catalog_resolve_function_ptr = boost::intrusive_ptr<node_catalog_resolve_function_t>;

    node_catalog_resolve_function_ptr
    make_node_catalog_resolve_function(std::pmr::memory_resource* resource,
                                       std::string dbname,
                                       std::string function_name);

} // namespace components::logical_plan
