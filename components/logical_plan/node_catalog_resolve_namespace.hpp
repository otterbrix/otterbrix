#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/identifier_types.hpp>

#include <string>

namespace components::logical_plan {

    // CATALOG_RESOLVE_NAMESPACE: logical-plan leaf carrying the
    // catalog dependency "resolve namespace 'X'". Produced when a plan needs
    // a namespace OID before it can be enriched/executed (e.g. a USE/SET
    // namespace, or a DDL that targets a database/schema by name only).
    //
    // The node stores the raw textual database name (PostgreSQL-style:
    // databases map to pg_namespace rows). enrich_logical_plan populates
    // namespace_oid_ via set_namespace_oid() before the planner consumes it.
    // Default value is INVALID_OID so that "not yet resolved" is observable.
    class node_catalog_resolve_namespace_t final : public node_t {
    public:
        explicit node_catalog_resolve_namespace_t(std::pmr::memory_resource* resource, core::dbname_t dbname);

        const std::string& dbname() const noexcept { return dbname_; }
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_catalog_resolve_namespace_ptr = boost::intrusive_ptr<node_catalog_resolve_namespace_t>;

    node_catalog_resolve_namespace_ptr make_node_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                                                           core::dbname_t dbname);

} // namespace components::logical_plan
