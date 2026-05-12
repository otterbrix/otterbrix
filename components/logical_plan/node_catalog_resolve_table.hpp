#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    // Phase 13 T5: catalog-dependency leaf node carrying "resolve table 'relname'
    // in namespace 'dbname' (or under ns_oid once enriched)". Built by the
    // transformer for catalog-touching statements that previously inlined
    // catalog_view_t reads. enrich_logical_plan resolves dbname -> namespace_oid
    // (pg_namespace.oid) so downstream operators can read pg_class/pg_attribute
    // through the pipeline rather than via direct catalog access.
    //
    // The node carries no children/expressions and emits no tuples; it is a
    // pure resolved-dependency marker. namespace_oid() == INVALID_OID prior to
    // enrichment, or when the namespace does not exist (caller decides whether
    // that is an error — see DROP IF EXISTS semantics in node_drop_index_t).
    class node_catalog_resolve_table_t final : public node_t {
    public:
        explicit node_catalog_resolve_table_t(std::pmr::memory_resource* resource,
                                              std::string dbname,
                                              std::string relname);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& relname() const noexcept { return relname_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_catalog_resolve_table_ptr = boost::intrusive_ptr<node_catalog_resolve_table_t>;

    node_catalog_resolve_table_ptr make_node_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                                   std::string dbname,
                                                                   std::string relname);

} // namespace components::logical_plan
