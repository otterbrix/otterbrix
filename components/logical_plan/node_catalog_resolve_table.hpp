#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/identifier_types.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace components::logical_plan {

    // Per-column metadata mirrored from pg_attribute
    // (relkind='r') or pg_computed_column (relkind='g'), reconstructed at
    // Pass 1 time by operator_resolve_table_t. Carries the full surface
    // enrich_plan / validate_schema read via the plan-tree idx.
    struct resolved_column_metadata_t {
        std::string attname;
        types::complex_logical_type type;
        std::int32_t attnum{0};
        // Storage chunk column index — position in storage_t::scan_batched output.
        // For relkind='r' this is attnum-1. For relkind='g' it can differ because
        // storage retains tombstoned columns between VACUUMs. -1 = unknown
        // (plan-gen falls back to pass-through).
        std::int32_t chunk_position{-1};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
        bool attnotnull{false};
        bool atthasdefault{false};
        std::string attdefspec; // serialized default expression
        std::string atttypspec; // serialized type spec
    };

    struct resolved_table_metadata_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        std::vector<resolved_column_metadata_t> columns;
        // pg_rewrite.ev_action body SQL, populated by operator_resolve_table_t for
        // relkind 'v' (regular view) and 'm' (matview — used by REFRESH). Empty
        // for other relkinds. Consumed by dispatcher Phase 1.5 rewrite_views.
        std::string view_sql;
    };

    // Catalog-dependency leaf node carrying "resolve table 'relname' in
    // namespace 'dbname' (or under ns_oid once enriched)". Built by the
    // transformer for catalog-touching statements. enrich_logical_plan resolves
    // dbname -> namespace_oid (pg_namespace.oid) so downstream operators can
    // read pg_class/pg_attribute through the pipeline.
    //
    // The node carries no children/expressions and emits no tuples; it is a
    // pure resolved-dependency marker. namespace_oid() == INVALID_OID prior to
    // enrichment, or when the namespace does not exist (caller decides whether
    // that is an error — see DROP IF EXISTS semantics in node_drop_index_t).
    class node_catalog_resolve_table_t final : public node_t {
    public:
        explicit node_catalog_resolve_table_t(std::pmr::memory_resource* resource,
                                              core::dbname_t dbname,
                                              core::relname_t relname);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& relname() const noexcept { return relname_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // Full table metadata reconstructed by operator_resolve_table_t at
        // Pass 1 time. Reset / unset state means the resolve operator did
        // not find the table (or hasn't run yet).
        const std::optional<resolved_table_metadata_t>& resolved_metadata() const noexcept {
            return resolved_metadata_;
        }
        void set_resolved_metadata(resolved_table_metadata_t md) { resolved_metadata_ = std::move(md); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        std::optional<resolved_table_metadata_t> resolved_metadata_;
    };

    using node_catalog_resolve_table_ptr = boost::intrusive_ptr<node_catalog_resolve_table_t>;

    node_catalog_resolve_table_ptr make_node_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                                   core::dbname_t dbname,
                                                                   core::relname_t relname);

} // namespace components::logical_plan