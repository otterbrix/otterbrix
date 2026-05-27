#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_type_t;
} // namespace components::logical_plan

namespace components::operators {

    // operator_resolve_type_t.
    //
    // Self-resolving leaf operator: scans pg_type by (typname, typnamespace) and
    // emits the structural metadata for the matched row. Used by enrichment
    // and the legacy manager_disk_t::resolve_type path.
    //
    // Output chunk layout (one row when found, zero rows otherwise):
    //   col 0: oid           — pg_type.oid                 (UINTEGER)
    //   col 1: typname       — pg_type.typname             (STRING_LITERAL)
    //   col 2: typnamespace  — pg_type.typnamespace        (UINTEGER)
    //   col 3: typdefspec    — encoded complex_logical_type
    //                          (STRING_LITERAL, empty for builtin scalars)
    //
    // The operator deliberately mirrors pg_type's persisted schema (see
    // components/catalog/system_table_schemas.cpp::pg_type_columns), so callers
    // can map columns by index without re-reading the schema definition.
    //
    // Scope:
    //   - Only walks pg_type by typname+typnamespace. Composite-type fallback
    //     (relkind='c' rows in pg_class + per-field pg_attribute rows) is
    //     intentionally out-of-scope; the legacy synchronous resolve_type_sync
    //     remains the entry point for that case until a separate operator
    //     covers composite reconstruction.
    //   - Uses manager_disk_t::read_rows_by_key (pure storage primitive).
    //     No dispatcher state.
    class operator_resolve_type_t final : public read_write_operator_t {
    public:
        // Legacy ctor: (ns_oid, name). Caller has already resolved namespace
        // via well_known_oid::* or other means.
        operator_resolve_type_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::catalog::oid_t namespace_oid,
                                std::string name);

        // back-pointer form. dbname is resolved internally (well_known
        // constants for "public" / "pg_catalog", otherwise scan
        // pg_namespace) and the operator stamps resolved_metadata + type_oid
        // onto the back-pointed logical node.
        operator_resolve_type_t(std::pmr::memory_resource* resource,
                                log_t log,
                                std::string dbname,
                                std::string name,
                                components::logical_plan::node_catalog_resolve_type_t* target_node);

        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t namespace_oid_;
        std::string dbname_;
        std::string name_;
        components::logical_plan::node_catalog_resolve_type_t* target_node_{nullptr};
    };

} // namespace components::operators
