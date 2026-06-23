#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_t;
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
                                components::logical_plan::node_catalog_resolve_t* target_node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // is_streaming_pipeline admits the resolve front-pass as an all-sink chain
        // and drives await_async_and_resume via the bottom-up needs_async_finalize
        // pass. The async pg_namespace + pg_type scan emits the resolved row into
        // output_ and stamps type_oid/resolved_type_metadata onto target_node_;
        // push()/finalize() inherit the no-op defaults (the metadata handoff is the
        // node stamp, read later via plan_resolve_index). Replaces the legacy
        // on_execute drive.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t namespace_oid_;
        std::string dbname_;
        std::string name_;
        components::logical_plan::node_catalog_resolve_t* target_node_{nullptr};
        // Static output chunk schema, built once in the constructor (TASK C10).
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
