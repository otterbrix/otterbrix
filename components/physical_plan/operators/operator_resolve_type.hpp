#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // operator_resolve_type_t.
    //
    // Leaf operator that resolves EVERY entry on a kind()==type resolve node. Per
    // entry it turns the entry's dbname into a namespace_oid (well-known constants
    // for "public" / "pg_catalog", otherwise a pg_namespace scan), scans pg_type by
    // (typname, typnamespace), and stamps type_oid + type_md (the decoded
    // complex_logical_type) back into that entry.
    //
    // Output: a 0-row chunk — this is a SINK whose product is the entry stamps,
    // which validate / enrich / resolve_type.cpp read straight off the node.
    //
    // Scope:
    //   - Only walks pg_type by typname+typnamespace. Composite-type fallback
    //     (relkind='c' rows in pg_class + per-field pg_attribute rows) is
    //     intentionally out-of-scope.
    //   - Uses manager_disk_t::read_chunks_by_key (pure storage primitive).
    //     No dispatcher state.
    class operator_resolve_type_t final : public read_write_operator_t {
    public:
        operator_resolve_type_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::logical_plan::node_catalog_resolve_t* node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor's resolve pass drives await_async_and_resume via the
        // bottom-up needs_async_finalize walk; push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::logical_plan::node_catalog_resolve_t* node_;
        // Static output schema, built once in the constructor.
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
