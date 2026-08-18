#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator that resolves EVERY entry on a kind()==namespace_ resolve
    // node: one pg_namespace scan by nspname per entry, stamping the resolved
    // namespace_oid back into that entry. All namespaces a statement depends on
    // are resolved in this single operator run.
    //
    // The storage scan is performed by manager_disk_t::read_chunks_by_key (a pure
    // storage primitive), so this composes cleanly into the standard pipeline.
    //
    // Output: a 0-row chunk. The operator is a SINK — its product is the entry
    // stamps, which validate / enrich / the executor read straight off the node.
    // Nothing consumes the chunk as pipeline data; it exists only so downstream
    // code sees a well-formed output.
    class operator_resolve_namespace_t final : public read_write_operator_t {
    public:
        operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::logical_plan::node_catalog_resolve_t* node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor's resolve pass drives await_async_and_resume via the
        // bottom-up needs_async_finalize walk; push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::node_catalog_resolve_t* node_;
        // Static output schema, built once in the constructor.
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
