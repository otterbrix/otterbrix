#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator that resolves EVERY entry on a kind()==database resolve node:
    // one pg_database (OID=19, distinct from pg_namespace) scan by datname per
    // entry, stamping the resolved database_oid back into that entry. The executor
    // reads those stamps to populate execution_context_t.database_oid without a
    // second async message.
    //
    // Output: a 0-row chunk — this is a SINK whose product is the entry stamps.
    class operator_resolve_database_t final : public read_write_operator_t {
    public:
        operator_resolve_database_t(std::pmr::memory_resource* resource,
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
