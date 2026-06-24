#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator that scans pg_namespace by nspname and emits
    // the resolved namespace_oid as a single-row data_chunk.
    //
    // Output chunk schema:
    //   col 0: UINTEGER  — namespace_oid (oid_t / uint32_t). One row when
    //                       the namespace exists; zero rows when it doesn't.
    //
    // The actual storage scan is performed by manager_disk_t::read_rows_by_key
    // (pure storage primitive), so this operator composes cleanly into
    // pipelines that resolve names through the catalog pipeline.
    //
    // When constructed with a back-pointer to its logical-plan node, the
    // operator stamps the resolved namespace_oid onto that node so the
    // dispatcher's Pass-2 (validate / enrich) can read it via
    // plan_resolve_index_t without re-issuing an async actor message.
    class operator_resolve_namespace_t final : public read_write_operator_t {
    public:
        operator_resolve_namespace_t(std::pmr::memory_resource* resource, log_t log, std::string name);

        // Back-pointer form. The operator stamps the resolved namespace_oid
        // onto `target_node` after a successful pg_namespace scan. The node
        // is owned by the dispatcher's logical plan tree and outlives this
        // operator (operators live only for the duration of execute_plan).
        operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     std::string name,
                                     components::logical_plan::node_catalog_resolve_t* target_node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor's run_resolve_subplan lowers the resolve front-pass to an
        // all-sink chain and drives this operator's await_async_and_resume via the
        // bottom-up needs_async_finalize pass (the single-resolve case is a sourceless
        // sink-root, a multi-resolve case a sourceless all-sink chain). The async scan
        // emits a single-row chunk into output_ and stamps the resolved oid onto
        // target_node_; push()/finalize() inherit the no-op defaults (no consumer
        // reads the chunk as pipeline data — the metadata handoff is the node stamp,
        // read later via plan_resolve_index).
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
        components::logical_plan::node_catalog_resolve_t* target_node_{nullptr};
        // Static output chunk schema, built once in the constructor (TASK C10).
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
