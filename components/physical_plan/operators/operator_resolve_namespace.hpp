#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_namespace_t;
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
                                     components::logical_plan::node_catalog_resolve_namespace_t* target_node);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
        components::logical_plan::node_catalog_resolve_namespace_t* target_node_{nullptr};
    };

} // namespace components::operators
