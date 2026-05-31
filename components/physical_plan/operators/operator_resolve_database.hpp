#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_database_t;
} // namespace components::logical_plan

namespace components::operators {

    // B14.C (Pass 9 USER DECISION): leaf operator that scans pg_database
    // (OID=19) by datname and emits the resolved database_oid as a single-row
    // data_chunk. Mirrors operator_resolve_namespace_t's shape, but targets
    // pg_database instead of pg_namespace — these are distinct catalog tables
    // and their OIDs are routing keys for different subsystems
    // (manager_wal_replicate per-database workers / Variant C multi-inner).
    //
    // Output chunk schema:
    //   col 0: UINTEGER  — database_oid (oid_t / uint32_t). One row when
    //                       the database exists; zero rows when it doesn't.
    //
    // The resolved oid is stamped onto the back-pointer node so the
    // dispatcher's Pass-2 (enrich) can populate execution_context_t.database_oid
    // without re-issuing an async actor message — same lifecycle pattern as
    // operator_resolve_namespace_t.
    class operator_resolve_database_t final : public read_write_operator_t {
    public:
        operator_resolve_database_t(std::pmr::memory_resource* resource, log_t log, std::string name);

        operator_resolve_database_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::string name,
                                    components::logical_plan::node_catalog_resolve_database_t* target_node);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
        components::logical_plan::node_catalog_resolve_database_t* target_node_{nullptr};
    };

} // namespace components::operators
