#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator that scans pg_database (OID=19, distinct from pg_namespace)
    // by datname and emits the resolved database_oid as a single UINTEGER column:
    // one row when the database exists, zero rows otherwise. The oid is also
    // stamped onto the back-pointer node so the dispatcher's enrich pass can
    // populate execution_context_t.database_oid without a second async message.
    class operator_resolve_database_t final : public read_write_operator_t {
    public:
        operator_resolve_database_t(std::pmr::memory_resource* resource, log_t log, std::string name);

        operator_resolve_database_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::string name,
                                    components::logical_plan::node_catalog_resolve_t* target_node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor admits the resolve front-pass as an all-sink chain and drives
        // await_async_and_resume via the bottom-up needs_async_finalize pass. The
        // async pg_database scan emits a single-row chunk into output_ and stamps the
        // resolved database_oid onto target_node_; push()/finalize() inherit the
        // no-op defaults (the metadata handoff is the node stamp, read later via
        // plan_resolve_index).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
        components::logical_plan::node_catalog_resolve_t* target_node_{nullptr};
        // Static output chunk schema, built once in the constructor (TASK C10).
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
