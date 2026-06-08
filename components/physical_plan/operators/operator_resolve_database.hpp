#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_database_t;
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
                                    components::logical_plan::node_catalog_resolve_database_t* target_node);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
        components::logical_plan::node_catalog_resolve_database_t* target_node_{nullptr};
    };

} // namespace components::operators
