#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Deletes all rows from a pg_catalog table where column[oid_col_idx] == target_oid.
    // Built by the planner (execute_ddl) with pre-computed delete specs from plan_drop_cascade.
    // At execution time calls disk.drop_catalog_rows (WAL + MVCC tombstone).
    class operator_primitive_delete_t final : public read_write_operator_t {
    public:
        operator_primitive_delete_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    components::catalog::oid_t catalog_table_oid,
                                    std::int64_t oid_col_idx,
                                    components::catalog::oid_t target_oid);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t catalog_table_oid_;
        std::int64_t oid_col_idx_;
        components::catalog::oid_t target_oid_;
    };

} // namespace components::operators
