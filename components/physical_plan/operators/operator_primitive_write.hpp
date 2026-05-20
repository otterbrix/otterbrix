#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {

    // Writes a single pre-built row into a pg_catalog table.
    // The row is built by the planner (ddl_metadata_builder) with allocated OIDs.
    // At execution time calls disk.append_pg_catalog_row (WAL + direct_append_sync).
    class operator_primitive_write_t final : public read_write_operator_t {
    public:
        operator_primitive_write_t(std::pmr::memory_resource* resource,
                                   log_t log,
                                   components::catalog::oid_t catalog_table_oid,
                                   vector::data_chunk_t row);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t catalog_table_oid_;
        vector::data_chunk_t row_;
    };

} // namespace components::operators
