#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {

    // Inserts one pre-built catalog row into a pg_* system table.
    // Used by the DDL pipeline emitted by the planner for CREATE operations.
    // Disk primitive (direct_append_sync) added in Etap 5.
    class operator_primitive_write_t final : public read_write_operator_t {
    public:
        operator_primitive_write_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    catalog::oid_t table_oid,
                                    vector::data_chunk_t row);

        catalog::oid_t                   table_oid() const noexcept { return table_oid_; }
        const vector::data_chunk_t&      row()       const noexcept { return row_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::oid_t       table_oid_;
        vector::data_chunk_t row_;
    };

} // namespace components::operators
