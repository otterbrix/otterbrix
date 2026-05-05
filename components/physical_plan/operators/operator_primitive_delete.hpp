#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/catalog/catalog_oids.hpp>

#include <cstddef>

namespace components::operators {

    // Deletes a catalog row matched by OID from a pg_* system table.
    // Used by the DDL pipeline emitted by the planner for DROP operations.
    // Disk primitive (direct_delete_sync) added in Etap 5.
    class operator_primitive_delete_t final : public read_write_operator_t {
    public:
        operator_primitive_delete_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::size_t    oid_col_index,
                                     catalog::oid_t match_oid);

        catalog::oid_t table_oid()     const noexcept { return table_oid_; }
        std::size_t    oid_col_index() const noexcept { return oid_col_index_; }
        catalog::oid_t match_oid()     const noexcept { return match_oid_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::oid_t table_oid_;
        std::size_t    oid_col_index_;
        catalog::oid_t match_oid_;
    };

} // namespace components::operators
