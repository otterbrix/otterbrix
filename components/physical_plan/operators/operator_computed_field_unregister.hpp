#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Drops a relkind='g' column by appending a pg_computed_column tombstone row (attrefcount = 0)
    // rather than a delete_pg_catalog_rows physical delete, keeping the audit trail of every
    // (column, version) pair -- the row-versioning style the rest of the catalog uses for relkind='g'.
    class operator_computed_field_unregister_t final : public read_write_operator_t {
    public:
        operator_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t attoid,
                                             std::string column_name,
                                             bool missing_ok);

        // The executor admits this sourceless sink leaf as a streaming sink-root, driving
        // await_async_and_resume via the bottom-up needs_async_finalize pass.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
        std::string column_name_;
        // Same node field the regular pg_attribute drop reads -- only IF EXISTS accepts a missing column.
        bool missing_ok_;
    };

} // namespace components::operators
