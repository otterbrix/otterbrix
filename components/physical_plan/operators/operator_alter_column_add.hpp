#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>

namespace components::operators {

    // ALTER TABLE ... ADD COLUMN — single clause.
    //
    // Steps (in await_async_and_resume):
    //   1. read_rows_by_key on pg_attribute (attrelid=table_oid) to compute next attnum.
    //   2. allocate_oids_batch(1) for the new attoid.
    //   3. build_pg_attribute_row + append_pg_catalog_row.
    // resolve_table reads columns from pg_attribute on every call.
    class operator_alter_column_add_t final : public read_write_operator_t {
    public:
        operator_alter_column_add_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    components::catalog::oid_t table_oid,
                                    components::table::column_definition_t column);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults; replaces the legacy on_execute + find_waiting_operator drive.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        components::table::column_definition_t column_;
    };

} // namespace components::operators
