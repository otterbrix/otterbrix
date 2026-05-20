#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // ALTER TABLE ... RENAME COLUMN old TO new — single clause.
    //
    // Steps (in await_async_and_resume):
    //   1. read_rows_by_key on pg_attribute (attoid=attoid_) — keyed single-row
    //      lookup. attoid_ is pre-stamped by enrich_logical_plan from the
    //      resolved column metadata.
    //   2. delete_pg_catalog_rows on the matched attoid (idx=0).
    //   3. build_pg_attribute_row reusing attoid/attnum/atttypid but with attname=new_name
    //      and append_pg_catalog_row.
    //
    // No in-memory schema rename hook exists today; the change becomes visible
    // on subsequent resolve_table operator runs (which read pg_attribute fresh).
    //
    // old_name_ is retained for trace/error display only — routing is by attoid_.
    class operator_alter_column_rename_t final : public read_write_operator_t {
    public:
        operator_alter_column_rename_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       components::catalog::oid_t table_oid,
                                       components::catalog::oid_t attoid,
                                       std::string old_name,
                                       std::string new_name);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
        std::string old_name_;
        std::string new_name_;
    };

} // namespace components::operators
