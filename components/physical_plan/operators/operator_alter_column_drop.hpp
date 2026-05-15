#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // ALTER TABLE ... DROP COLUMN — single clause.
    //
    // Steps (in await_async_and_resume):
    //   1. Read pg_attribute rows for attrelid=table_oid; locate the live row matching
    //      attname=column_name to capture its attoid + atttypid + attnum + flags.
    //   2. Read pg_depend rows where (refclassid=pg_attribute_table, refobjid=attoid)
    //      for dependent indexes / constraints.
    //   3. For RESTRICT: any normal-deptype dep aborts with set_error.
    //      For CASCADE (default): drop each dependent object.
    //        - dep_cls == pg_class -> delete pg_index/pg_depend/pg_class rows.
    //        - dep_cls == pg_constraint -> delete pg_constraint/pg_depend rows.
    //   4. Soft-delete the column: delete the original pg_attribute row and
    //      append_pg_catalog_row a tombstone with attisdropped=true. This is the
    //      same pattern the legacy ddl.cpp drop_column branch used.
    //
    // No in-memory schema mutation hook is invoked: subsequent resolve_table
    // operator runs pick up the tombstone via pg_attribute reads.
    class operator_alter_column_drop_t final : public read_write_operator_t {
    public:
        operator_alter_column_drop_t(std::pmr::memory_resource*           resource,
                                      log_t                                log,
                                      components::catalog::oid_t           table_oid,
                                      components::catalog::oid_t           namespace_oid,
                                      std::string                          column_name,
                                      components::catalog::oid_t           attoid,
                                      components::catalog::drop_behavior_t behavior);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t           table_oid_;
        components::catalog::oid_t           namespace_oid_;
        std::string                          column_name_;
        components::catalog::oid_t           attoid_;
        components::catalog::drop_behavior_t behavior_;
    };

} // namespace components::operators
