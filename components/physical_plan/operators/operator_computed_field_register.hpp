#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>

#include <vector>

namespace components::operators {

    // Runs after an INSERT into a relkind='g' (computing / Mongo-style
    // dynamic-schema) table; (re)registers the schema columns into
    // pg_computed_column.
    //
    // Per column, the operator:
    //   1. read_rows_by_key on pg_computed_column for (relid, attname).
    //   2. Classifies into NEW (no rows), SAME-TYPE (latest atttypid matches),
    //      or TYPE-EVOLUTION (latest atttypid differs).
    //   3. For NEW or TYPE-EVOLUTION: allocate one attoid, build a
    //      pg_computed_column row (attversion = prior_max + 1, attrefcount = 1)
    //      and append it via append_pg_catalog_row, recording the resulting
    //      append range into pipeline::context_t::pg_catalog_appends.
    //   4. SAME-TYPE rows are no-ops (refcount stays 1; we do not bump per
    //      INSERT — the simplified binary refcount model used here treats
    //      refcount as alive/dead only).
    class operator_computed_field_register_t final : public read_write_operator_t {
    public:
        operator_computed_field_register_t(std::pmr::memory_resource* resource,
                                           log_t log,
                                           components::catalog::oid_t table_oid,
                                           std::vector<components::table::column_definition_t> columns);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        std::vector<components::table::column_definition_t> columns_;
    };

} // namespace components::operators
