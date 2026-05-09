#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

#include <vector>

namespace components::logical_plan {

    // Phase 7.1 — planner-emitted leaf that registers (or evolves) the set of
    // pg_computed_column rows for an INSERT into a relkind='g' (generated /
    // computing / Mongo-style dynamic-schema) table.
    //
    // Carries the parent table's OID and the list of columns observed in the
    // INSERT row(s). The matching operator
    // (operator_computed_field_register_t) does, for each column:
    //
    //   1. read_rows_by_key on pg_computed_column where
    //        relid == table_oid AND attname == column.name()
    //   2. classify the result into NEW (no rows) / SAME-TYPE (latest row's
    //      atttypid matches) / TYPE-EVOLUTION (latest row's atttypid differs).
    //   3. for NEW or TYPE-EVOLUTION: allocate a fresh attoid, build a
    //      pg_computed_column row with a refreshed attversion and
    //      attrefcount=1, and append it via append_pg_catalog_row, recording
    //      the resulting range into pipeline::context_t::pg_catalog_appends.
    //
    // Refcount model (simplified): in this milestone refcount is BINARY —
    // 1 = column is alive, 0 = column has been unregistered. We do not bump
    // refcount on every INSERT. The version counter (attversion) still
    // increases on type evolution so the resolver picks the latest type.
    class node_computed_field_register_t final : public node_t {
    public:
        node_computed_field_register_t(std::pmr::memory_resource* resource,
                                        collection_full_name_t      collection,
                                        components::catalog::oid_t  table_oid,
                                        std::vector<components::table::column_definition_t> columns);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const std::vector<components::table::column_definition_t>& columns() const noexcept { return columns_; }
        std::vector<components::table::column_definition_t>&       columns()       noexcept { return columns_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t                                  table_oid_;
        std::vector<components::table::column_definition_t>         columns_;
    };

    using node_computed_field_register_ptr = boost::intrusive_ptr<node_computed_field_register_t>;

} // namespace components::logical_plan
