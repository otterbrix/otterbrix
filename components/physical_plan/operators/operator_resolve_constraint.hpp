#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::logical_plan {
    class node_catalog_resolve_constraint_t;
} // namespace components::logical_plan

namespace components::operators {

    // Pipeline FK + CHECK constraint resolution.
    // Reads pg_constraint (+ pg_attribute / pg_class / pg_namespace for FK
    // metadata) and stamps the result vectors on the back-pointed logical node
    // so enrich_logical_plan can consume them via plan_resolve_index_t.
    //
    // Steps (outgoing direction, INSERT/UPDATE):
    //   1. read pg_constraint by conrelid=table_oid.
    //   2. for each row with contype='f': resolve child/parent col names via
    //      pg_attribute scans, append to fks vector.
    //   3. for each row with contype='c' and non-empty conexpr: append
    //      (conname, conexpr) to check_exprs vector.
    //
    // Steps (referencing direction, DELETE):
    //   1. read pg_constraint by confrelid=table_oid.
    //   2. for each row with contype='f': resolve child/parent col names AND
    //      resolve child table {schema, collection} via pg_class + pg_namespace
    //      scans. Append to fks vector.
    //
    // table_oid is read from target_node_->resolved_metadata()->table_oid at
    // execute time — Pass 1 guarantees the prerequisite resolve_table operator
    // ran before this one.
    class operator_resolve_constraint_t final : public read_write_operator_t {
    public:
        operator_resolve_constraint_t(std::pmr::memory_resource* resource,
                                      log_t log,
                                      components::logical_plan::node_catalog_resolve_constraint_t* target_node);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::node_catalog_resolve_constraint_t* target_node_{nullptr};
    };

} // namespace components::operators