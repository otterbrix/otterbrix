#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Pipeline FK + CHECK + UNIQUE/PK constraint resolution. Resolves EVERY entry
    // on a kind()==constraint resolve node, reading pg_constraint (+ pg_attribute /
    // pg_class / pg_namespace for FK metadata) and stamping the result vectors back
    // into that entry for enrich_logical_plan to consume.
    //
    // Steps (outgoing direction, INSERT/UPDATE):
    //   1. read pg_constraint by conrelid=table_oid.
    //   2. contype='f': resolve child/parent col names via pg_attribute, append to fks.
    //   3. contype='c' with non-empty conexpr: append (conname, conexpr) to check_exprs.
    //   4. contype='u'/'p': resolve conkey attoids to names, append to
    //      unique_constraints (and, for 'p', flatten into pk_columns).
    //
    // Steps (referencing direction, DELETE):
    //   1. read pg_constraint by confrelid=table_oid.
    //   2. contype='f': resolve child/parent col names AND the child table's
    //      {schema, collection} via pg_class + pg_namespace. Append to fks.
    //
    // Each entry's table_oid comes from `tables_node`: the entry's `target` indexes
    // that node's entries, and the fixed resolve order (tables before constraints)
    // guarantees its table_md is already stamped.
    //
    // Output: a 0-row chunk — this is a SINK whose product is the entry stamps.
    class operator_resolve_constraint_t final : public read_write_operator_t {
    public:
        operator_resolve_constraint_t(std::pmr::memory_resource* resource,
                                      log_t log,
                                      components::logical_plan::node_catalog_resolve_t* node,
                                      const components::logical_plan::node_catalog_resolve_t* tables_node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor's resolve pass drives await_async_and_resume via the
        // bottom-up needs_async_finalize walk, after the tables node has run.
        // push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::node_catalog_resolve_t* node_;
        const components::logical_plan::node_catalog_resolve_t* tables_node_;
        // Static output schema, built once in the constructor.
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
