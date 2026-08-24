#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator that resolves EVERY entry on a kind()==table resolve node,
    // stamping each entry's namespace_oid + table_md. All tables a statement
    // depends on — across every sub-query — are resolved in this one run, and the
    // per-dbname namespace lookup is cached across entries.
    //
    // Per entry, table identity is resolved at EXECUTION time:
    //   - dbname non-empty -> pg_namespace scan by nspname first, then the
    //     MANDATORY two-key (relname, relnamespace) pg_class scan. A namespace miss
    //     is a hard not-found: a qualified name never degrades to a relname-only
    //     scan (that degradation was the #557 cross-database leak — the first
    //     same-named table from ANY database won).
    //   - dbname empty (unqualified name) -> pg_class scan by relname alone. No
    //     session default-database substitution exists, so this is what makes
    //     unqualified access work. Deliberately NO ""->public defaulting (unlike
    //     resolve_type): unqualified CREATE TABLE writes relnamespace=INVALID.
    //
    // Then, per resolved table:
    //   1. read pg_class by oid -> capture relkind, relnamespace.
    //   2. relkind 'v'/'m': read pg_rewrite.ev_action -> view_sql.
    //   3. relkind='r'/'m': read pg_attribute by attrelid. Drop tombstones
    //      (attisdropped) and columns outside the txn snapshot, sort by attnum.
    //   4. relkind='g': read pg_computed_column by relid. Keep the max-version row
    //      per (attname, atttypid, atttypspec) variant, drop tombstoned variants
    //      (attrefcount<=0), sort by attoid (the register-order layout storage
    //      adopt_schema uses), then bind each to its physical storage column.
    //
    // Output: a 0-row chunk — this is a SINK whose product is the entry stamps,
    // which validate / enrich / the planner read straight off the node.
    class operator_resolve_table_t final : public read_write_operator_t {
    public:
        operator_resolve_table_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 components::logical_plan::node_catalog_resolve_t* node);

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor's resolve pass drives await_async_and_resume via the
        // bottom-up needs_async_finalize walk. The operator is self-contained:
        // every input it needs is a plan-time constant on the node, and all
        // catalog lookups — including dbname -> namespace_oid — happen inside
        // await_async_and_resume, so it depends on no other operator's stamps.
        // push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::node_catalog_resolve_t* node_;
        // Static output schema, built once in the constructor.
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
