#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Operator-pipeline replacement for the dedicated
    // manager_disk_t::resolve_table actor message. Self-resolves a table's
    // column schema via standard disk-actor primitives (read_rows_by_key).
    //
    // Inputs: (namespace_oid, dbname, relname). Table identity is resolved
    // at EXECUTION time with the following precedence:
    //   1. namespace_oid != INVALID_OID -> pg_class scan by
    //      (relname, relnamespace).
    //   2. dbname non-empty -> pg_namespace scan by nspname first (an
    //      execution-time disk read, mirroring operator_resolve_type_t),
    //      then the MANDATORY two-key pg_class scan. A namespace miss is a
    //      hard not-found: a qualified name never degrades to a
    //      relname-only scan (that degradation was the #557 cross-database
    //      leak — the first same-named table from ANY database won).
    //   3. dbname empty (unqualified name) -> pg_class scan by relname
    //      alone. No session default-database substitution exists, so this
    //      is what makes unqualified access work.
    //
    // Steps:
    //   0. resolve table_oid as described above.
    //   1. read pg_class by oid -> capture relkind, relnamespace.
    //   2. relkind='r': read pg_attribute by attrelid -> rows. Drop tombstones
    //      (attisdropped=true), sort by attnum.
    //   3. relkind='g': read pg_computed_column by relid -> rows. Apply
    //      max-version-per-attname filter, drop entries whose max-version
    //      row is a tombstone (attrefcount<=0), sort by attoid (matches the
    //      register-order layout used by storage adopt_schema).
    //   4. Build a data_chunk_t with columns
    //      (position int32, attoid uint32, attname string,
    //       atttypid uint32, atttypspec string). Empty when the table is
    //      unknown / has no columns.
    //
    // Side info: relkind, relnamespace, and the resolved oid are exposed via
    // accessors for callers that need to branch on table flavor or chain
    // another resolve.
    class operator_resolve_table_t final : public read_write_operator_t {
    public:
        // After resolving, the operator stamps namespace_oid + table_oid
        // onto target_node so the dispatcher (validate / enrich) can read
        // them via plan_resolve_index_t. namespace_oid is a pre-resolved
        // relnamespace when the caller already has one; INVALID_OID
        // otherwise (the common case — dbname is then resolved at
        // execution time, see the class comment).
        operator_resolve_table_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 components::catalog::oid_t namespace_oid,
                                 std::string dbname,
                                 std::string relname,
                                 components::logical_plan::node_catalog_resolve_t* target_node);

        // Accessors for downstream callers. resolved_found() is false when
        // pg_class has no row matching the inputs; the other accessors hold
        // their default-init values in that case.
        bool resolved_found() const noexcept { return found_; }
        char resolved_relkind() const noexcept { return relkind_; }
        components::catalog::oid_t resolved_namespace() const noexcept { return namespace_oid_; }
        components::catalog::oid_t resolved_table_oid() const noexcept { return table_oid_; }

        // Sourceless SINK leaf (catalog read, no data pipeline, no children).
        // The executor admits the resolve front-pass as an all-sink chain and
        // drives await_async_and_resume via the bottom-up needs_async_finalize
        // pass. The operator is self-contained: every input it needs (dbname,
        // relname) is a plan-time constant, and all catalog lookups — including
        // the dbname -> namespace_oid translation — happen inside
        // await_async_and_resume, so it does not depend on any sibling
        // operator's execution-time stamps. The async pg_namespace / pg_class /
        // pg_attribute / pg_computed_column scans emit the column-schema chunk
        // into output_ and stamp namespace_oid_/table_oid_ onto target_node_;
        // push()/finalize() inherit the no-op defaults (the metadata handoff is
        // the node stamp, read later via plan_resolve_index).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // table_oid_ is resolved inside await_async_and_resume from
        // (input_namespace_oid_ | dbname_, relname_). namespace_oid_ (below)
        // is the *resolved* relnamespace, populated by the oid-keyed
        // pg_class scan.
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t input_namespace_oid_{components::catalog::INVALID_OID};
        std::string dbname_;
        std::string relname_;
        bool found_{false};
        char relkind_{0};
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::logical_plan::node_catalog_resolve_t* target_node_{nullptr};
        // Static output chunk schema, built once in the constructor (TASK C10).
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
