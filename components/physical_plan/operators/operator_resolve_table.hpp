#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_table_t;
} // namespace components::logical_plan

namespace components::operators {

    // Operator-pipeline replacement for the dedicated
    // manager_disk_t::resolve_table actor message. Self-resolves a table's
    // column schema via standard disk-actor primitives (read_rows_by_key).
    //
    // Inputs: either table_oid (oid in pg_class) OR (namespace_oid, relname).
    // In the name-resolution form the operator first scans pg_class by
    // (relname, relnamespace) to recover the oid, then proceeds identically.
    //
    // Steps:
    //   0. (name form only) read pg_class by (relname,relnamespace) -> oid.
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
        // oid-form: caller already knows pg_class.oid.
        operator_resolve_table_t(std::pmr::memory_resource* resource, log_t log, components::catalog::oid_t table_oid);

        // name-form: operator scans pg_class by (relname, relnamespace) to
        // recover the oid before resolving columns. namespace_oid may be
        // INVALID_OID, in which case the operator scans by relname alone
        // (matches "no namespace enrichment yet" — typically yields not-found).
        operator_resolve_table_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 components::catalog::oid_t namespace_oid,
                                 std::string relname);

        // back-pointer form. After resolving, the operator stamps
        // namespace_oid + table_oid onto target_node so the dispatcher's
        // (validate / enrich) can read it via plan_resolve_index_t.
        operator_resolve_table_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 components::catalog::oid_t namespace_oid,
                                 std::string relname,
                                 components::logical_plan::node_catalog_resolve_table_t* target_node);

        // Accessors for downstream callers. resolved_found() is false when
        // pg_class has no row matching the inputs; the other accessors hold
        // their default-init values in that case.
        bool resolved_found() const noexcept { return found_; }
        char resolved_relkind() const noexcept { return relkind_; }
        components::catalog::oid_t resolved_namespace() const noexcept { return namespace_oid_; }
        components::catalog::oid_t resolved_table_oid() const noexcept { return table_oid_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // When table_oid_ == INVALID_OID we use (input_namespace_oid_,
        // relname_) and resolve table_oid_ inside await_async_and_resume.
        // namespace_oid_ (below) is the *resolved* relnamespace, populated by
        // the oid-keyed pg_class scan.
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t input_namespace_oid_{components::catalog::INVALID_OID};
        std::string relname_;
        bool found_{false};
        char relkind_{0};
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::logical_plan::node_catalog_resolve_table_t* target_node_{nullptr};
    };

} // namespace components::operators
