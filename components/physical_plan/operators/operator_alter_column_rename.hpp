#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // ALTER TABLE ... RENAME COLUMN old TO new — single clause.
    //
    // Steps (in await_async_and_resume):
    //   1. read_chunks_by_key on pg_attribute (attrelid=table_oid_), then match the live row BY
    //      (attrelid, attname==old_name_). Keying on attoid_ instead is a silent no-op:
    //      node_alter_column_t::set_attoid has no callers, so attoid_ is always INVALID. It is a
    //      CROSS-CHECK only: a stamped identity must match the row its name found.
    //   2. delete_pg_catalog_rows on the matched attoid (idx=0).
    //   3. build_pg_attribute_row reusing attoid/attnum/atttypid/added_at but with attname=new_name, and
    //      append_pg_catalog_row. Renaming is identity-preserving, so added_at_commit_id is carried over.
    //   4. arm a kind_t::storage_rename marker on the pipeline context. The STORAGE keeps its own copy of
    //      the column name and the write path addresses columns by it, so it has to be renamed too — and
    //      only after the commit, so an ABORT leaves nothing behind. operator_commit_transaction_t performs
    //      it after the WAL commit marker and the publish barrier. Identity is the attoid: the bootstrap
    //      reconciliation compares on it and repairs a stale storage name, so this marker keeps the LIVE
    //      halves in step rather than surviving a restart.
    //
    // The catalog half becomes visible to SQL on subsequent resolve_table runs (which read pg_attribute
    // fresh).
    class operator_alter_column_rename_t final : public read_write_operator_t {
    public:
        operator_alter_column_rename_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       components::catalog::oid_t table_oid,
                                       components::catalog::oid_t attoid,
                                       std::string old_name,
                                       std::string new_name);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
        std::string old_name_;
        std::string new_name_;
    };

} // namespace components::operators
