#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // ALTER TABLE ... RENAME COLUMN old TO new — single clause.
    //
    // Matches the live row by (attrelid, attname==old_name_), not attoid_: node_alter_column_t::set_attoid
    // has no callers, so attoid_ is always INVALID and serves only as a cross-check. Re-appends the row with
    // attname=new_name, carrying over attoid/attnum/atttypid/added_at_commit_id (rename is identity-preserving).
    // Storage keeps its own copy of the column name, so a kind_t::storage_rename marker is armed for
    // operator_commit_transaction_t to apply AFTER the WAL commit marker, leaving an ABORT with nothing to undo.
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
