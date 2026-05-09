#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // COMMIT TRANSACTION — Phase 4 #56 operator-pipeline replacement for
    // manager_dispatcher_t::commit_transaction.
    //
    // Steps (in await_async_and_resume):
    //   1. Snapshot txn_data via ctx->txn_manager->find_transaction(session)
    //      *before* commit() removes the transaction from the active set.
    //   2. ctx->txn_manager->commit(session) — synchronous, returns commit_id.
    //   3. If txn_data.transaction_id != 0, commit_id > 0, and disk_address is
    //      set: send storage_commit_appends(execution_context_t{...},
    //      commit_id, ranges) to flip MVCC state on pg_catalog rows that were
    //      appended under this explicit transaction (matches the auto-commit
    //      DDL path).
    //   4. mark_executed().
    //
    // WAL semantics: identical to the legacy dispatcher path, which did not
    // emit a wal::commit_txn record from the manager-level commit_transaction
    // (DML/DDL paths emit their own commit_txn records as part of execute_plan
    // / execute_ddl). Preserved as a no-op here.
    //
    // commit_id is exposed via commit_id() so the dispatcher can fulfil its
    // unique_future<uint64_t> public API.
    class operator_commit_transaction_t final : public read_write_operator_t {
    public:
        operator_commit_transaction_t(std::pmr::memory_resource* resource, log_t log);

        // Result accessor; valid only after the operator reports is_executed().
        std::uint64_t commit_id() const noexcept { return commit_id_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::uint64_t commit_id_{0};
    };

} // namespace components::operators
