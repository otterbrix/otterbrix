#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // ABORT (ROLLBACK) TRANSACTION — Phase 4 #56 operator-pipeline replacement
    // for manager_dispatcher_t::abort_transaction.
    //
    // Steps (in await_async_and_resume):
    //   1. Snapshot txn_data via ctx->txn_manager->find_transaction(session)
    //      *before* abort() purges the active map.
    //   2. ctx->txn_manager->abort(session) — synchronous.
    //   3. If txn_data.transaction_id != 0 and disk_address is set: send
    //      storage_revert_appends(execution_context_t{...}, ranges) so any DDL rows
    //      this transaction wrote into pg_catalog tables are tombstoned.
    //   4. mark_executed().
    //
    // WAL semantics: there is no wal::abort_txn message; the legacy dispatcher
    // body did not emit any WAL record from abort_transaction (replay simply
    // discards uncommitted records). Preserved as a no-op here.
    class operator_abort_transaction_t final : public read_write_operator_t {
    public:
        operator_abort_transaction_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
