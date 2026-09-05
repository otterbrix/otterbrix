#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>

namespace components::operators {

    // COMMIT TRANSACTION operator.
    //
    // RPC mode (default): one txn_commit_drain_msg round-trip returns the allocated commit_id, then
    // storage_publish_* flips MVCC state and txn_publish_msg advances the ProcArray barrier. STEP
    // ORDER IS AN INVARIANT — no step that can fail may run after the first step that stamps
    // commit_id (full step table at the head of the .cpp's await_async_and_resume). DDL-commit mode
    // (set_ddl_commit) emits its WAL marker at STEP 2, not before, so a restart can't resurrect a
    // commit this process rejected.
    //
    // commit_id() exposes the result for the dispatcher's unique_future API.
    class operator_commit_transaction_t final : public read_write_operator_t {
    public:
        operator_commit_transaction_t(std::pmr::memory_resource* resource, log_t log);

        // Configure DDL-commit mode (default is RPC mode).
        void set_ddl_commit(std::uint64_t txn_id, components::catalog::oid_t database_oid) noexcept {
            is_ddl_commit_ = true;
            txn_id_ = txn_id;
            database_oid_ = database_oid;
        }

        // Result accessor; valid only after the operator reports is_executed().
        std::uint64_t commit_id() const noexcept { return commit_id_; }

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults. All commit work — the dispatcher drain, storage publishes,
        // WAL marker, ProcArray barrier — runs in await_async_and_resume.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        std::uint64_t commit_id_{0};
    };

} // namespace components::operators
