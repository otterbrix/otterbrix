#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>

namespace components::operators {

    // COMMIT TRANSACTION operator.
    //
    // RPC mode (is_ddl_commit=false, default): replaces the legacy
    // manager_dispatcher_t::commit_transaction inline body. Steps:
    //   1. Snapshot txn_data + swap-info from ctx->txn_manager.
    //   2. ctx->txn_manager->commit(session) → commit_id.
    //   3. storage_commit_appends / storage_commit_deletes via disk actor.
    //
    // DDL-commit mode (is_ddl_commit=true): same as RPC mode plus a
    // prefix of:
    //   0a. manager_disk_t::flush(session, wal::id_t{0}) — durability barrier.
    //   0b. manager_wal_replicate_t::commit_txn(session, txn_id, FULL,
    //       database_oid) — emit WAL commit record.
    //
    // commit_id is exposed via commit_id() so the dispatcher can fulfil its
    // unique_future<uint64_t> public API.
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

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        std::uint64_t commit_id_{0};
    };

} // namespace components::operators
