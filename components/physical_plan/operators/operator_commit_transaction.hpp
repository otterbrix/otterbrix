#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>

namespace components::operators {

    // COMMIT TRANSACTION operator.
    //
    // RPC mode (default): one txn_commit_drain_msg round-trip to the dispatcher
    // (sole owner of transaction_manager_t) returns the snapshotted txn_data,
    // the drained swap-info and the allocated commit_id; then
    // storage_publish_commits / storage_publish_deletes flip MVCC state, and a
    // final txn_publish_msg advances the ProcArray barrier.
    //
    // DDL-commit mode (set_ddl_commit): prepends a flush durability barrier and
    // a WAL commit_txn record (with commit_id=0, since it isn't allocated yet)
    // before the RPC-mode body.
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

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        std::uint64_t commit_id_{0};
    };

} // namespace components::operators
