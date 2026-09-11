#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>

namespace components::operators {

    // Step order is an invariant: no step that can fail may run after the one that stamps commit_id
    // (full step table atop the .cpp's await_async_and_resume). DDL-commit mode emits its WAL
    // marker at step 2, not before, so a restart can't resurrect a rejected commit.
    class operator_commit_transaction_t final : public read_write_operator_t {
    public:
        operator_commit_transaction_t(std::pmr::memory_resource* resource, log_t log);

        // Default is RPC mode.
        void set_ddl_commit(std::uint64_t txn_id, components::catalog::oid_t database_oid) noexcept {
            is_ddl_commit_ = true;
            txn_id_ = txn_id;
            database_oid_ = database_oid;
        }

        // For the dispatcher's unique_future API; valid only after is_executed().
        std::uint64_t commit_id() const noexcept { return commit_id_; }

        // Sourceless sink leaf; push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        std::uint64_t commit_id_{0};
    };

} // namespace components::operators
