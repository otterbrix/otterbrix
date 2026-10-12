#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/unique_key.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/dispatcher/txn_messages.hpp>

#include <cstdint>
#include <vector>

namespace components::operators {

    struct commit_table_checks_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        std::vector<components::catalog::unique_key_t> unique_constraints;
        std::vector<components::catalog::fk_info_t> fks;
    };

    struct commit_checks_t {
        core::error_t refusal{core::error_t::no_error()};
        std::vector<commit_table_checks_t> outgoing;
        std::vector<commit_table_checks_t> referencing;
    };

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

        void set_input(services::dispatcher::txn_commit_drain_t drain, commit_checks_t checks);

        // For the dispatcher's unique_future API; valid only after is_executed().
        std::uint64_t commit_id() const noexcept { return commit_id_; }

        // Sourceless sink leaf; push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
        // constraints can be broken between last test and commit attempt by a concurrent transaction
        // TODO: check other constraints as well
        actor_zeta::unique_future<core::error_t>
        check_constraints_(pipeline::context_t* ctx,
                           const components::table::transaction_data& commit_snapshot,
                           const std::vector<components::pg_catalog_append_range_t>& appends,
                           const std::vector<components::table::referenced_delete_t>& referenced_deletes);
        actor_zeta::unique_future<core::error_t> fetch_rows_(pipeline::context_t* check_ctx,
                                                             components::catalog::oid_t table_oid,
                                                             const std::pmr::vector<int64_t>& row_ids,
                                                             components::table::fetch_visibility_t visibility,
                                                             chunks_vector_t* rows);
        // Releases commit_id and undoes every write, as ROLLBACK would.
        actor_zeta::unique_future<void>
        refuse_(pipeline::context_t* ctx, components::table::txn_abort_drain_t undo, core::error_t refusal);

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        std::uint64_t commit_id_{0};
        services::dispatcher::txn_commit_drain_t drain_;
        commit_checks_t checks_;
    };

} // namespace components::operators
