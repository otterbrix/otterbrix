#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <set>
#include <vector>

namespace components::compute {
    class function_registry_t;
} // namespace components::compute

namespace components::pipeline {

    class context_t {
    public:
        using disk_future_t = actor_zeta::unique_future<void>;

        session::session_id_t session;
        actor_zeta::address_t current_message_sender{actor_zeta::address_t::empty_address()};
        const compute::function_registry_t* function_registry = nullptr;
        logical_plan::storage_parameters parameters;

        actor_zeta::address_t disk_address{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t index_address{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t wal_address{actor_zeta::address_t::empty_address()};

        table::transaction_data txn{0, 0};
        core::date::timezone_offset_t session_tz{};
        // VACUUM/MVCC GC threshold: snapshots older than this start_time are
        // safe to drop. Populated by the executor from the session context
        // (txn_begin_session_msg) before each operator invocation; consumed by
        // operator_vacuum_t to gate cleanup_versions / cleanup_all_versions.
        //
        // NOTE: there is deliberately NO transaction_manager_t* here. The
        // dispatcher is the sole txn-state owner; operators that need txn
        // mutations (begin/commit/abort) send txn_*_msg messages to
        // current_message_sender (the executor's parent — the dispatcher).
        uint64_t lowest_active_start_time{0};

        // Aggregated by operators that touch pg_catalog. Drained by
        // execute_sub_plan_ into result_tracking after pipeline runs.
        std::vector<pg_catalog_append_range_t> pg_catalog_appends;
        std::set<catalog::oid_t> pg_catalog_delete_tables;

        // pg_attribute commit_id backfill markers.
        // operator_alter_column_{add,drop,rename} push entries here;
        // operator_commit_transaction drains them after commit_id allocation
        // and patches the rows. Empty in implicit-txn / non-ALTER paths.
        std::vector<pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;

        // DML operators (insert/delete/update) record MVCC swap-info here from
        // inside await_async_and_resume; the executor's commit-side block drives
        // storage_publish_commit / storage_publish_delete after txn_manager_->commit.
        // WAL physical writes happen in the operators; only the commit-side swap
        // needs this back-channel.
        int64_t dml_append_row_start{0};
        uint64_t dml_append_row_count{0};
        uint64_t dml_delete_txn_id{0};
        catalog::oid_t dml_table_oid{catalog::INVALID_OID};

        explicit context_t(logical_plan::storage_parameters init_parameters);
        context_t(context_t&& context) noexcept;
        context_t(session::session_id_t session,
                  actor_zeta::address_t address,
                  actor_zeta::address_t sender,
                  const compute::function_registry_t* function_registry,
                  logical_plan::storage_parameters init_parameters);

        const actor_zeta::address_t& address() const noexcept { return address_; }

        void add_pending_disk_future(disk_future_t&& future) { pending_disk_futures_.push_back(std::move(future)); }

        std::vector<disk_future_t> take_pending_disk_futures() { return std::move(pending_disk_futures_); }

        bool has_pending_disk_futures() const noexcept { return !pending_disk_futures_.empty(); }

    private:
        actor_zeta::address_t address_{actor_zeta::address_t::empty_address()};
        std::vector<disk_future_t> pending_disk_futures_;
    };

} // namespace components::pipeline
