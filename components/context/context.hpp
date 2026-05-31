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

namespace components::table {
    class transaction_manager_t;
} // namespace components::table

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
        // safe to drop. Populated by the executor from txn_manager_t before
        // each operator invocation; consumed by operator_vacuum_t (and any
        // future GC operator) to gate cleanup_versions / cleanup_all_versions.
        uint64_t lowest_active_start_time{0};
        // Transaction manager back-reference for operators that need to
        // mutate the global txn map (Phase 4 #56: operator_commit_transaction_t
        // / operator_abort_transaction_t — invoked from manager_dispatcher_t
        // where the txn_manager_t lives, not from the executor pipeline).
        // Null whenever the operator does not need it (DML/DDL paths leave it
        // unset).
        table::transaction_manager_t* txn_manager{nullptr};

        // Phase 5b: aggregated by operators that touch pg_catalog. Drained by
        // execute_sub_plan_ into result_tracking after pipeline runs.
        std::vector<pg_catalog_append_range_t> pg_catalog_appends;
        std::set<catalog::oid_t> pg_catalog_delete_tables;

        // Block C §3.5 dec 32 V2 OPTION X: pg_attribute commit_id backfill
        // markers. operator_alter_column_{add,drop,rename} push entries here;
        // operator_commit_transaction drains them after commit_id allocation
        // and patches the rows. Empty in implicit-txn / non-ALTER paths.
        std::vector<pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;

        // Phase 5: DML operators (operator_insert / operator_delete /
        // operator_update) record their MVCC swap-info here from inside
        // await_async_and_resume. The executor's commit-side block then drives
        // storage_publish_commit / storage_publish_delete after
        // txn_manager_->commit using these fields. WAL physical writes happen
        // inside the operators themselves; only the commit-side swap requires
        // back-channel.
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
