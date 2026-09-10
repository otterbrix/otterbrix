#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/settings.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction.hpp>
#include <set>
#include <vector>

namespace components::compute {
    class function_registry_t;
} // namespace components::compute

namespace components::pipeline {

    // Forward-declared to avoid an include cycle (subplan_runner.hpp -> operator_data.hpp -> ... -> context.hpp).
    struct subplan_runner_t;

    // Spelled as a word so a call site that drives send-free operators, or runs with the WAL off, reads as a choice.
    inline actor_zeta::address_t no_mailbox() { return actor_zeta::address_t::empty_address(); }

    class context_t {
    public:
        using disk_future_t = actor_zeta::unique_future<void>;

        session::session_id_t session;
        actor_zeta::address_t current_message_sender{actor_zeta::address_t::empty_address()};
        const compute::function_registry_t* function_registry = nullptr;
        logical_plan::storage_parameters parameters;

        // All three are live in any engine base_spaces builds. no_mailbox() here means a unit-test
        // topology driving an operator without that manager -- never a production configuration.
        actor_zeta::address_t disk_address;
        actor_zeta::address_t index_address;
        actor_zeta::address_t wal_address;

        table::transaction_data txn{0, 0};
        components::graph_execution_context execution_context{};
        // VACUUM/MVCC threshold; no transaction_manager_t* here -- the dispatcher owns all txn state.
        uint64_t lowest_active_start_time{0};

        // Non-owning; nullptr when no executor is driving -- callers must null-check before run_subplan.
        subplan_runner_t* runner{nullptr};

        std::vector<pg_catalog_append_range_t> pg_catalog_appends;
        std::set<catalog::oid_t> pg_catalog_delete_tables;

        // ALTER COLUMN pushes entries here; drained and patched after commit_id allocation.
        std::vector<pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;

        // MVCC swap-info back-channel; cascade children push here too, under the PARENT txn id.
        std::vector<table::dml_append_range_t> dml_appends;
        std::vector<table::dml_delete_range_t> dml_deletes;
        // dml_flush_is_final gates RETURNING/mark_executed; dml_has_parent_constraint gates constraint accumulation.
        bool dml_flush_is_final{true};
        bool dml_has_parent_constraint{false};
        // When true, execute_pipeline records per-operator time/rows/loops (zero-cost when false).
        bool analyze{false};
        // Recorded by operator_dynamic_cascade_delete_t so COMMIT's drain runs GC off the ACTUAL drops.
        std::vector<catalog::oid_t> dropped_storage_oids;
        // Mirror of dropped_storage_oids for CREATE: publishable at COMMIT, revertible at ABORT.
        std::vector<catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;
        // commit_id drained by operator_commit_transaction_t, for follow-ups like an inline CREATE INDEX commit.
        uint64_t committed_id{0};

        catalog::setting_id applied_setting{};
        std::string applied_setting_value;

        // By reference: a by-value parameter froze the default allocator into `parameters` (context.cpp).
        context_t(const logical_plan::storage_parameters& init_parameters,
                  actor_zeta::address_t disk,
                  actor_zeta::address_t index,
                  actor_zeta::address_t wal);
        // Defaulted so every member moves; a hand-written ctor would drop whatever it forgets to list.
        context_t(context_t&& context) noexcept = default;
        context_t(session::session_id_t session,
                  actor_zeta::address_t address,
                  actor_zeta::address_t sender,
                  const compute::function_registry_t* function_registry,
                  const logical_plan::storage_parameters& init_parameters,
                  actor_zeta::address_t disk,
                  actor_zeta::address_t index,
                  actor_zeta::address_t wal);

        const actor_zeta::address_t& address() const noexcept { return address_; }

        // No producer left (manager_disk_t::flush is gone); accessors stay for drain sites outside this component.
        void add_pending_disk_future(disk_future_t&& future) { pending_disk_futures_.push_back(std::move(future)); }

        std::vector<disk_future_t> take_pending_disk_futures() { return std::move(pending_disk_futures_); }

        bool has_pending_disk_futures() const noexcept { return !pending_disk_futures_.empty(); }

    private:
        actor_zeta::address_t address_{actor_zeta::address_t::empty_address()};
        std::vector<disk_future_t> pending_disk_futures_;
    };

} // namespace components::pipeline
