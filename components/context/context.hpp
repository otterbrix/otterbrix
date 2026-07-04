#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/pg_catalog_swap.hpp>
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

    // Forward-declared (NOT included): context_t holds only a raw, non-owning
    // pointer to the runner, so an incomplete type suffices and we avoid the
    // include cycle (subplan_runner.hpp -> operator_data.hpp; operator.hpp ->
    // context.hpp). subplan_runner.hpp itself stays out of this header.
    struct subplan_runner_t;

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

        // Sub-plan execution seam. The executor sets this to itself (it
        // implements subplan_runner_t) right where it builds the context, before
        // driving the plan. An operator that needs to run a child sub-plan
        // through the SAME streaming executor calls runner->run_subplan(root,
        // this). Non-owning raw pointer: the executor outlives every context it
        // builds, and this is an INTRA-actor seam (the operator runs inside the
        // executor actor's coroutine), not cross-actor shared state. nullptr when
        // no executor is driving (e.g. a context built for a path that never runs
        // operators); callers must null-check before use.
        subplan_runner_t* runner{nullptr};

        // Aggregated by operators that touch pg_catalog. Drained by
        // execute_sub_plan_ into result_tracking after pipeline runs.
        std::vector<pg_catalog_append_range_t> pg_catalog_appends;
        std::set<catalog::oid_t> pg_catalog_delete_tables;

        // pg_attribute commit_id backfill markers.
        // operator_alter_column_{add,drop,rename} push entries here;
        // operator_commit_transaction drains them after commit_id allocation
        // and patches the rows. Empty in implicit-txn / non-ALTER paths.
        std::vector<pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;

        // DML append/delete RANGE-LISTS. insert/update/delete/backfill record their
        // MVCC swap-info here from inside await_async_and_resume; the executor's
        // lift_dml_ranges drains them into the per-statement accumulators that feed
        // txn_accumulate_msg. WAL physical writes happen in the operators; only the
        // commit-side swap needs this back-channel. These are LISTS (not single
        // slots) so a bounded DML sink can flush per-batch and record ONE range per
        // flush; a single-flush op records exactly one. operator_fk_cascade_t (a
        // DIFFERENT child table) pushes here too under the PARENT txn id, so COMMIT
        // publishes and ABORT (revert_all_deletes(parent_txn_id) / storage_revert_
        // appends) reverts parent + cascade child mutations as one atomic batch.
        std::vector<table::dml_append_range_t> dml_appends;
        std::vector<table::dml_delete_range_t> dml_deletes;
        // Executor-set flush control for bounded DML sinks (3b-B). dml_flush_is_final:
        // false before a MID-pump flush of a buffering DML sink, true before the final
        // post-pump async-finalize drive (the DML emits its RETURNING / affected-count
        // output_ + mark_executed ONLY on the true call). dml_has_parent_constraint:
        // true when a constraint sink (fk_check / fk_cascade / check_constraint) sits
        // ABOVE the DML in the chain -> the DML accumulates constraint_input_ across
        // flushes; false -> it drops it (bounded memory). Defaults = pre-3b-B behavior.
        bool dml_flush_is_final{true};
        bool dml_has_parent_constraint{false};
        // DROP back-channel: operator_dynamic_cascade_delete_t records each
        // storage oid it dropped (alongside the mark_storage_dropped_many send). The
        // executor lifts these into execute_result_t.dropped_storage_oids and
        // ships them in the txn_accumulate payload so COMMIT's drain can drive
        // the DROP-GC value-space remap off the ACTUAL drops (decoupled from
        // whichever DDL mode lowered the statement). Plain std::vector matching
        // the sibling cross-mailbox value fields above.
        std::vector<catalog::oid_t> dropped_storage_oids;
        // CREATE back-channel (mirror of dropped_storage_oids): the DDL operators
        // that bring a storage / index into being record them here —
        // operator_create_collection / operator_create_matview push the new
        // storage oid into created_storage_oids, operator_create_index_backfill
        // pushes {table_oid, name} into created_indexes. The executor lifts both
        // into execute_result_t and ships them in the txn_accumulate payload so a
        // CREATE inside an explicit txn is publishable at COMMIT and revertible at
        // ABORT (ABORT drops the still-uncommitted storage / index). Plain
        // std::vector matching the sibling cross-mailbox value fields above.
        std::vector<catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;
        // Commit back-channel: operator_commit_transaction_t records the
        // commit_id it drained (txn_commit_drain_msg reply) so the executor's
        // tail can drive follow-ups that need it (the inline CREATE INDEX
        // index commit). 0 = no commit ran in this pipeline.
        uint64_t committed_id{0};

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
