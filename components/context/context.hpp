#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
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

        // THREE MAILBOXES THAT DEFAULT TO "NO MAILBOX", AND EVERY READER HAS TO KNOW IT.
        // Neither constructor takes them, so a context is born fully UNWIRED and each site
        // fills in only what it happens to need: agent_disk's group-by context sets none of
        // the three, and the dispatcher's and executor's admin contexts set disk_address and
        // leave index/wal empty. Those sites are correct today only because the operators they
        // drive send to nothing else — a property of the operator, not of the context, so
        // adding one send inside such an operator breaks a caller that never changed.
        //
        // The structural fix is to make them constructor arguments (or a type with no default
        // state), which deletes the partially-wired context as a shape. It is not done here
        // because it rewrites every construction site, and those live in the dispatcher, the
        // collection executor, agent_disk and the tests.
        //
        // Until then, the containment is that an addressed send goes through
        // actor_zeta::otterbrix::send (core/executor.hpp), which refuses an empty target with a
        // message and abort() in every build, rather than walking a null resource into the
        // mailbox. That turns "silently wrong" into "loudly dead"; it does not make an unwired
        // context correct. A site that legitimately may be unwired (a WAL manager that was
        // never spawned) still needs its own `!= empty_address()` check, and ~57 sites have one.
        actor_zeta::address_t disk_address{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t index_address{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t wal_address{actor_zeta::address_t::empty_address()};

        table::transaction_data txn{0, 0};
        components::graph_execution_context execution_context{};
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
        // commit-side swap needs this back-channel. LISTS so a bounded DML sink
        // can flush per-batch and record ONE range per flush; a single-flush op
        // records exactly one. operator_fk_cascade_t (a DIFFERENT child table)
        // pushes here too under the PARENT txn id, so COMMIT publishes and ABORT
        // (revert_all_deletes(parent_txn_id) / storage_revert_appends) reverts
        // parent + cascade child mutations as one atomic batch.
        std::vector<table::dml_append_range_t> dml_appends;
        std::vector<table::dml_delete_range_t> dml_deletes;
        // Executor-set flush control for bounded DML sinks. dml_flush_is_final:
        // false before a MID-pump flush of a buffering DML sink, true before the final
        // post-pump async-finalize drive (the DML emits its RETURNING / affected-count
        // output_ + mark_executed ONLY on the true call). dml_has_parent_constraint:
        // true when a constraint sink (fk_check / fk_cascade / check_constraint) sits
        // ABOVE the DML in the chain -> the DML accumulates constraint_input_ across
        // flushes; false -> it drops it (bounded memory). Defaults match the
        // single-final-flush, no-parent-constraint case.
        bool dml_flush_is_final{true};
        bool dml_has_parent_constraint{false};
        // EXPLAIN ANALYZE: when true, execute_pipeline records per-operator time/rows/loops into the
        // operators it drives (zero clock sampling when false). Set in-place in execute_sub_plan_ and
        // read via this ctx pointer only.
        bool analyze{false};
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

        // BY REFERENCE, NOT BY VALUE. storage_parameters wraps a std::pmr::unordered_map and has
        // no copy constructor of its own, so a by-value parameter copied the caller's map with a
        // DEFAULT-constructed polymorphic_allocator — std::pmr::get_default_resource() — and the
        // move into `parameters` then froze the process-global arena into the member. The
        // executor hands an LVALUE (`*plan_data.parameters`) once per sub-plan, so EVERY
        // parameterised statement paid that. Both constructors rebuild the map on the arena the
        // caller's parameters already live on; see parameters_on_their_own_arena in context.cpp.
        explicit context_t(const logical_plan::storage_parameters& init_parameters);
        // Defaulted so EVERY member moves. A hand-written one drops whatever it forgets
        // (txn, the DML range lists, the created/dropped back-channels, committed_id, ...)
        // and a moved context then publishes nothing and reverts nothing. Defaulting also
        // keeps future members covered without anyone remembering this ctor.
        context_t(context_t&& context) noexcept = default;
        context_t(session::session_id_t session,
                  actor_zeta::address_t address,
                  actor_zeta::address_t sender,
                  const compute::function_registry_t* function_registry,
                  const logical_plan::storage_parameters& init_parameters);

        const actor_zeta::address_t& address() const noexcept { return address_; }

        // NO PRODUCER LEFT IN THE TREE. The only callers of add_pending_disk_future were the
        // update/delete DML flush legs, which parked the future of manager_disk_t::flush — a
        // contract method whose body traced and returned without flushing anything. Both the
        // method and those sends are gone, so the list is now always empty and the four
        // dispatcher drains plus the two executor drains that read it are inert. The accessors
        // stay because those drain sites live outside this component and go in their own edit;
        // do not read the surviving drains as evidence that a disk future is ever parked here.
        void add_pending_disk_future(disk_future_t&& future) { pending_disk_futures_.push_back(std::move(future)); }

        std::vector<disk_future_t> take_pending_disk_futures() { return std::move(pending_disk_futures_); }

        bool has_pending_disk_futures() const noexcept { return !pending_disk_futures_.empty(); }

    private:
        actor_zeta::address_t address_{actor_zeta::address_t::empty_address()};
        std::vector<disk_future_t> pending_disk_futures_;
    };

} // namespace components::pipeline
