#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>
#include <set>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/table/row_version_manager.hpp>
#include <components/table/transaction.hpp>
#include <core/date/date_types.hpp>
#include <services/collection/context_storage.hpp>
#include <stack>
#include <string>

namespace services::collection::executor {

    // One range per (table, DML fragment), accumulated across sub-plans.
    // Must accumulate, not overwrite: FK cascade DELETE on >=2 tables emits a
    // range per child table, and a single last-wins field would silently drop
    // the publishes for every non-last child.
    struct dml_append_range_t {
        components::catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        components::catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    struct execute_result_t {
        components::cursor::cursor_t_ptr cursor;
        // INTERNAL accumulators: populated by execute_plan (the operator
        // pipeline) and fully CONSUMED by execute_plan_full's commit/abort
        // tail before the result crosses the mailbox back to the dispatcher —
        // implicit DML publishes them inline (per-range + index mirrors),
        // explicit DML / DDL ships them to the dispatcher's transaction_t via
        // txn_accumulate_msg. The dispatcher reads ONLY cursor and
        // applied_timezone.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends{};
        std::set<components::catalog::oid_t> pg_catalog_delete_tables{};
        // markers emitted by ALTER COLUMN ADD/DROP/RENAME; ride to
        // transaction_t via txn_accumulate_msg so operator_commit_transaction
        // can patch the rows after commit_id allocation.
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills{};
        std::vector<dml_append_range_t> dml_appends{};
        std::vector<dml_delete_range_t> dml_deletes{};
        // Storage oids whose backing files a DROP scrubbed this statement,
        // lifted from pipeline::context_t::dropped_storage_oids. Shipped in the
        // accumulate payload so operator_commit_transaction's DROP-GC remap
        // block keys off the drained drop set rather than the ddl-commit mode
        // flag. Cleared with the other accumulators by the commit tail.
        std::vector<components::catalog::oid_t> dropped_storage_oids{};
        // CREATE counterpart of dropped_storage_oids: the storage oids / indexes
        // a CREATE TABLE / CREATE INDEX brought into being this statement, lifted
        // from pipeline::context_t::created_storage_oids / created_indexes.
        // Shipped in the accumulate payload so COMMIT publishes them and a same-
        // txn ABORT drops the still-uncommitted artifacts. Cleared with the
        // other accumulators by the commit/abort tail.
        std::vector<components::catalog::oid_t> created_storage_oids{};
        std::vector<components::table::created_index_t> created_indexes{};
        // Commit back-channel lifted from pipeline::context_t::committed_id:
        // non-zero when this pipeline ran operator_commit_transaction_t (the
        // CREATE INDEX tail needs the allocated commit_id for its index-only
        // backfill commit). 0 = no commit ran.
        uint64_t commit_id{0};
        // Non-empty => a SET TIMEZONE statement persisted this zone name to
        // pg_settings; the dispatcher refreshes its default_tz_cat_ from it.
        // The ONLY post-execute signal the dispatcher consumes besides cursor.
        std::string applied_timezone{};
    };

    using function_result_t = core::result_wrapper_t<components::compute::function_uid>;

    struct plan_t {
        std::stack<components::operators::operator_ptr> sub_plans;
        // Non-owning: points at the shared parameter node's storage owned by
        // the execute_plan frame, which outlives execute_sub_plan_. Avoids
        // copying the parameter map into every plan_t (the per-sub-plan
        // pipeline::context_t still owns its own copy).
        const components::logical_plan::storage_parameters* parameters;
        services::context_storage_t context_storage_;
        components::logical_plan::limit_t limit;

        explicit plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                        const components::logical_plan::storage_parameters* parameters,
                        services::context_storage_t&& context_storage,
                        components::logical_plan::limit_t limit = components::logical_plan::limit_t::unlimit());
    };

    // Internal result with MVCC tracking (never crosses an actor boundary).
    // DML operators self-contain WAL/storage/index I/O and record swap-info on
    // pipeline::context_t::dml_*. execute_sub_plan_ drains those onto the
    // dml_* vectors below so execute_plan_full's commit tail can drive
    // storage_publish_commit / storage_publish_delete for every range.
    struct sub_plan_result_t {
        components::cursor::cursor_t_ptr cursor;
        // Accumulating vectors (FK cascade correctness — see dml_append_range_t).
        std::vector<dml_append_range_t> dml_appends;
        std::vector<dml_delete_range_t> dml_deletes;
        // Storage oids drained from pipeline::context_t::dropped_storage_oids
        // (a DROP scrubbed their backing files). execute_plan moves these into
        // execute_result_t for the accumulate tail.
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        // CREATE counterpart: storage oids / indexes drained from
        // pipeline::context_t::created_storage_oids / created_indexes (a CREATE
        // brought them into being). execute_plan moves these into
        // execute_result_t for the accumulate tail.
        std::vector<components::catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;

        // pg_catalog swap-info drained from each pipeline::context_t inside
        // execute_sub_plan_. execute_plan moves these into the outer
        // execute_result_t for the commit/accumulate tail.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;
        // Commit back-channel (pipeline::context_t::committed_id).
        uint64_t commit_id{0};
    };

    class executor_t final : public actor_zeta::basic_actor<executor_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        executor_t(std::pmr::memory_resource* resource,
                   actor_zeta::address_t parent_address,
                   actor_zeta::address_t wal_address,
                   actor_zeta::address_t disk_address,
                   actor_zeta::address_t index_address,
                   log_t&& log);
        ~executor_t() = default;

        // Operator-pipeline run over an already-rewritten plan. INTERNAL:
        // called only from execute_plan_full (directly, via co_await — never
        // through the mailbox). The txn lifecycle is owned by the caller.
        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::execution_plan_t plan,
                                                     services::context_storage_t context_storage,
                                                     components::table::transaction_data txn,
                                                     uint64_t lowest_active_start_time);

        // THE per-query entry point (the dispatcher's only execute send).
        // Runs the full pipeline on an unrewritten logical_plan: session
        // context fetch (txn/tz/is_explicit/lowest_active via one
        // txn_begin_session_msg round-trip), optimize, resolve wrap, catalog
        // resolve loop, view splice, validate, enrich, planner rewrites, OID
        // allocation, the operator pipeline, and the DML/DDL commit (or
        // abort/accumulate) tail. All txn-state access rides txn_*_msg
        // messages to the dispatcher — the sole transaction_manager_t owner.
        unique_future<execute_result_t> execute_plan_full(components::session::session_id_t session,
                                                          components::logical_plan::execution_plan_t plan);

        unique_future<std::unique_ptr<function_result_t>> register_udf(components::session::session_id_t session,
                                                                       components::compute::function_ptr function);

        // No-op poke target for the dispatcher's lost-wakeup watchdog (see
        // executor.cpp for the rationale).
        unique_future<void> poke_msg();

        using dispatch_traits = actor_zeta::
            dispatch_traits<&executor_t::execute_plan_full, &executor_t::register_udf, &executor_t::poke_msg>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        plan_t traverse_plan_(components::operators::operator_ptr&& plan,
                              const components::logical_plan::storage_parameters& parameters,
                              services::context_storage_t&& context_storage);

        unique_future<sub_plan_result_t> execute_sub_plan_(components::session::session_id_t session,
                                                           plan_t plan_data,
                                                           components::table::transaction_data txn,
                                                           uint64_t lowest_active_start_time);

        // THE unified commit publisher: builds node_commit_transaction_t
        // (ddl_mode adds the flush/WAL prefix) and runs it through the same
        // execute_plan pipeline every statement uses. The operator drains
        // transaction_t via txn_commit_drain_msg, batch-publishes storage,
        // commits the index mirrors per table, writes the WAL marker, crosses
        // the ProcArray barrier (txn_publish_msg) and fans out maybe_cleanup.
        unique_future<execute_result_t> run_commit_pipeline_(components::session::session_id_t session,
                                                             components::table::transaction_data txn,
                                                             core::date::timezone_offset_t session_tz,
                                                             uint64_t lowest_active_start_time,
                                                             bool ddl_mode);

    private:
        actor_zeta::address_t parent_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();
        log_t log_;
        components::compute::function_registry_t function_registry_;
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor
