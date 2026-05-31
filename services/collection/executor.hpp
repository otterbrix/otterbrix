#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>
#include <set>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/table/row_version_manager.hpp>
#include <core/btree/btree.hpp>
#include <services/collection/context_storage.hpp>
#include <stack>
#include <string>
#include <unordered_map>

namespace components::table {
    class transaction_manager_t;
    class collection_t;
}

namespace services::collection::executor {

    struct execute_result_t {
        components::cursor::cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t updates{};
        // pg_catalog ranges/tables collected during this execute_plan call.
        // Dispatcher merges these into transaction_t when txn_id != 0.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends{};
        std::set<components::catalog::oid_t> pg_catalog_delete_tables{};
        // markers emitted by ALTER COLUMN ADD/DROP/RENAME. Dispatcher pushes
        // them onto transaction_t so operator_commit_transaction can patch
        // the rows after commit_id allocation.
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills{};
    };

    using function_result_t = core::result_wrapper_t<components::compute::function_uid>;

    struct plan_t {
        std::stack<components::operators::operator_ptr> sub_plans;
        components::logical_plan::storage_parameters parameters;
        services::context_storage_t context_storage_;
        components::logical_plan::limit_t limit;

        explicit plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                        components::logical_plan::storage_parameters parameters,
                        services::context_storage_t&& context_storage,
                        components::logical_plan::limit_t limit = components::logical_plan::limit_t::unlimit());
    };
    using plan_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, plan_t>;

    // B33 (Pass 9 BLOCKER 33 preemptive accumulation): accumulate per-table DML
    // ranges across sub-plans so FK cascade DELETE on >=2 tables publishes each
    // child's flip — previously last-DML-fragment-wins overwrote earlier ranges
    // and silently dropped publishes for non-last child tables.
    struct dml_append_range_t {
        components::catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        components::catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    // Internal result with MVCC tracking (not exposed to dispatcher).
    // DML operators self-contain WAL/storage/index I/O and record swap-info on
    // pipeline::context_t::dml_*. execute_sub_plan_ drains those onto the
    // dml_* vectors below so execute_plan can drive storage_publish_commit /
    // storage_publish_delete for every accumulated range.
    struct sub_plan_result_t {
        components::cursor::cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t updates;
        // B33: accumulating vectors replace single fields (FK cascade correctness).
        std::vector<dml_append_range_t> dml_appends;
        std::vector<dml_delete_range_t> dml_deletes;

        // pg_catalog swap-info drained from each pipeline::context_t inside
        // execute_sub_plan_. execute_plan moves these into the outer
        // execute_result_t so the dispatcher can push them onto transaction_t.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;
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
                   components::table::transaction_manager_t* txn_manager,
                   log_t&& log);
        ~executor_t() = default;

        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::node_ptr logical_plan,
                                                     components::logical_plan::storage_parameters parameters,
                                                     services::context_storage_t context_storage,
                                                     components::table::transaction_data txn);

        // executor takes over manager_dispatcher_t::execute_plan
        // pipeline — Pass 1 catalog resolve, validate, enrich, planner.rewrite,
        // optimizer, physical_plan_generator, then operator pipeline. The current
        // execute_plan method runs the operator pipeline only; execute_plan_full
        // adds the upstream Pass 1/1.5/1.6 stages so the dispatcher can shed them.
        // Implementation lives in executor.cpp at execute_plan_full (Pass 1
        // resolve loop, Phase 1.5 view splice, Phase 1.6 stamp+gather, Pass 2
        // validate / enrich / planner.rewrite, then delegate to execute_plan
        // for the operator pipeline). Call signature parallels execute_plan
        // but takes the unrewritten logical_plan and runs the full pipeline.
        unique_future<execute_result_t>
        execute_plan_full(components::session::session_id_t session,
                          components::logical_plan::node_ptr logical_plan,
                          components::logical_plan::storage_parameters parameters,
                          services::context_storage_t context_storage,
                          components::table::transaction_data txn);

        unique_future<std::unique_ptr<function_result_t>> register_udf(components::session::session_id_t session,
                                                                       components::compute::function_ptr function);

        // dispatcher fans these out (single send, NOT broadcast) to the executor
        // whose index == hash(oid) % executor_pool_size_ (= oid % 4). Each
        // executor owns its slice of the global table map in `local_collections_`.
        //
        // (this revision): replaced the raw `collection_t*` placeholder
        // with a value-type POD entry that the executor copy-stores in its own
        // map. This deliberately avoids the constraint #11 trap of sharing a
        // mutable `collection_t` between actor and dispatcher — the entry is a
        // by-value membership + identity record (oid + database + schema +
        // name). Cross-partition queries (JOIN of tables landing in different
        // executor slices) fall back to the dispatcher's `collections_` set;
        // intra-partition DML / DDL can probe `find_local_collection(oid)`
        // before paying that mailbox hop.
        //
        // + (deferred): erase `dispatcher.collections_` entirely. That
        // requires migrating the 8 physical_plan_generator membership probes
        // that read it indirectly through enrich/validate, and is intentionally
        // out of scope here — see executor.cpp Pass 1/2/3 Serial track.
        //
        // NOT a `collection_t*` and NOT a `shared_ptr<collection_t>` (rule 14,
        // constraint #11). If ownership migration later requires moving state
        // into this slot, the path is a pmr-allocated owning handle reachable
        // only from this actor.
        struct local_collection_entry_t {
            components::catalog::oid_t oid{components::catalog::INVALID_OID};
            std::string database;
            std::string schema;
            std::string name;
        };

        // Kept as an alias for the dispatcher fanout signature continuity —
        // its body now constructs `local_collection_entry_t` directly. The
        // typedef is no longer used as a transport type.
        using collection_ptr_t = local_collection_entry_t;

        unique_future<void> register_collection_local(components::session::session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       local_collection_entry_t entry);
        unique_future<void> unregister_collection_local(components::session::session_id_t session,
                                                         components::catalog::oid_t table_oid);

        // Hot-path lookup for DML inside this actor's partition. Returns
        // nullptr for oids whose hash routes to a different executor slice —
        // callers must then fall through to the cross-partition path (today:
        // dispatcher.collections_ + disk_address_ resolve). Single-actor
        // private map, no synchronization (rule 10).
        const local_collection_entry_t*
        find_local_collection(components::catalog::oid_t oid) const noexcept {
            auto it = local_collections_.find(oid);
            return it == local_collections_.end() ? nullptr : &it->second;
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&executor_t::execute_plan,
                                                            &executor_t::execute_plan_full,
                                                            &executor_t::register_udf,
                                                            &executor_t::register_collection_local,
                                                            &executor_t::unregister_collection_local>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        plan_t traverse_plan_(components::operators::operator_ptr&& plan,
                              components::logical_plan::storage_parameters&& parameters,
                              services::context_storage_t&& context_storage);

        unique_future<sub_plan_result_t> execute_sub_plan_(components::session::session_id_t session,
                                                           plan_t plan_data,
                                                           components::table::transaction_data txn);

        // + Pass 12 LoC bump) collections_ partition:
        // FUTURE WORK — manager_dispatcher_t::collections_ map currently owns all
        // tables. Variant E.3 partitions this across 4 executors by hash:
        //   pool_idx = oid % executor_pool_size_ = oid % 4
        // Each executor owns its slice (`std::pmr::unordered_map<oid_t, collection_ptr>`)
        // so DML hot path stops crossing the manager_dispatcher mailbox for local-partition
        // ops.
        //
        // Cross-partition queries (JOIN) fall back to resolve_table via disk_address_
        // (already async). Pre-check on CREATE/DROP DDL: each executor maintains
        // local pre/post DDL checks on its own slice — no router round-trip on CREATE
        // success.
        //
        // Migration order: (1) add executors_'s `local_collections_` map AND the
        // routing helper `pool_idx_for_plan(node_ptr)`; (2) populate each executor's
        // slice from manager_dispatcher's collections_ at base_spaces bootstrap
        // (Phase 3.5 hook); (3) DML hot path reads from local_collections_; (4)
        // remove manager_dispatcher::collections_ once all callers are migrated.
    private:
        actor_zeta::address_t parent_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();
        components::table::transaction_manager_t* txn_manager_{nullptr};
        log_t log_;
        components::compute::function_registry_t function_registry_;

        // Keeps fire-and-forget WAL flush futures alive until they resolve.
        std::pmr::vector<unique_future<void>> pending_void_;

        // collections_ partition — Step 2 field.
        // Owned slice of the global table map; this executor owns oids where
        // (oid % executor_pool_size_) == own_index. Populated by
        // register_collection_local mailbox handler, cleared by
        // unregister_collection_local. execute_plan probes this map via
        // find_local_collection(oid) before falling back to the
        // dispatcher.collections_ cross-partition path. By-value POD entries —
        // no shared mutable state with the dispatcher (constraint #11).
        std::pmr::unordered_map<components::catalog::oid_t, local_collection_entry_t> local_collections_;

        void poll_pending();
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor
