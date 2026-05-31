#pragma once

#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include <core/date/date_types.hpp>
#include <core/executor.hpp>
#include <mutex>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/compute/function.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/collection/context_storage.hpp>
#include <services/collection/executor.hpp>
#include <services/disk/disk_contract.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_contract.hpp>

namespace services::disk {
    class manager_disk_t;
} // namespace services::disk

namespace services::dispatcher {

    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
        using collection_storage_t = std::pmr::set<qualified_name_t>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using sync_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        using run_fn_t = std::function<void()>;

        manager_dispatcher_t(
            std::pmr::memory_resource*,
            actor_zeta::scheduler_raw,
            log_t& log,
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_dispatcher_t();

        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(sync_pack pack);

        // Bootstrap-time hook called from base_spaces after WAL replay scans
        // all records and finds the maximum durable commit_id. Advances
        // published_horizon_ past everything already on disk so post-recovery
        // snapshots observe the right MVCC visibility. Direct sync call is
        // allowed here: scheduler_dispatcher_ has not been started yet
        // (rule 11 base_spaces bootstrap exception). Idempotent — publish()
        // is monotonic and ignores stale ids.
        void set_replay_horizon_sync(uint64_t commit_id);

        // Catalog scan rebuild — base_spaces calls these after rebuilding
        // dropped_storages_ / dropped_table_agents_ so the very first
        // post-start horizon advance broadcasts on_horizon_advanced to the
        // affected subscribers and finishes the GC pass that the pre-crash
        // DROP didn't get to. Equivalent to the mailbox handler
        // on_drop_resource_marked() but usable pre-scheduler.start (rule 11
        // base_spaces bootstrap exception). Idempotent.
        void set_disk_has_dropped_sync(bool value) noexcept { disk_has_dropped_ = value; }
        void set_index_has_dropped_sync(bool value) noexcept { index_has_dropped_ = value; }

        unique_future<components::cursor::cursor_t_ptr>
        execute_plan(components::session::session_id_t session,
                     components::logical_plan::node_ptr plan,
                     components::logical_plan::parameter_node_ptr params);
        unique_future<components::cursor::cursor_t_ptr>
        get_schema(components::session::session_id_t session,
                   std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids);
        unique_future<bool> register_udf(components::session::session_id_t session,
                                         components::compute::function_ptr function);
        unique_future<bool> unregister_udf(components::session::session_id_t session,
                                           std::string function_name,
                                           std::pmr::vector<components::types::complex_logical_type> inputs);

        // Transaction lifecycle (actor-callable by executor)
        unique_future<components::table::transaction_data> begin_transaction(components::session::session_id_t session);
        unique_future<uint64_t> commit_transaction(components::session::session_id_t session);
        unique_future<void> abort_transaction(components::session::session_id_t session);

        // Low-level transaction-manager wrappers for the executor (constraint
        // #11). The executor currently dereferences a raw
        // `transaction_manager_t*` shared from this dispatcher (anti-pattern —
        // mutable state shared across actors). These handlers replay the same
        // sync calls inside the dispatcher's own actor context, so the
        // executor only ever communicates via the mailbox.
        //
        // Each handler is a thin pass-through to `txn_manager_.{method}()`
        // with no operator-pipeline / WAL / disk side-effects. The heavier
        // commit_transaction / abort_transaction handlers above continue to
        // route DDL commits/aborts through their operator pipelines.
        //
        //   txn_begin_msg          → tm.begin_transaction(session).data()
        //   txn_commit_msg         → tm.commit(session)  (returns commit_id)
        //   txn_abort_msg          → tm.abort(session)
        //   txn_publish_msg        → tm.publish(commit_id)  (ProcArray barrier)
        //   txn_lowest_active_msg  → tm.lowest_active_start_time()
        unique_future<components::table::transaction_data>
        txn_begin_msg(components::session::session_id_t session);
        unique_future<uint64_t> txn_commit_msg(components::session::session_id_t session);
        unique_future<void> txn_abort_msg(components::session::session_id_t session);
        unique_future<void> txn_publish_msg(uint64_t commit_id);
        unique_future<uint64_t> txn_lowest_active_msg();

        // SET TIME ZONE mailbox handler (constraint #11). The dispatcher owns
        // default_tz_cat_ (session-shared mutable state); the executor cannot
        // mutate it directly without violating the no-shared-state rule, so it
        // routes SET TIME ZONE through this handler. The body mirrors the
        // dispatcher's own set_timezone_t case in execute_plan_impl: mutates
        // default_tz_cat_, then appends a ("TimeZone", <name>) row to
        // pg_settings via disk_address_'s append_pg_catalog_row mailbox.
        // Returns the success/error cursor.
        unique_future<components::cursor::cursor_t_ptr>
        set_default_timezone_msg(components::session::session_id_t session, std::pmr::string tz_name);

        // Selective broadcast — DROP TABLE / DROP INDEX path marks the owning
        // subscriber as "has dropped resources pending GC" via this mailbox
        // handler. Cleared by on_subscriber_empty once the subscriber's
        // dropped_storages_ queue is empty. These are mailbox handlers invoked
        // via actor_zeta::send (rule 11 — no sync parent-pointer calls). They
        // return unique_future<void> because actor_zeta::dispatch requires all
        // actor methods to return unique_future<T> or generator<T>.
        unique_future<void> on_drop_resource_marked(uint8_t subscriber_kind);
        // Subscriber-empty ack — subscriber sends this back once its
        // dropped_storages_ queue drained (i.e. nothing left to GC for that
        // kind), which clears the corresponding broadcast flag and stops
        // further on_horizon_advanced broadcasts to that subscriber.
        unique_future<void> on_subscriber_empty(uint8_t subscriber_kind);

        using dispatch_traits = actor_zeta::dispatch_traits<&manager_dispatcher_t::execute_plan,
                                                            &manager_dispatcher_t::get_schema,
                                                            &manager_dispatcher_t::register_udf,
                                                            &manager_dispatcher_t::unregister_udf,
                                                            &manager_dispatcher_t::begin_transaction,
                                                            &manager_dispatcher_t::commit_transaction,
                                                            &manager_dispatcher_t::abort_transaction,
                                                            &manager_dispatcher_t::txn_begin_msg,
                                                            &manager_dispatcher_t::txn_commit_msg,
                                                            &manager_dispatcher_t::txn_abort_msg,
                                                            &manager_dispatcher_t::txn_publish_msg,
                                                            &manager_dispatcher_t::txn_lowest_active_msg,
                                                            &manager_dispatcher_t::set_default_timezone_msg,
                                                            &manager_dispatcher_t::on_drop_resource_marked,
                                                            &manager_dispatcher_t::on_subscriber_empty>;

    private:
        // Cleanup-trigger helper. Called INLINE from the commit_txn /
        // abort_txn handler bodies (not wired yet). Regular private member
        // function — NOT a std::function callback (rule 13) — so the call
        // site stays a direct method call without indirection. Reads
        // `txn_manager_.lowest_active_start_time()`, and if it advanced since
        // `last_broadcast_horizon_`, sends `on_horizon_advanced(new)` to each
        // subscriber whose drop-resource flag is set. Skip-send is the common
        // case (no drops outstanding → no message bursts on every commit).
        void try_trigger_cleanup_if_horizon_advanced() noexcept;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        run_fn_t run_fn_; // Yield function for cooperative scheduling

        static constexpr std::size_t executor_pool_size_ = 4;

        // FUTURE: this map will be partitioned across 4 executors by `oid % 4`.
        // Until that migration, collections_ stays here and is the source of
        // truth. See services/collection/executor.hpp for the migration
        // contract.
        //
        // Fast-path membership cache for collections. Read by
        // physical_plan_generator in 8 sites (join, match, aggregate, sort,
        // group) to drive optimizer decisions. Removing would require touching
        // all 8 + replacing with on-demand pg_class scans. Cache rebuild on
        // init_from_state is cheap.
        collection_storage_t collections_;
        std::pmr::vector<services::collection::executor::executor_ptr> executors_;
        std::pmr::vector<actor_zeta::address_t> executor_addresses_;

        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();

        // Selective broadcast flags. Set when DROP TABLE / DROP INDEX marks
        // a resource dropped (via on_drop_resource_marked); cleared by the
        // subscriber's on_subscriber_empty ack. Single-actor private state —
        // no atomic / no shared (rule 10).
        bool disk_has_dropped_{false};
        bool index_has_dropped_{false};
        // Cached last-broadcast horizon to skip redundant on_horizon_advanced
        // sends — every commit advances lowest_active by at most one txn, but
        // many commits do not advance it at all (long-running concurrent txn
        // pins it). Only re-broadcast when the value actually moves forward.
        uint64_t last_broadcast_horizon_{0};

        std::mutex mutex_;

        components::table::transaction_manager_t txn_manager_;
        components::catalog::session_catalog_t default_tz_cat_;

        core::date::timezone_offset_t session_tz(components::session::session_id_t /*session*/) const {
            return default_tz_cat_.timezone_offset;
        }

        components::logical_plan::node_ptr create_logic_plan(components::logical_plan::node_ptr plan);

        unique_future<services::collection::executor::execute_result_t>
        execute_plan_impl(components::session::session_id_t session,
                          components::logical_plan::node_ptr logical_plan,
                          components::logical_plan::storage_parameters parameters,
                          components::table::transaction_data txn,
                          context_storage_t* collections_context_storage);

        actor_zeta::behavior_t current_behavior_;
    };

} // namespace services::dispatcher
