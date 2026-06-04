#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include <atomic>
#include <boost/lockfree/queue.hpp>

#include <core/date/date_types.hpp>
#include <core/executor.hpp>
#include <list>
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

        // In-flight slot for the event-loop model. The loop thread drains the
        // lock-free inbox_ into a loop-private list of these, lazily creates
        // the behavior (coroutine) for each, resumes ready continuations, and
        // erases done entries. pending_msg holds the message until the loop
        // calls behavior(msg.get()); stale_ticks counts consecutive loop
        // passes a slot has been busy-but-not-ready (watchdog input).
        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
            uint32_t stale_ticks{0};
        };

        manager_dispatcher_t(
            std::pmr::memory_resource*,
            actor_zeta::scheduler_raw,
            log_t& log);
        ~manager_dispatcher_t();

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

        static constexpr std::size_t executor_pool_size_ = 4;

        // FUTURE: this map will be partitioned across 4 executors by `oid % 4`.
        // Until that migration, collections_ stays here and is the source of
        // truth. See services/collection/executor.hpp for the migration
        // contract.
        //
        // Fast-path membership cache for collections. Loop-thread-private:
        // only the event-loop thread (via execute_plan handlers) mutates or
        // reads it — grep shows zero external readers. Cache rebuild on
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

        // Event-loop model: enqueue_impl (any sender thread) only delivers a
        // message into the lock-free inbox_ and notifies pump_cv_; ALL message
        // processing — behavior creation, continuation resume, cleanup —
        // happens on loop_thread_. mutex_/pump_cv_ now guard only the loop's
        // idle sleep at the bottom of each pass (woken early by enqueue).
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // lock-free inbox: senders only deliver; ALL processing happens on
        // loop_thread_. Stores raw message* (boost::lockfree requires
        // trivially-copyable): release() on push, re-wrapped into message_ptr
        // by the loop. Node allocations are non-PMR (infra queue).
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_;
        std::condition_variable pump_cv_;

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

        // Fire-and-forget unique_future<void> GC list. Loop-thread-private —
        // only the event loop appends (broadcast/register sends) and drains it
        // via poll_pending(); the in_flight slot list is a local inside the
        // loop lambda (see ctor) rather than a member.
        std::pmr::vector<actor_zeta::unique_future<void>> pending_void_;

        void poll_pending();
    };

} // namespace services::dispatcher
