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

        // One in-flight message in the event loop. behavior is created lazily;
        // pending_msg holds the message until the loop calls behavior(msg.get()).
        // stale_ticks counts consecutive passes the slot stayed busy-but-not-
        // ready (watchdog input).
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

        // Bootstrap hook: advance the published horizon past the max durable
        // commit_id found during WAL replay, so post-recovery snapshots get the
        // right MVCC visibility. Direct sync call is safe only because the
        // scheduler is not started yet. Idempotent — publish() ignores stale ids.
        void set_replay_horizon_sync(uint64_t commit_id);

        // Like the on_drop_resource_marked() mailbox handler but usable before
        // scheduler.start: base_spaces calls these after rebuilding the dropped-
        // resource queues so the first post-start horizon advance broadcasts
        // on_horizon_advanced and finishes the GC the pre-crash DROP missed.
        // Idempotent.
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

        // Low-level transaction-manager wrappers for the executor. The executor
        // currently dereferences a raw `transaction_manager_t*` shared from this
        // dispatcher (anti-pattern — mutable state shared across actors). These
        // handlers replay the same sync calls inside the dispatcher's own actor
        // context, so the executor only ever communicates via the mailbox.
        // Each is a thin pass-through to txn_manager_ with no operator-pipeline /
        // WAL / disk side-effects; the heavier commit_transaction /
        // abort_transaction handlers above still route DDL through their pipelines.
        unique_future<components::table::transaction_data>
        txn_begin_msg(components::session::session_id_t session);
        unique_future<uint64_t> txn_commit_msg(components::session::session_id_t session);
        unique_future<void> txn_abort_msg(components::session::session_id_t session);
        unique_future<void> txn_publish_msg(uint64_t commit_id);
        unique_future<uint64_t> txn_lowest_active_msg();

        // Selective broadcast: DROP TABLE / DROP INDEX marks the owning
        // subscriber as having dropped resources pending GC; on_subscriber_empty
        // clears the flag once that subscriber's dropped_storages_ queue drains,
        // stopping further on_horizon_advanced broadcasts to it. Return
        // unique_future<void> (not void) because actor_zeta::dispatch requires
        // every actor method to return unique_future<T> or generator<T>.
        unique_future<void> on_drop_resource_marked(uint8_t subscriber_kind);
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
        // Reads txn_manager_.lowest_active_start_time(); if it advanced past
        // last_broadcast_horizon_, sends on_horizon_advanced(new) to each
        // subscriber whose drop-resource flag is set. Skips the send in the
        // common case (no drops outstanding), avoiding a message burst per commit.
        void try_trigger_cleanup_if_horizon_advanced() noexcept;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;

        static constexpr std::size_t executor_pool_size_ = 4;

        // Fast-path membership cache for collections, and the source of truth
        // for them. Loop-thread-private: only the event-loop thread (via
        // execute_plan handlers) mutates or reads it, so no synchronization.
        collection_storage_t collections_;
        std::pmr::vector<services::collection::executor::executor_ptr> executors_;
        std::pmr::vector<actor_zeta::address_t> executor_addresses_;

        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();

        // Selective broadcast flags. Set when DROP TABLE / DROP INDEX marks
        // a resource dropped (via on_drop_resource_marked); cleared by the
        // subscriber's on_subscriber_empty ack. Single-actor private state —
        // no atomic / no shared.
        bool disk_has_dropped_{false};
        bool index_has_dropped_{false};
        // Cached last-broadcast horizon to skip redundant on_horizon_advanced
        // sends — every commit advances lowest_active by at most one txn, but
        // many commits do not advance it at all (long-running concurrent txn
        // pins it). Only re-broadcast when the value actually moves forward.
        uint64_t last_broadcast_horizon_{0};

        // Event-loop model: enqueue_impl (any sender thread) only delivers into
        // inbox_ and notifies pump_cv_; ALL message processing — behavior
        // creation, continuation resume, cleanup — happens on loop_thread_.
        // mutex_/pump_cv_ guard only the loop's idle sleep at the end of each
        // pass (woken early by enqueue).
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Stores raw message* (boost::lockfree requires trivially-copyable):
        // release() on push, re-wrapped into message_ptr by the loop. Node
        // allocations are non-PMR (infra queue).
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
        // via poll_pending().
        std::pmr::vector<actor_zeta::unique_future<void>> pending_void_;

        void poll_pending();
    };

} // namespace services::dispatcher
