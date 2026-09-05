#pragma once

#include <chrono>
#include <condition_variable>
#include <string>
#include <thread>

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

#include <components/casts/cast_registry.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/compute/function.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/session/session.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/executor.hpp>
#include <services/dispatcher/txn_messages.hpp>

namespace services::disk {
    class manager_disk_t;
} // namespace services::disk

namespace services::dispatcher {

    // While work is in flight a future completed on another thread notifies nobody, so readiness is
    // discovered by the wait TIMING OUT: that timeout is the per-hop latency, and a statement crosses
    // ~20 hops. The floor is therefore hops * in_flight_wait. The two waits are only correct
    // together — a shorter tick alone does not revive an executor parked busy && ready.
#ifdef DEV_MODE
    // Hops a statement makes: one timed-out in-flight wait per hop. Machine-independent, unlike the
    // latency it multiplies out to.
    uint64_t pump_hops() noexcept;
    void reset_pump_hops() noexcept;
    void note_pump_hop() noexcept;
#endif

    struct pump_tuning_t final {
        static constexpr auto in_flight_wait = std::chrono::microseconds(5);
        static constexpr auto idle_wait = std::chrono::microseconds(100);
        static constexpr auto poke_after = std::chrono::microseconds(100);
        static constexpr uint32_t stale_tick_threshold = poke_after / in_flight_wait;
    };

    static_assert(pump_tuning_t::stale_tick_threshold == pump_tuning_t::poke_after / pump_tuning_t::in_flight_wait,
                  "the poke threshold must stay DERIVED from the two waits, not written out as a number");
    static_assert(pump_tuning_t::in_flight_wait < pump_tuning_t::idle_wait,
                  "the in-flight tick is the per-hop latency and must be shorter than the idle tick");

    // Thin router + txn-state mailbox service + executor-pool admin.
    //
    // Per-query work (optimize, resolve, validate, enrich, planner rewrites,
    // the operator pipeline, and the DML/DDL commit tails) lives ENTIRELY in
    // executor_t. The dispatcher owns exactly the state that must stay global:
    //   - txn_manager_   (sole owner; commit_id allocation, the ProcArray
    //                     publish horizon, and every transaction_t body) —
    //                     reachable ONLY through the txn_*_msg handlers below;
    //   - default_tz_cat_ (session timezone catalog);
    //   - the executor pool and the DROP-GC subscriber flags.
    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Bootstrap address bundle (plain named struct — no std::tuple).
        struct sync_pack {
            actor_zeta::address_t wal = actor_zeta::address_t::empty_address();
            actor_zeta::address_t disk = actor_zeta::address_t::empty_address();
            actor_zeta::address_t index = actor_zeta::address_t::empty_address();
            // Config-gated DML flush bound (0 = disabled).
            // Trailing so existing 3-field aggregate inits (tests) stay valid.
            uint64_t dml_flush_row_threshold = 0;
        };

        // One in-flight message in the event loop. behavior is created lazily;
        // pending_msg holds the message until the loop calls behavior(msg.get()).
        // stale_ticks counts consecutive passes the slot stayed busy-but-not-
        // ready (watchdog input).
        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
            uint32_t stale_ticks{0};
            // Consecutive watchdog poke rounds this slot stayed stale (reset when
            // its await completes). Routine staleness is normal for any executor
            // operation past ~2ms; a slot surviving hundreds of poke rounds is a
            // genuine stall and escalates the (otherwise trace-level) watchdog
            // log to a warning.
            uint32_t poke_rounds{0};
        };

        // Host-injected customization hooks (create_plan rule + optimizer pass) arrive
        // through THIS constructor (not sync_pack, which carries only late-wired actor
        // addresses) and are forwarded to every executor spawned in sync(). Both plain
        // fn-ptrs, defaulting to their Null Objects so they are never null.
        manager_dispatcher_t(std::pmr::memory_resource*,
                             actor_zeta::scheduler_raw,
                             log_t& log,
                             planner::create_plan_rule_t create_plan_rule = &planner::no_custom_lowering,
                             components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass);
        ~manager_dispatcher_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(sync_pack pack);

        // Bootstrap hook: restore the MVCC commit clock from the combined durable
        // frontier (max of the persisted pg_attribute commit-ids and the max WAL
        // COMMIT-marker commit_id). Raises BOTH halves together via
        // transaction_manager_t::restore_commit_clock: current_timestamp_ to
        // frontier+1 (so a reopened instance issues start_times/commit-ids strictly
        // above every persisted id — no reuse of the already-published band) AND
        // published_horizon_ to frontier (so post-recovery snapshots see persisted
        // commits as published). Direct sync call — safe only because the scheduler
        // is not started yet. Idempotent (never lowers either half).
        void seed_commit_clock_sync(uint64_t high_water);

        // Like the on_drop_resource_marked() mailbox handler but usable before
        // scheduler.start: base_spaces calls these after rebuilding the dropped-
        // resource queues so the first post-start horizon advance broadcasts
        // on_horizon_advanced and finishes the GC the pre-crash DROP missed.
        // Idempotent.
        void set_disk_has_dropped_sync(bool value) noexcept { disk_has_dropped_ = value; }
        void set_index_has_dropped_sync(bool value) noexcept { index_has_dropped_ = value; }

        unique_future<components::cursor::cursor_t_ptr> execute_plan(components::session::session_id_t session,
                                                                     components::logical_plan::execution_plan_t plan);

        // ===== pool-admin API =====
        // Every entry point below answers core::error_t, never a bare bool. These paths refuse
        // for reasons that are NOT interchangeable — a name that already exists, an overload
        // nobody holds, a cast source type the catalog never heard of, a renderer slot out of
        // range, a catalog write the disk turned down, an executor that would not drop what it
        // was told to drop — and a `false` erased every one of them, including the TYPED error
        // executor_t::register_udf already produces. no_error() means the operation happened.
        unique_future<core::error_t> register_udf(components::session::session_id_t session,
                                                  components::compute::function_ptr function);
        unique_future<core::error_t> unregister_udf(components::session::session_id_t session,
                                                    std::string function_name,
                                                    std::pmr::vector<components::types::complex_logical_type> inputs);
        // Fan a cast out to (register) / remove it from (unregister) every executor's
        // cast_registry_, then write / delete the pg_cast row via the operator. The catalog step
        // runs ONLY after every executor confirmed: an executor that kept applying a cast while
        // pg_cast said the cast was gone is the divergence these two guard.
        unique_future<core::error_t> register_cast(components::session::session_id_t session,
                                                   components::types::complex_logical_type source,
                                                   components::types::complex_logical_type target,
                                                   components::casts::cast_entry entry);
        unique_future<core::error_t> unregister_cast(components::session::session_id_t session,
                                                     components::types::complex_logical_type source,
                                                     components::types::complex_logical_type target);
        // Fan a host-supplied EXPLAIN renderer out to every executor, registering it at registry
        // slot `id` (each keeps its own POD fn-pointer copy — no shared state). Pool-admin
        // op, like register_udf. Per-query selection then rides execution_plan_t::explain_render_id.
        unique_future<core::error_t> set_explain_renderer(uint32_t id, services::collection::explain_render_fn fn);

        // ===== txn-state mailbox service =====
        // The ONLY way any other actor (executors, the txn operators running
        // inside them) reads or mutates transaction state. Every handler body
        // is a pure co_return over intra-actor txn_manager_ calls — none of
        // them awaits an executor (anti-deadlock invariant).

        // Session-context bundle fetched by the executor at plan start.
        // Unconditionally (and idempotently) begins the session's txn, so an
        // active txn exists before any operator runs — including the BEGIN
        // statement's own plan.
        unique_future<txn_session_context_t> txn_begin_session_msg(components::session::session_id_t session);
        // begin (idempotent) THEN mark_explicit — never a no-op on a missing
        // txn. Sent by operator_begin_transaction_t.
        unique_future<void> txn_mark_explicit_msg(components::session::session_id_t session);
        // Snapshot + drain every range parked on transaction_t + commit()
        // (allocates the commit_id, leaving it in in_flight_commits_).
        // INVARIANT: NO publish() here — the ProcArray barrier runs ONLY via
        // txn_publish_msg, sent by the caller AFTER storage_publish_* / WAL.
        unique_future<txn_commit_drain_t> txn_commit_drain_msg(components::session::session_id_t session);
        // Drain the pg_catalog appends needing revert + abort().
        unique_future<txn_abort_drain_t> txn_abort_drain_msg(components::session::session_id_t session);
        // Park executor-produced ranges on the session's transaction_t
        // (explicit-DML statements and the DDL swap-info merge — one message
        // for both; implicit DML never sends it).
        //
        // Answers core::error_t because the no-active-transaction case has nowhere else to go:
        // the payload is a whole statement's parked work — base insert and delete ranges, the
        // storage oids a CREATE brought up and a DROP retired, pg_catalog row ranges,
        // pg_attribute commit-id backfills — and parking it on a transaction that does not
        // exist is impossible. It used to be dropped on the floor behind a `void`.
        unique_future<core::error_t> txn_accumulate_msg(components::session::session_id_t session,
                                                        txn_accumulate_payload_t payload);
        // Abort: executor error-path (after its local revert cascade) and the
        // read-only release tail.
        unique_future<void> txn_abort_msg(components::session::session_id_t session);
        // ProcArray publish barrier — sent by operator_commit_transaction_t
        // AFTER storage_publish_* / index commits / WAL. Returns the
        // COMPACT-WATERMARK (txn_manager_.compact_watermark(), commit-id value
        // space): the visible-to-all horizon the maybe_cleanup fan-out hands to
        // data_table_t::compact(). Any version stamp above it (another txn's
        // snapshot, an in-flight commit) makes the compact a no-op.
        unique_future<uint64_t> txn_publish_msg(uint64_t commit_id);
        // THE OTHER END OF txn_publish_msg, for the commits that never reach it.
        // Sent by operator_commit_transaction_t from an early exit that already holds
        // an allocated commit_id: transaction_manager_t::discard() takes the id out of
        // in_flight_commits_ WITHOUT advancing published_horizon_, so the horizon stops
        // being floored at commit_id - 1 for the rest of the process and nothing
        // becomes visible in the process. The second half is not optional — the DROP-GC
        // broadcast is event-driven behind `new_lowest > last_broadcast_horizon_`, so
        // without try_trigger_cleanup_if_horizon_advanced() here the horizon would rise
        // with nobody told, and the deferred index-delete queue would go on waiting for
        // a broadcast that never comes.
        //
        // The operator has to be the sender: services/collection/executor.cpp lifts
        // pipeline_context.committed_id into the result only AFTER both of its error
        // breaks, so a failed commit reaches the dispatcher with commit_id 0 and no
        // dispatcher-side net can see the id at all.
        unique_future<void> txn_discard_msg(uint64_t commit_id);
        // Read-only fetch of txn_manager_.compact_watermark() for the
        // checkpoint/vacuum paths (operator_checkpoint, operator_vacuum and the
        // WAL auto-checkpoint), whose compact runs outside the commit pipeline.
        // Stale-safe: the watermark is monotone, an earlier value never
        // green-lights a compact a later value would refuse.
        unique_future<uint64_t> txn_compact_watermark_msg();

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
                                                            &manager_dispatcher_t::register_cast,
                                                            &manager_dispatcher_t::unregister_cast,
                                                            &manager_dispatcher_t::set_explain_renderer,
                                                            &manager_dispatcher_t::txn_begin_session_msg,
                                                            &manager_dispatcher_t::txn_mark_explicit_msg,
                                                            &manager_dispatcher_t::txn_commit_drain_msg,
                                                            &manager_dispatcher_t::txn_abort_drain_msg,
                                                            &manager_dispatcher_t::txn_accumulate_msg,
                                                            &manager_dispatcher_t::txn_abort_msg,
                                                            &manager_dispatcher_t::txn_publish_msg,
                                                            &manager_dispatcher_t::txn_discard_msg,
                                                            &manager_dispatcher_t::txn_compact_watermark_msg,
                                                            &manager_dispatcher_t::on_drop_resource_marked,
                                                            &manager_dispatcher_t::on_subscriber_empty>;

    private:
        // Reads txn_manager_.lowest_active_snapshot_horizon() (commit-id value
        // space — matches the subscribers' dropped_at_commit_id sweep after
        // the dropped-committed remap); if it advanced past
        // last_broadcast_horizon_, sends on_horizon_advanced(new) to each
        // subscriber whose drop-resource flag is set. Skips the send in the
        // common case (no drops outstanding), avoiding a message burst per commit.
        // Called from every txn-completing handler (publish/abort).
        void try_trigger_cleanup_if_horizon_advanced() noexcept;

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;

        // Host-injected customization, ctor-provided, forwarded to each executor.
        planner::create_plan_rule_t create_plan_rule_{&planner::no_custom_lowering};
        components::planner::optimizer_pass_t optimizer_pass_{&components::planner::no_op_pass};

        static constexpr std::size_t executor_pool_size_ = 4;

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
        components::casts::cast_registry_t cast_registry_;
        components::catalog::session_catalog_t default_tz_cat_;

        core::date::timezone_offset_t session_tz(components::session::session_id_t /*session*/) const {
            return default_tz_cat_.timezone_offset;
        }

        // Fire-and-forget unique_future<void> GC list. Loop-thread-private —
        // only the event loop appends (broadcast/register sends) and drains it
        // via poll_pending().
        std::pmr::vector<actor_zeta::unique_future<void>> pending_void_;

        void poll_pending();
    };

} // namespace services::dispatcher
