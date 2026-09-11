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
#include <unordered_map>

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

    // A future completed on another thread notifies nobody, so readiness is found only by polling.
#ifdef DEV_MODE
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

    // Thin router + txn-state mailbox service + executor-pool admin: per-query work lives entirely
    // in executor_t; the dispatcher owns only state that must stay global — txn_manager_ (reachable
    // solely through the txn_*_msg handlers below), default_settings_, the executor pool, DROP-GC flags.
    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
            uint32_t stale_ticks{0};
            // Crossing the threshold escalates the routine watchdog trace to a warning.
            uint32_t poke_rounds{0};
            bool waiting{false};
        };

        // The two host-customization hooks default to Null Objects, never null.
        manager_dispatcher_t(std::pmr::memory_resource*,
                             actor_zeta::scheduler_raw,
                             log_t& log,
                             actor_zeta::address_t wal_address,
                             actor_zeta::address_t disk_address,
                             actor_zeta::address_t index_address,
                             uint64_t dml_flush_row_threshold = 0,
                             planner::create_plan_rule_t create_plan_rule = &planner::no_custom_lowering,
                             components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass);
        ~manager_dispatcher_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        // Direct sync call, safe only because the scheduler has not started yet; idempotent.
        void seed_commit_clock_sync(uint64_t high_water);

        void cache_settings_sync(const components::catalog::session_catalog_t& settings);

        // Sync twin of on_drop_resource_marked(), for use before scheduler.start. Idempotent.
        void set_disk_has_dropped_sync(bool value) noexcept { disk_has_dropped_ = value; }
        void set_index_has_dropped_sync(bool value) noexcept { index_has_dropped_ = value; }

        unique_future<components::cursor::cursor_t_ptr> execute_plan(components::session::session_id_t session,
                                                                     components::logical_plan::execution_plan_t plan);

        unique_future<core::error_t> register_udf(components::session::session_id_t session,
                                                  components::compute::function_ptr function);
        unique_future<core::error_t> unregister_udf(components::session::session_id_t session,
                                                    std::string function_name,
                                                    std::pmr::vector<components::types::complex_logical_type> inputs);
        // pg_cast is written/deleted only after every executor confirms, so none applies a stale cast.
        unique_future<core::error_t> register_cast(components::session::session_id_t session,
                                                   components::types::complex_logical_type source,
                                                   components::types::complex_logical_type target,
                                                   components::casts::cast_entry entry);
        unique_future<core::error_t> unregister_cast(components::session::session_id_t session,
                                                     components::types::complex_logical_type source,
                                                     components::types::complex_logical_type target);
        // Fans a renderer to every executor at slot `id`; selection rides
        // execution_plan_t::explain_render_id.
        unique_future<core::error_t> set_explain_renderer(uint32_t id, services::collection::explain_render_fn fn);

        // txn-state mailbox service: the only way any other actor reads or mutates transaction state.

        // Each names the transaction its statement was resolved against
        unique_future<core::error_t> txn_mark_explicit_msg(components::session::session_id_t session,
                                                           uint64_t transaction_id);
        // Drains every parked range, then commit() allocates the commit_id into in_flight_commits_.
        unique_future<txn_commit_drain_t> txn_commit_drain_msg(components::session::session_id_t session,
                                                               uint64_t transaction_id);
        unique_future<txn_abort_drain_t> txn_abort_drain_msg(components::session::session_id_t session,
                                                             uint64_t transaction_id);
        // Answers core::error_t, not void, so a no-active-transaction refusal isn't silently dropped.
        unique_future<core::error_t> txn_accumulate_msg(components::session::session_id_t session,
                                                        uint64_t transaction_id,
                                                        txn_accumulate_payload_t payload);
        unique_future<void> txn_abort_msg(components::session::session_id_t session, uint64_t transaction_id);
        // Returns the compact watermark data_table_t::compact() treats as its visible-to-all horizon.
        unique_future<uint64_t> txn_publish_msg(uint64_t commit_id);
        // The other end of txn_publish_msg, for commits that never reach it. The operator must be
        // the sender — a failed commit reaches the dispatcher with commit_id 0, unrecoverable elsewhere.
        unique_future<void> txn_discard_msg(uint64_t commit_id);
        unique_future<uint64_t> txn_compact_watermark_msg();

        // DROP TABLE/INDEX marks the subscriber pending GC; on_subscriber_empty clears it when drained.
        unique_future<void> on_drop_resource_marked(uint8_t subscriber_kind);
        unique_future<void> on_subscriber_empty(uint8_t subscriber_kind);

        using dispatch_traits = actor_zeta::dispatch_traits<&manager_dispatcher_t::execute_plan,
                                                            &manager_dispatcher_t::register_udf,
                                                            &manager_dispatcher_t::unregister_udf,
                                                            &manager_dispatcher_t::register_cast,
                                                            &manager_dispatcher_t::unregister_cast,
                                                            &manager_dispatcher_t::set_explain_renderer,
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
        // Member coroutine, not a lambda, so `this` supplies the frame memory_resource.
        unique_future<void> unwind_udf_fanout_(
            components::session::session_id_t session,
            std::pmr::vector<std::pair<std::size_t, components::compute::function_uid>> registered);

        void try_trigger_cleanup_if_horizon_advanced() noexcept;

        std::size_t next_executor_index() noexcept;

        core::result_wrapper_t<txn_session_context_t>
        create_session_context(components::session::session_id_t session,
                               components::logical_plan::execution_plan_t* plan);

        unique_future<void> finish_failed_statement_(components::session::session_id_t session,
                                                     uint64_t transaction_id);
        unique_future<void> run_rollback_plan_(components::session::session_id_t session,
                                               components::table::transaction_data txn);

        txn_abort_drain_t drain_for_abort_(components::table::transaction_t& txn);

        components::table::transaction_t* statement_transaction_(components::session::session_id_t session,
                                                                 uint64_t transaction_id);

        struct waiting_statement_t {
            components::table::transaction_control_t control;
            actor_zeta::promise<void> admitted;
        };
        struct session_order_t {
            explicit session_order_t(std::pmr::memory_resource* resource)
                : waiting(resource) {}
            std::size_t running{0};
            bool closing{false};
            // list for pointer stability
            std::pmr::list<waiting_statement_t> waiting;
        };

        class session_turn_t {
        public:
            session_turn_t(manager_dispatcher_t* dispatcher,
                           components::session::session_id_t session,
                           components::table::transaction_control_t control)
                : dispatcher_(dispatcher)
                , session_(session)
                , control_(control) {}
            ~session_turn_t() { dispatcher_->end_turn_(session_, control_); }
            session_turn_t(const session_turn_t&) = delete;
            session_turn_t& operator=(const session_turn_t&) = delete;

        private:
            manager_dispatcher_t* dispatcher_;
            components::session::session_id_t session_;
            components::table::transaction_control_t control_;
        };

        unique_future<void> take_turn_(components::session::session_id_t session,
                                       components::table::transaction_control_t control);
        void end_turn_(components::session::session_id_t session, components::table::transaction_control_t control);
        bool may_start_(components::session::session_id_t session,
                        const session_order_t& order,
                        components::table::transaction_control_t control);
        void start_(session_order_t* order, components::table::transaction_control_t control);

        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;

        planner::create_plan_rule_t create_plan_rule_{&planner::no_custom_lowering};
        components::planner::optimizer_pass_t optimizer_pass_{&components::planner::no_op_pass};

        static constexpr std::size_t executor_pool_size_ = 4;

        std::pmr::vector<services::collection::executor::executor_ptr> executors_;
        std::pmr::vector<actor_zeta::address_t> executor_addresses_;
        std::size_t next_executor_{0};

        // Constructor arguments, never defaults. An empty wal_address_ means a test topology that
        // spawned no WAL manager, not a configuration a user can ask for.
        actor_zeta::address_t wal_address_;
        actor_zeta::address_t disk_address_;
        actor_zeta::address_t index_address_;

        bool disk_has_dropped_{false};
        bool index_has_dropped_{false};
        // A long-running concurrent txn can pin lowest_active, so this skips redundant re-broadcasts.
        uint64_t last_broadcast_horizon_{0};

        // mutex_/pump_cv_ guard only the loop's idle sleep, woken early by enqueue.
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        // Raw message* since boost::lockfree requires trivially-copyable; re-wrapped by the loop.
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_;
        std::condition_variable pump_cv_;

        components::table::transaction_manager_t txn_manager_;
        std::pmr::unordered_map<components::session::session_id_t, session_order_t> session_order_{resource_};
        in_flight_entry_t* current_entry_{nullptr};
        components::casts::cast_registry_t cast_registry_;
        // global cached settings. updated on every set.
        // TODO: settings for the session
        components::catalog::session_catalog_t default_settings_;

        const components::catalog::session_catalog_t&
        session_settings(components::session::session_id_t /*session*/) const {
            return default_settings_;
        }

        // Fire-and-forget GC list for broadcast/register sends, drained via poll_pending().
        std::pmr::vector<actor_zeta::unique_future<void>> pending_void_;

        void poll_pending();
    };

} // namespace services::dispatcher
