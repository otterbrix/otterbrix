#include "dispatcher.hpp"
#include <atomic>
#include <services/dev_pump.hpp>

#include <components/logical_plan/node_set_setting.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>

#include <services/collection/context_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <functional>

using namespace components::cursor;

namespace services::dispatcher {

    namespace {
        constexpr uint8_t DISK_KIND = 1;
        constexpr uint8_t INDEX_KIND = 2;
    } // namespace

    namespace {
        template<typename MethodList>
        struct behavior_expected_ids_t;

        template<auto... Ptrs>
        struct behavior_expected_ids_t<actor_zeta::type_traits::type_list<actor_zeta::method_map_entry<Ptrs>...>> {
            static constexpr std::array<actor_zeta::mailbox::message_id, sizeof...(Ptrs)> value{
                actor_zeta::msg_id<manager_dispatcher_t, Ptrs>...};
        };

        constexpr auto kImplementedIds = behavior_expected_ids_t<manager_dispatcher_t::dispatch_traits::methods>::value;

        constexpr std::array kBehaviorHandledIds{
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::refuse_statement>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::set_explain_renderer>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_mark_explicit_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_commit_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_accumulate_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_savepoint_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_release_savepoint_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_rollback_to_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_publish_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_discard_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_compact_watermark_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_drop_resource_marked>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_subscriber_empty>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::begin_shutdown>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_session>,
        };

        constexpr bool behavior_covers_all_implements() noexcept {
            if (kImplementedIds.size() != kBehaviorHandledIds.size())
                return false;
            for (auto id : kImplementedIds) {
                bool found = false;
                for (auto hid : kBehaviorHandledIds) {
                    if (id == hid) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
            return true;
        }

        static_assert(behavior_covers_all_implements(),
                      "behavior() is out of sync with dispatch_traits: "
                      "add a case to behavior() AND an entry to kBehaviorHandledIds");
    } // namespace

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log,
                                               actor_zeta::address_t wal_address,
                                               actor_zeta::address_t disk_address,
                                               actor_zeta::address_t index_address,
                                               const configuration::config_execution& execution,
                                               components::planner::primitives_t primitives,
                                               dispatcher_seed_t seed)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr)
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , pump_(execution.pump)
        , txn_manager_(resource_ptr)
        , function_registry_(resource_ptr)
        , pending_void_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");
        // Restores BOTH halves of the commit clock: raising only the horizon left post-reopen
        // INSERTs reusing already-published commit-ids (symptom: SSB q1-1 returned 0 rows on reopen).
        txn_manager_.restore_commit_clock(seed.commit_frontier);
        txn_manager_.seed_transaction_ids(seed.txn_id_high_water);
        trace(log_,
              "manager_dispatcher_t: commit frontier {} , transaction ids above {}",
              seed.commit_frontier,
              seed.txn_id_high_water);
        default_settings_ = std::move(seed.settings);
        disk_has_dropped_ = seed.disk_has_dropped;
        index_has_dropped_ = seed.index_has_dropped;
        components::compute::register_default_functions(function_registry_);

        assert(execution.executor_pool_size != 0 && "the engine factory refuses a zero executor pool before spawning");
        executors_.reserve(execution.executor_pool_size);
        executor_addresses_.reserve(execution.executor_pool_size);
        for (std::size_t i = 0; i < execution.executor_pool_size; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            log_.clone(),
                                                                            execution,
                                                                            primitives);
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_,
              "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses",
              execution.executor_pool_size);

        construction_error_ =
            loop_thread_.start<manager_dispatcher_t, &manager_dispatcher_t::loop_body>(resource_,
                                                                                       "manager_dispatcher",
                                                                                       this);
    }

    void manager_dispatcher_t::loop_body() {
        auto& in_flight = in_flight_;
        while (loop_running_.load(std::memory_order_acquire)) {
            actor_zeta::mailbox::message* raw = nullptr;
            while (inbox_.pop(raw)) {
                in_flight.emplace_back();
                in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
            }

            bool progress = true;
            while (progress) {
                progress = false;

                {
                    in_flight_entry_t* slot = nullptr;
                    for (auto& e : in_flight) {
                        if (e.pending_msg && !e.behavior) {
                            slot = &e;
                            break;
                        }
                    }
                    if (slot) {
                        current_entry_ = slot;
                        slot->behavior = behavior(slot->pending_msg.get());
                        current_entry_ = nullptr;
                        progress = true;
                        continue;
                    }
                }

                {
                    in_flight_entry_t* ready_slot = nullptr;
                    actor_zeta::detail::coroutine_handle<> cont{};
                    for (auto& e : in_flight) {
                        if (e.behavior.is_awaited_ready()) {
                            cont = e.behavior.take_awaited_continuation();
                            if (cont) {
                                ready_slot = &e;
                                break;
                            }
                        }
                    }
                    if (cont) {
                        current_entry_ = ready_slot;
                        cont.resume();
                        current_entry_ = nullptr;
                        poll_pending();
                        progress = true;
                        continue;
                    }
                }

                for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                    if (it->behavior && it->behavior.done()) {
                        in_flight.erase(it);
                        progress = true;
                        break;
                    }
                }

                poll_pending();
            }

#ifdef DEV_MODE
            const dev_pump_wait_t pump_probe{in_flight.empty()};
#endif
            std::unique_lock<std::mutex> lk(mutex_);
            pump_cv_.wait_for(lk, in_flight.empty() ? pump_.idle : pump_.in_flight, [this] {
                return !inbox_.empty() || !loop_running_.load(std::memory_order_acquire);
            });
        }
    }

    void manager_dispatcher_t::stop_loop() noexcept {
        loop_running_.store(false, std::memory_order_release);
        wake_loop_();
        loop_thread_.join();
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        stop_loop();
        in_flight_.clear();
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drop{raw};
        }
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        inbox_.push(msg.release());
        wake_loop_();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    // The mutex is taken between the push and the notify, so the loop either sees the message before
    // it sleeps or is already waiting when the notify comes.
    void manager_dispatcher_t::wake_loop_() noexcept {
        { std::lock_guard<std::mutex> guard(mutex_); }
        pump_cv_.notify_one();
    }

    void manager_dispatcher_t::poll_pending() {
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(), [](auto& f) { return f.is_ready(); }),
            pending_void_.end());
        // Every executor's registry is a copy of the master's: one that refuses what the master took has diverged.
        for (auto& ack : registry_acks_) {
            // A failed one was cancelled: the executor is gone.
            if (!ack.is_ready() || ack.failed()) {
                continue;
            }
            if (const auto refused = std::move(ack).take_ready(); refused.contains_error()) {
                std::fprintf(stderr, "dispatcher: an executor refused a committed change: %s\n", refused.what.c_str());
                assert(false && "dispatcher: an executor's registry diverged from the master");
                std::abort();
            }
        }
        registry_acks_.erase(std::remove_if(registry_acks_.begin(),
                                            registry_acks_.end(),
                                            [](auto& f) { return !f.valid() || f.is_ready(); }),
                             registry_acks_.end());
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::refuse_statement>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::refuse_statement, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::set_explain_renderer>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::set_explain_renderer, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_mark_explicit_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_mark_explicit_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_commit_drain_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_commit_drain_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_drain_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_abort_drain_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_accumulate_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_accumulate_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_savepoint_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_savepoint_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_release_savepoint_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_release_savepoint_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_rollback_to_drain_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_rollback_to_drain_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_publish_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_publish_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_discard_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_discard_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_compact_watermark_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_compact_watermark_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_drop_resource_marked>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::on_drop_resource_marked, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_subscriber_empty>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::on_subscriber_empty, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::begin_shutdown>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::begin_shutdown, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_session>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::close_session, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_dispatcher_t::try_trigger_cleanup_if_horizon_advanced() noexcept {
        // Must key off lowest_active_snapshot_horizon() (commit-id space), not lowest_active_start_time
        // — mixing the two left DROP-GC permanently dead.
        auto new_lowest = txn_manager_.lowest_active_snapshot_horizon();
        if (new_lowest > last_broadcast_horizon_) {
            last_broadcast_horizon_ = new_lowest;
            auto sweep_broadcast = [&] {
                if (disk_has_dropped_) {
                    auto disk_send_result =
                        actor_zeta::otterbrix::send(disk_address_,
                                                    &services::disk::manager_disk_t::on_horizon_advanced,
                                                    new_lowest);
                    pending_void_.emplace_back(std::move(disk_send_result.second));
                }
                if (index_has_dropped_ && index_address_ != actor_zeta::address_t::empty_address()) {
                    auto index_send_result =
                        actor_zeta::otterbrix::send(index_address_,
                                                    &services::index::manager_index_t::on_horizon_advanced,
                                                    new_lowest);
                    pending_void_.emplace_back(std::move(index_send_result.second));
                }
            };
            sweep_broadcast();
        }
    }

    // it is not the best approach, but a little bit more stable than pure session hashing
    std::size_t manager_dispatcher_t::next_executor_index() noexcept {
        assert(!executors_.empty());
        return next_executor_++ % executors_.size();
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::on_drop_resource_marked(uint8_t subscriber_kind) {
        if (subscriber_kind == DISK_KIND) {
            disk_has_dropped_ = true;
        } else if (subscriber_kind == INDEX_KIND) {
            index_has_dropped_ = true;
        }
        co_return;
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::begin_shutdown() {
        trace(log_, "manager_dispatcher_t::begin_shutdown");
        shutting_down_ = true;
        co_return;
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::on_subscriber_empty(uint8_t subscriber_kind) {
        if (subscriber_kind == DISK_KIND) {
            disk_has_dropped_ = false;
        } else if (subscriber_kind == INDEX_KIND) {
            index_has_dropped_ = false;
        }
        co_return;
    }

    namespace {
        bool describes(const components::logical_plan::execution_plan_t& plan) {
            return components::logical_plan::is_describe(plan.explain);
        }

        // PostgreSQL 18 IsTransactionExitStmt: COMMIT, ROLLBACK, ROLLBACK TO.
        bool ends_transaction(const components::logical_plan::node_t& root) {
            if (root.type() != components::logical_plan::node_type::transaction_t) {
                return false;
            }
            switch (static_cast<const components::logical_plan::node_transaction_t&>(root).op()) {
                case components::logical_plan::transaction_op::commit:
                case components::logical_plan::transaction_op::abort:
                case components::logical_plan::transaction_op::rollback_to_savepoint:
                    return true;
                default:
                    return false;
            }
        }

        components::table::transaction_control_t
        transaction_control_of(const components::logical_plan::execution_plan_t& plan) {
            const auto* root = plan.sub_queries.back().get();
            if (describes(plan) || root == nullptr ||
                root->type() != components::logical_plan::node_type::transaction_t) {
                return components::table::transaction_control_t::none;
            }
            switch (static_cast<const components::logical_plan::node_transaction_t*>(root)->op()) {
                case components::logical_plan::transaction_op::commit:
                    return components::table::transaction_control_t::commit;
                case components::logical_plan::transaction_op::abort:
                    return components::table::transaction_control_t::rollback;
                case components::logical_plan::transaction_op::savepoint:
                case components::logical_plan::transaction_op::release_savepoint:
                    return components::table::transaction_control_t::savepoint;
                case components::logical_plan::transaction_op::rollback_to_savepoint:
                    return components::table::transaction_control_t::rollback_to;
                case components::logical_plan::transaction_op::begin:
                    return components::table::transaction_control_t::none;
            }
            return components::table::transaction_control_t::none;
        }

        // The registries a statement sees are its session's: one that changes them runs alone, as the end of a
        // transaction does.
        bool changes_registries(const components::logical_plan::execution_plan_t& plan) {
            using components::logical_plan::node_type;
            const auto* root = plan.sub_queries.back().get();
            if (root == nullptr || describes(plan)) {
                return false;
            }
            switch (root->type()) {
                case node_type::register_udf_t:
                case node_type::unregister_udf_t:
                case node_type::register_cast_t:
                case node_type::unregister_cast_t:
                    return true;
                default:
                    return false;
            }
        }

        components::logical_plan::execution_plan_t make_rollback_plan(std::pmr::memory_resource* resource) {
            return components::logical_plan::execution_plan_t{
                resource,
                components::logical_plan::make_node_transaction(resource,
                                                                components::logical_plan::transaction_op::abort),
                components::logical_plan::make_parameter_node(resource)};
        }
    } // namespace

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       components::logical_plan::execution_plan_t plan) {
        if (log_.should_log(log_t::level::trace)) {
            trace(log_,
                  "manager_dispatcher_t::execute_plan session: {}, {}",
                  session.data(),
                  plan.sub_queries.back()->to_string());
        }

        assert(!executors_.empty());
        if (shutting_down_ && plan.sub_queries.back()->type() != components::logical_plan::node_type::checkpoint_t) {
            co_return stamp_transaction_status_(
                session,
                components::cursor::make_cursor(
                    resource(),
                    core::error_t{
                        core::error_code_t::connection_closed,
                        std::pmr::string{"the engine is shutting down: only CHECKPOINT is admitted", resource()}}));
        }
        const auto control = transaction_control_of(plan);
        const auto turn_of_plan = control != components::table::transaction_control_t::none || changes_registries(plan)
                                      ? turn_t::alone
                                      : turn_t::beside;
        const bool after_close = is_closed_(session);
        co_await take_turn_(session, turn_of_plan);
        session_turn_t turn{this, session, turn_of_plan};
        if (after_close) {
            co_return refuse_closed_(session);
        }
        co_return co_await run_statement_(session, control, std::move(plan));
    }

    core::error_t manager_dispatcher_t::failed_transaction_() const {
        return core::error_t{core::error_code_t::transaction_finalized,
                             std::pmr::string{"the transaction failed; only ROLLBACK, ROLLBACK TO SAVEPOINT or "
                                              "COMMIT (which rolls back) is accepted until it ends",
                                              resource()}};
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::run_statement_(components::session::session_id_t session,
                                         components::table::transaction_control_t control,
                                         components::logical_plan::execution_plan_t plan) {
        const auto settings_statement = settings_statement_of(plan);
        const bool failed_at_admission = failed_sessions_.contains(session);
        // A failed transaction's writes wait for its end; a COMMIT ends it as a ROLLBACK.
        bool commits_failed = control == components::table::transaction_control_t::commit && failed_at_admission;
        auto executed = commits_failed ? make_rollback_plan(resource()) : std::move(plan);
        auto resolved = create_session_context(session, &executed);
        if (resolved.has_error()) {
            co_return stamp_transaction_status_(session, components::cursor::make_cursor(resource(), resolved.error()));
        }
        auto session_ctx = std::move(resolved.value());
        if (executed.sub_queries.back()->type() == components::logical_plan::node_type::register_udf_t) {
            session_ctx.function_uid = function_registry_.reserve_uid();
        }
        // The end of a transaction or a savepoint calls no function.
        const auto own = control == components::table::transaction_control_t::none ? registry_changes_.find(session)
                                                                                   : registry_changes_.end();
        auto own_changes =
            own == registry_changes_.end() ? registry_changes_t{} : own->second.changes().copy(resource());
        const uint64_t statement_txn_id = session_ctx.txn.transaction_id;
        const std::size_t pool_idx = next_executor_index();
        trace(log_, "manager_dispatcher_t::run_statement_: routing to executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan_full,
                                                                 session,
                                                                 std::move(executed),
                                                                 std::move(session_ctx),
                                                                 std::move(own_changes));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto exec_result = co_await std::move(future);

        trace(log_,
              "manager_dispatcher_t::run_statement_: result received, success: {}",
              exec_result.cursor->is_success());
        if (exec_result.cursor && exec_result.cursor->is_error()) {
            co_await finish_failed_statement_(session, statement_txn_id);
        } else if (control == components::table::transaction_control_t::none && !failed_at_admission &&
                   failed_sessions_.contains(session)) {
            exec_result.cursor = components::cursor::make_cursor(
                resource(),
                core::error_t{core::error_code_t::transaction_finalized,
                              std::pmr::string{"the transaction failed while this statement ran", resource()}});
        } else if (control == components::table::transaction_control_t::rollback_to) {
            failed_sessions_.erase(session);
        } else {
            end_statement_transaction_(session, statement_txn_id);
        }
        finish_registry_changes_(session, settings_statement, exec_result);
        finish_settings_(session, settings_statement, commits_failed, exec_result);
        co_return stamp_transaction_status_(session, std::move(exec_result.cursor));
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::refuse_statement(components::session::session_id_t session, core::error_t error) {
        trace(log_, "manager_dispatcher_t::refuse_statement session: {}, {}", session.data(), error.what);
        const bool after_close = is_closed_(session);
        co_await take_turn_(session, turn_t::beside);
        session_turn_t turn{this, session, turn_t::beside};
        if (after_close) {
            co_return refuse_closed_(session);
        }
        if (const auto* txn = txn_manager_.find_transaction(session); txn != nullptr) {
            co_await finish_failed_statement_(session, txn->transaction_id());
        }
        co_return stamp_transaction_status_(session, components::cursor::make_cursor(resource(), std::move(error)));
    }

    bool manager_dispatcher_t::is_closed_(components::session::session_id_t session) const {
        const auto order = session_order_.find(session);
        return order != session_order_.end() && order->second.closed;
    }

    core::error_t manager_dispatcher_t::session_closed_() const {
        return core::error_t{core::error_code_t::connection_closed,
                             std::pmr::string{"the session is closed", resource()}};
    }

    components::cursor::cursor_t_ptr manager_dispatcher_t::refuse_closed_(components::session::session_id_t session) {
        return stamp_transaction_status_(session, components::cursor::make_cursor(resource(), session_closed_()));
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::close_session(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::close_session session: {}", session.data());
        const auto control = components::table::transaction_control_t::rollback;
        session_order_.try_emplace(session, resource()).first->second.closed = true;
        co_await take_turn_(session, turn_t::alone);
        session_turn_t turn{this, session, turn_t::alone};
        if (txn_manager_.find_transaction(session) != nullptr) {
            auto cursor = co_await run_statement_(session, control, make_rollback_plan(resource()));
            if (cursor->is_error()) {
                error(log_,
                      "manager_dispatcher_t::close_session: the rollback of session {} failed: {}",
                      session.data(),
                      cursor->get_error().what);
            }
            if (txn_manager_.find_transaction(session) != nullptr) {
                std::fprintf(stderr,
                             "dispatcher: session %llu still has a transaction after the ROLLBACK of its close\n",
                             static_cast<unsigned long long>(session.data()));
                assert(false && "dispatcher: a closed session still has a transaction");
                std::abort();
            }
        }
        session_settings_.erase(session);
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::set_explain_renderer(uint32_t id, services::collection::explain_render_fn fn) {
        std::pmr::vector<actor_zeta::unique_future<bool>> ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(executor_addresses_[i],
                                            &collection::executor::executor_t::set_explain_renderer,
                                            id,
                                            fn);
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(fut));
        }
        core::error_t fanout_error = core::error_t::no_error();
        for (std::size_t i = 0; i < ack_futures.size(); ++i) {
            const bool res = co_await std::move(ack_futures[i]);
            if (!res && !fanout_error.contains_error()) {
                fanout_error = core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"set_explain_renderer: executor " + std::to_string(i) + " of " +
                                         std::to_string(ack_futures.size()) + " refused renderer slot " +
                                         std::to_string(id) + " (slot id past the registry limit, or a null renderer)",
                                     resource()}};
            }
        }
        if (fanout_error.contains_error()) {
            error(log_, "dispatcher_t::set_explain_renderer: {}", fanout_error.what);
        }
        co_return fanout_error;
    }

    core::result_wrapper_t<txn_session_context_t>
    manager_dispatcher_t::create_session_context(components::session::session_id_t session,
                                                 components::logical_plan::execution_plan_t* plan) {
        if (failed_sessions_.contains(session)) {
            if (!ends_transaction(*plan->sub_queries.back())) {
                return failed_transaction_();
            }
            // A ROLLBACK TO clears the failure only once it has undone the work (run_statement_); a describe of an
            // end clears nothing.
            const auto control = transaction_control_of(*plan);
            if (control == components::table::transaction_control_t::commit ||
                control == components::table::transaction_control_t::rollback) {
                failed_sessions_.erase(session);
            }
        }
        const auto settings = session_settings(session);
        const auto statement = settings_statement_of(*plan);
        // An ALTER SYSTEM opens no implicit transaction of autocommit off (psql's command_no_begin): it runs alone,
        // or is refused inside the block already open. A describe opens none either: it reads the catalog only.
        const auto scope = settings.autocommit || statement.alter_system || describes(*plan)
                               ? components::table::transaction_scope_t::statement
                               : components::table::transaction_scope_t::until_commit;
        const bool opens = txn_manager_.find_transaction(session) == nullptr;
        auto& txn = txn_manager_.resolve_transaction(session, scope);
        plan->commits_when_done = txn.scope() == components::table::transaction_scope_t::statement;
        // The settings as the transaction found them: what ROLLBACK returns to.
        if (opens &&
            (txn.scope() == components::table::transaction_scope_t::until_commit ||
             (statement.is_transaction_control && statement.op == components::logical_plan::transaction_op::begin))) {
            session_settings_.try_emplace(session, resource()).first->second.begin_transaction();
        }
        txn_session_context_t context;
        context.txn = txn.data();
        context.settings = settings;
        context.lowest_active_start_time = txn_manager_.lowest_active_start_time();
        context.write_connection = txn.write_connection();
        trace(log_,
              "manager_dispatcher_t::create_session_context, session: {}, txn: {}, ends with the statement: {}",
              session.data(),
              context.txn.transaction_id,
              plan->commits_when_done);
        return context;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::finish_failed_statement_(components::session::session_id_t session, uint64_t transaction_id) {
        auto* txn = statement_transaction_(session, transaction_id);
        if (txn == nullptr) {
            co_return;
        }
        if (txn->scope() == components::table::transaction_scope_t::statement) {
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
            co_return;
        }
        // Undone only by the ROLLBACK or ROLLBACK TO SAVEPOINT that ends the failure (Postgres semantics).
        if (failed_sessions_.insert(session).second) {
            trace(log_,
                  "manager_dispatcher_t::finish_failed_statement_: txn {} failed, session: {}",
                  txn->transaction_id(),
                  session.data());
        }
        co_return;
    }

    components::catalog::session_catalog_t
    manager_dispatcher_t::session_settings(components::session::session_id_t session) const {
        const auto own = session_settings_.find(session);
        return own == session_settings_.end() ? default_settings_ : own->second.effective(default_settings_);
    }

    components::catalog::place_t manager_dispatcher_t::place_of(components::session::session_id_t session) const {
        return components::catalog::place_of(session_settings(session).search_path);
    }

    manager_dispatcher_t::settings_statement_t
    manager_dispatcher_t::settings_statement_of(const components::logical_plan::execution_plan_t& plan) const {
        settings_statement_t statement{false, {}, std::pmr::string{resource()}, false};
        const auto* root = plan.sub_queries.back().get();
        if (root == nullptr || describes(plan)) {
            return statement;
        }
        if (root->type() == components::logical_plan::node_type::transaction_t) {
            const auto* control = static_cast<const components::logical_plan::node_transaction_t*>(root);
            statement.is_transaction_control = true;
            statement.op = control->op();
            statement.savepoint = control->savepoint_name();
        } else if (root->type() == components::logical_plan::node_type::set_setting_t) {
            statement.alter_system = static_cast<const components::logical_plan::node_set_setting_t*>(root)->scope() ==
                                     components::catalog::setting_scope_t::system;
        }
        return statement;
    }

    void manager_dispatcher_t::finish_settings_(components::session::session_id_t session,
                                                const settings_statement_t& statement,
                                                bool commit_rolled_back,
                                                const collection::executor::execute_result_t& result) {
        using components::logical_plan::transaction_op;
        const bool succeeded = result.cursor && result.cursor->is_success();
        if (succeeded && !result.applied_settings.empty()) {
            if (result.applied_setting_scope == components::catalog::setting_scope_t::system) {
                for (const auto& assignment : result.applied_settings) {
                    if (auto applied = components::catalog::set_setting(default_settings_,
                                                                         assignment.id,
                                                                         assignment.value,
                                                                         resource());
                        applied.contains_error()) {
                        std::fprintf(stderr,
                                     "dispatcher: ALTER SYSTEM reported a value its setting refuses: %s\n",
                                     applied.what.c_str());
                        assert(false && "dispatcher: ALTER SYSTEM reported a value its setting refuses");
                        std::abort();
                    }
                }
            } else {
                auto& own = session_settings_.try_emplace(session, resource()).first->second;
                for (const auto& assignment : result.applied_settings) {
                    if (assignment.is_default) {
                        own.reset(result.applied_setting_scope, assignment.id);
                    } else {
                        own.assign(result.applied_setting_scope, assignment.id, assignment.value);
                    }
                }
            }
        }
        const auto own = session_settings_.find(session);
        if (own == session_settings_.end()) {
            return;
        }
        if (succeeded && statement.is_transaction_control) {
            switch (statement.op) {
                case transaction_op::savepoint:
                    own->second.savepoint(statement.savepoint);
                    break;
                case transaction_op::release_savepoint:
                    own->second.release_savepoint(statement.savepoint);
                    break;
                case transaction_op::rollback_to_savepoint:
                    own->second.rollback_to_savepoint(statement.savepoint);
                    break;
                case transaction_op::begin:
                case transaction_op::commit:
                case transaction_op::abort:
                    break;
            }
        }
        if (txn_manager_.find_transaction(session) == nullptr) {
            own->second.end_transaction(succeeded && statement.is_transaction_control &&
                                        statement.op == transaction_op::commit && !commit_rolled_back);
        }
        if (own->second.empty()) {
            session_settings_.erase(own);
        }
    }

    void manager_dispatcher_t::finish_registry_changes_(components::session::session_id_t session,
                                                        const settings_statement_t& statement,
                                                        const collection::executor::execute_result_t& result) {
        using components::logical_plan::transaction_op;
        const bool succeeded = result.cursor && result.cursor->is_success();
        auto pending = registry_changes_.find(session);
        if (succeeded && statement.is_transaction_control && statement.op == transaction_op::savepoint) {
            pending = registry_changes_.try_emplace(session).first;
        }
        if (pending == registry_changes_.end()) {
            return;
        }
        if (succeeded && statement.is_transaction_control) {
            switch (statement.op) {
                case transaction_op::savepoint:
                    pending->second.savepoint(statement.savepoint);
                    break;
                case transaction_op::release_savepoint:
                    pending->second.release_savepoint(statement.savepoint);
                    break;
                case transaction_op::rollback_to_savepoint:
                    pending->second.rollback_to_savepoint(statement.savepoint);
                    break;
                case transaction_op::begin:
                case transaction_op::commit:
                case transaction_op::abort:
                    break;
            }
        }
        // A commit took its changes at its drain: what is left of an ended transaction was not committed.
        if (txn_manager_.find_transaction(session) == nullptr) {
            registry_changes_.erase(pending);
        }
    }

    registry_changes_t manager_dispatcher_t::take_registry_changes_(components::session::session_id_t session) {
        const auto pending = registry_changes_.find(session);
        if (pending == registry_changes_.end()) {
            return registry_changes_t{};
        }
        auto changes = pending->second.take();
        registry_changes_.erase(pending);
        return changes;
    }

    void manager_dispatcher_t::publish_registry_changes_(registry_changes_t changes) {
        if (auto refused = changes.apply_to(function_registry_, nullptr, resource()); refused.contains_error()) {
            std::fprintf(stderr,
                         "dispatcher: the master registry refused a committed change: %s\n",
                         refused.what.c_str());
            assert(false && "dispatcher: the master registry refused a committed change");
            std::abort();
        }
        // Sent before anything else this loop step sends: every statement routed after it finds the change applied on
        // its executor, whose mailbox keeps the order.
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [needs_sched, ack] =
                actor_zeta::otterbrix::send(executor_addresses_[i],
                                            &collection::executor::executor_t::apply_registry_changes,
                                            changes.copy(resource()));
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            registry_acks_.push_back(std::move(ack));
        }
    }

    void manager_dispatcher_t::end_statement_transaction_(components::session::session_id_t session,
                                                          uint64_t transaction_id) {
        auto* txn = statement_transaction_(session, transaction_id);
        if (txn == nullptr || txn->scope() != components::table::transaction_scope_t::statement) {
            return;
        }
        if (txn->has_accumulated()) {
            std::fprintf(stderr,
                         "dispatcher: the statement succeeded, but its transaction %llu (session %llu) still holds "
                         "uncommitted work: the commit pipeline was skipped\n",
                         static_cast<unsigned long long>(txn->transaction_id()),
                         static_cast<unsigned long long>(session.data()));
            assert(false && "dispatcher: a statement transaction still holds uncommitted work");
            std::abort();
        }
        txn_manager_.abort(session);
        try_trigger_cleanup_if_horizon_advanced();
    }

    components::cursor::cursor_t_ptr
    manager_dispatcher_t::stamp_transaction_status_(components::session::session_id_t session,
                                                    components::cursor::cursor_t_ptr cursor) {
        assert(cursor);
        if (failed_sessions_.contains(session)) {
            cursor->set_transaction_status(components::session::transaction_status_t::failed);
        } else if (const auto* txn = txn_manager_.find_transaction(session);
                   txn != nullptr && txn->scope() == components::table::transaction_scope_t::until_commit) {
            cursor->set_transaction_status(components::session::transaction_status_t::in_transaction);
        } else {
            cursor->set_transaction_status(components::session::transaction_status_t::idle);
        }
        return cursor;
    }

    components::table::transaction_t*
    manager_dispatcher_t::statement_transaction_(components::session::session_id_t session,
                                                 [[maybe_unused]] uint64_t transaction_id) {
        auto* txn = txn_manager_.find_transaction(session);
        assert(txn == nullptr || txn->transaction_id() == transaction_id);
        return txn;
    }

#ifdef DEV_MODE
    namespace {
        std::atomic<std::uint64_t> g_statements_waited_turn{0};
    } // namespace

    std::uint64_t dev_statements_waited_turn() noexcept { return g_statements_waited_turn.load(); }
#endif

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::take_turn_(components::session::session_id_t session, turn_t turn) {
        auto& order = session_order_.try_emplace(session, resource()).first->second;
        if (order.waiting.empty() && may_start_(session, order, turn)) {
            start_(&order, turn);
            co_return;
        }
        trace(log_, "manager_dispatcher_t::take_turn_: session {} waits for its running transaction", session.data());
#ifdef DEV_MODE
        g_statements_waited_turn.fetch_add(1);
#endif
        actor_zeta::promise<void> admitted(resource());
        auto admission = admitted.get_future();
        order.waiting.push_back(waiting_statement_t{turn, std::move(admitted)});
        // The entry outlives this coroutine, and only the statement running in it touches the flag.
        in_flight_entry_t* entry = current_entry_;
        if (entry != nullptr) {
            entry->waiting = true;
        }
        co_await std::move(admission);
        if (entry != nullptr) {
            entry->waiting = false;
        }
    }

    bool manager_dispatcher_t::may_start_(components::session::session_id_t session,
                                          const session_order_t& order,
                                          turn_t turn) {
        if (order.closing) {
            return false;
        }
        if (order.running == 0) {
            return true;
        }
        const auto* txn = txn_manager_.find_transaction(session);
        // Running statements with no open transaction to join are still ending the one before.
        if (txn == nullptr || txn->scope() == components::table::transaction_scope_t::statement) {
            return false;
        }
        return turn == turn_t::beside;
    }

    void manager_dispatcher_t::start_(session_order_t* order, turn_t turn) {
        ++order->running;
        if (turn == turn_t::alone) {
            order->closing = true;
        }
    }

    void manager_dispatcher_t::end_turn_(components::session::session_id_t session, turn_t turn) {
        auto it = session_order_.find(session);
        assert(it != session_order_.end() && it->second.running > 0);
        auto& order = it->second;
        --order.running;
        if (turn == turn_t::alone) {
            order.closing = false;
        }
        while (!order.waiting.empty() && may_start_(session, order, order.waiting.front().turn)) {
            auto next = std::move(order.waiting.front());
            order.waiting.pop_front();
            start_(&order, next.turn);
            next.admitted.set_value();
        }
        if (order.running == 0 && order.waiting.empty()) {
            session_order_.erase(it);
        }
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::txn_mark_explicit_msg(components::session::session_id_t session, uint64_t transaction_id) {
        trace(log_,
              "manager_dispatcher_t::txn_mark_explicit_msg, session: {}, txn: {}",
              session.data(),
              transaction_id);
        auto* txn = statement_transaction_(session, transaction_id);
        if (txn == nullptr) {
            co_return core::error_t{
                core::error_code_t::internal_error,
                std::pmr::string{"BEGIN: its transaction ended before BEGIN reached it", resource()}};
        }
        txn->keep_until_commit();
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<txn_commit_drain_t>
    manager_dispatcher_t::txn_commit_drain_msg(components::session::session_id_t session, uint64_t transaction_id) {
        trace(log_, "manager_dispatcher_t::txn_commit_drain_msg, session: {}", session.data());
        txn_commit_drain_t out;
        auto* txn_t = statement_transaction_(session, transaction_id);
        assert(txn_t != nullptr);
        auto committing = take_registry_changes_(session);
        if (!txn_t->has_accumulated()) {
            // Empty COMMIT aborts instead of committing: it must not allocate a commit_id or advance the horizon. The
            // storages it read still get its end. With no catalog row to publish, its registrations go now.
            out.txn = txn_t->data();
            out.participants = txn_t->drain_participants();
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
            if (!committing.empty()) {
                publish_registry_changes_(std::move(committing));
            }
            co_return out;
        }
        out.txn = txn_t->data();
        txn_t->drain_pg_catalog_pending(out.swap_appends, out.swap_deletes);
        out.swap_backfills = txn_t->drain_pg_attribute_commit_id_backfills();
        auto drained_appends = txn_t->drain_base_appends();
        out.base_appends.reserve(drained_appends.size());
        for (const auto& r : drained_appends) {
            out.base_appends.push_back(components::pg_catalog_append_range_t{r.table_oid, r.row_start, r.row_count});
        }
        auto drained_deletes = txn_t->drain_base_deletes();
        for (const auto& d : drained_deletes) {
            out.base_delete_tables.insert(d.table_oid);
        }
        out.dropped_storage_oids = txn_t->drain_dropped_storages();
        out.created_storage_oids = txn_t->drain_created_storages();
        out.created_indexes = txn_t->drain_created_indexes();
        out.referenced_deletes = txn_t->drain_referenced_deletes();
        out.participants = txn_t->drain_participants();
        // No publish barrier here — txn_publish_msg runs it after storage/WAL, so no snapshot sees it half-flipped.
        out.commit_id = txn_manager_.commit(session, &out.commit_view);
        if (!committing.empty()) {
            committing_registry_changes_.insert_or_assign(out.commit_id, std::move(committing));
        }
        co_return out;
    }

    components::table::txn_abort_drain_t manager_dispatcher_t::drain_for_abort_(components::table::transaction_t& txn) {
        components::table::txn_abort_drain_t out;
        out.txn = txn.data();
        txn.drain_pg_catalog_pending(out.swap_appends, out.pg_catalog_delete_tables);
        for (const auto& backfill : txn.drain_pg_attribute_commit_id_backfills()) {
            if (const auto table_oid = backfill.column_stamped_table(); table_oid != components::catalog::INVALID_OID) {
                out.column_stamped_tables.insert(table_oid);
            }
        }
        auto drained_appends = txn.drain_base_appends();
        out.base_appends.reserve(drained_appends.size());
        for (const auto& r : drained_appends) {
            out.base_append_tables.insert(r.table_oid);
            out.base_appends.push_back(components::pg_catalog_append_range_t{r.table_oid, r.row_start, r.row_count});
        }
        auto drained_deletes = txn.drain_base_deletes();
        for (const auto& d : drained_deletes) {
            out.base_delete_tables.insert(d.table_oid);
        }
        // DROP-retired storage oids are informational today — the abort operator does not yet un-stamp them.
        out.dropped_storage_oids = txn.drain_dropped_storages();
        out.created_storage_oids = txn.drain_created_storages();
        out.created_indexes = txn.drain_created_indexes();
        out.participants = txn.drain_participants();
        return out;
    }

    manager_dispatcher_t::unique_future<components::table::txn_abort_drain_t>
    manager_dispatcher_t::txn_abort_drain_msg(components::session::session_id_t session, uint64_t transaction_id) {
        trace(log_, "manager_dispatcher_t::txn_abort_drain_msg, session: {}", session.data());
        auto* txn_t = statement_transaction_(session, transaction_id);
        assert(txn_t != nullptr);
        auto out = drain_for_abort_(*txn_t);
        txn_manager_.abort(session);
        try_trigger_cleanup_if_horizon_advanced();
        co_return out;
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::txn_accumulate_msg(components::session::session_id_t session,
                                             uint64_t transaction_id,
                                             txn_accumulate_payload_t payload) {
        trace(log_, "manager_dispatcher_t::txn_accumulate_msg, session: {}", session.data());
        auto* txn_t = statement_transaction_(session, transaction_id);
        if (txn_t == nullptr) {
            error(log_,
                  "manager_dispatcher_t::txn_accumulate_msg: session {} has no active transaction; refusing to park "
                  "{} base appends, {} base deletes, {} pg_catalog appends, {} pg_catalog delete-tables, {} "
                  "backfills, {} dropped storages, {} created storages, {} created indexes",
                  session.data(),
                  payload.base_appends.size(),
                  payload.base_deletes.size(),
                  payload.pg_catalog_appends.size(),
                  payload.pg_catalog_delete_tables.size(),
                  payload.backfills.size(),
                  payload.dropped_storage_oids.size(),
                  payload.created_storage_oids.size(),
                  payload.created_indexes.size());
            co_return core::error_t{
                core::error_code_t::internal_error,
                std::pmr::string{"txn_accumulate_msg: the session has no active transaction, so the statement's "
                                 "accumulated ranges cannot be parked",
                                 resource()}};
        }
        for (const auto& app : payload.base_appends) {
            txn_t->accumulate_base_append(app);
        }
        for (const auto& del : payload.base_deletes) {
            txn_t->accumulate_base_delete(del);
        }
        txn_t->accumulate_pg_catalog_pending(std::move(payload.pg_catalog_appends),
                                             std::move(payload.pg_catalog_delete_tables));
        txn_t->accumulate_pg_attribute_commit_id_backfills(std::move(payload.backfills));
        for (auto oid : payload.dropped_storage_oids) {
            txn_t->accumulate_dropped_storage(oid);
        }
        for (auto oid : payload.created_storage_oids) {
            txn_t->accumulate_created_storage(oid);
        }
        for (auto& index : payload.created_indexes) {
            txn_t->accumulate_created_index(std::move(index));
        }
        for (auto& rows : payload.referenced_deletes) {
            txn_t->accumulate_referenced_delete(std::move(rows));
        }
        for (const auto& participant : payload.participants) {
            txn_t->accumulate_participant(participant);
        }
        if (!payload.registry_changes.empty()) {
            registry_changes_.try_emplace(session).first->second.add(std::move(payload.registry_changes));
        }
        co_return core::error_t::no_error();
    }

    core::result_wrapper_t<components::table::transaction_t*>
    manager_dispatcher_t::savepoint_transaction_(components::session::session_id_t session,
                                                 uint64_t transaction_id,
                                                 std::string_view statement) {
        auto* txn = statement_transaction_(session, transaction_id);
        if (txn == nullptr || txn->scope() == components::table::transaction_scope_t::statement) {
            std::pmr::string what{statement, resource()};
            what += " can only be used in transaction blocks";
            return core::error_t{core::error_code_t::no_active_sql_transaction, std::move(what)};
        }
        return txn;
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::txn_savepoint_msg(components::session::session_id_t session,
                                            uint64_t transaction_id,
                                            std::string name) {
        trace(log_, "manager_dispatcher_t::txn_savepoint_msg, session: {}, savepoint: {}", session.data(), name);
        auto txn = savepoint_transaction_(session, transaction_id, "SAVEPOINT");
        if (txn.has_error()) {
            co_return txn.error();
        }
        txn.value()->push_savepoint(name);
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::txn_release_savepoint_msg(components::session::session_id_t session,
                                                    uint64_t transaction_id,
                                                    std::string name) {
        trace(log_,
              "manager_dispatcher_t::txn_release_savepoint_msg, session: {}, savepoint: {}",
              session.data(),
              name);
        auto txn = savepoint_transaction_(session, transaction_id, "RELEASE SAVEPOINT");
        if (txn.has_error()) {
            co_return txn.error();
        }
        if (!txn.value()->release_savepoint(name)) {
            co_return core::error_t{core::error_code_t::missing_savepoint,
                                    std::pmr::string{"savepoint \"" + name + "\" does not exist", resource()}};
        }
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<txn_rollback_to_drain_t>
    manager_dispatcher_t::txn_rollback_to_drain_msg(components::session::session_id_t session,
                                                    uint64_t transaction_id,
                                                    std::string name) {
        trace(log_,
              "manager_dispatcher_t::txn_rollback_to_drain_msg, session: {}, savepoint: {}",
              session.data(),
              name);
        txn_rollback_to_drain_t out;
        auto txn = savepoint_transaction_(session, transaction_id, "ROLLBACK TO SAVEPOINT");
        if (txn.has_error()) {
            out.error = txn.error();
            co_return out;
        }
        if (!txn.value()->rollback_to_savepoint(name, &out.drain)) {
            out.error = core::error_t{core::error_code_t::missing_savepoint,
                                      std::pmr::string{"savepoint \"" + name + "\" does not exist", resource()}};
        }
        co_return out;
    }

    manager_dispatcher_t::unique_future<uint64_t> manager_dispatcher_t::txn_publish_msg(uint64_t commit_id) {
        trace(log_, "manager_dispatcher_t::txn_publish_msg, commit_id: {}", commit_id);
        // The registries take the commit's changes before any statement can see its catalog rows.
        if (auto committing = committing_registry_changes_.find(commit_id);
            committing != committing_registry_changes_.end()) {
            auto changes = std::move(committing->second);
            committing_registry_changes_.erase(committing);
            publish_registry_changes_(std::move(changes));
        }
        txn_manager_.publish(commit_id);
        try_trigger_cleanup_if_horizon_advanced();
        co_return txn_manager_.compact_watermark();
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::txn_discard_msg(uint64_t commit_id) {
        trace(log_, "manager_dispatcher_t::txn_discard_msg, commit_id: {}", commit_id);
        committing_registry_changes_.erase(commit_id);
        txn_manager_.discard(commit_id);
        try_trigger_cleanup_if_horizon_advanced();
        co_return;
    }

    manager_dispatcher_t::unique_future<uint64_t> manager_dispatcher_t::txn_compact_watermark_msg() {
        const auto watermark = txn_manager_.compact_watermark();
        trace(log_, "manager_dispatcher_t::txn_compact_watermark_msg, watermark: {}", watermark);
        co_return watermark;
    }
} // namespace services::dispatcher
