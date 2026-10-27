#include "dispatcher.hpp"
#include <atomic>
#include <services/dev_pump.hpp>

#include <components/casts/default_casts.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_setting.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_register_cast.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_resolve_namespace.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_cast.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_udf.hpp>
#include <components/sql/transformer/utils.hpp>
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
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_cast>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_cast>,
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
        , cast_registry_(resource_ptr)
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
        components::casts::register_default_casts(cast_registry_);
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
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::unregister_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_cast>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::register_cast, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_cast>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::unregister_cast, msg);
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
        components::table::transaction_control_t
        transaction_control_of(const components::logical_plan::execution_plan_t& plan) {
            const auto* root = plan.sub_queries.back().get();
            if (root == nullptr || root->type() != components::logical_plan::node_type::transaction_t) {
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
        const auto settings_statement = settings_statement_of(plan);
        co_await take_turn_(session, control);
        session_turn_t turn{this, session, control};
        // A failed transaction's writes wait for its end; a COMMIT ends it as a ROLLBACK.
        bool commits_failed =
            control == components::table::transaction_control_t::commit && failed_sessions_.contains(session);
        auto executed = commits_failed ? make_rollback_plan(resource()) : std::move(plan);
        auto resolved = create_session_context(session, &executed);
        if (resolved.has_error()) {
            co_return stamp_transaction_status_(session, components::cursor::make_cursor(resource(), resolved.error()));
        }
        auto session_ctx = std::move(resolved.value());
        const uint64_t statement_txn_id = session_ctx.txn.transaction_id;
        const std::size_t pool_idx = next_executor_index();
        trace(log_, "manager_dispatcher_t::execute_plan: routing to executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan_full,
                                                                 session,
                                                                 std::move(executed),
                                                                 std::move(session_ctx));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto exec_result = co_await std::move(future);

        trace(log_,
              "manager_dispatcher_t::execute_plan: result received, success: {}",
              exec_result.cursor->is_success());
        if (exec_result.cursor && exec_result.cursor->is_error()) {
            co_await finish_failed_statement_(session, statement_txn_id);
        } else if (control == components::table::transaction_control_t::none && failed_sessions_.contains(session)) {
            exec_result.cursor = components::cursor::make_cursor(
                resource(),
                core::error_t{core::error_code_t::transaction_finalized,
                              std::pmr::string{"the transaction failed while this statement ran", resource()}});
        } else if (control == components::table::transaction_control_t::rollback_to) {
            failed_sessions_.erase(session);
        } else {
            end_statement_transaction_(session, statement_txn_id);
        }
        finish_settings_(session, settings_statement, commits_failed, exec_result);
        co_return stamp_transaction_status_(session, std::move(exec_result.cursor));
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::refuse_statement(components::session::session_id_t session, core::error_t error) {
        trace(log_, "manager_dispatcher_t::refuse_statement session: {}, {}", session.data(), error.what);
        const auto control = components::table::transaction_control_t::none;
        co_await take_turn_(session, control);
        session_turn_t turn{this, session, control};
        if (const auto* txn = txn_manager_.find_transaction(session); txn != nullptr) {
            co_await finish_failed_statement_(session, txn->transaction_id());
        }
        co_return stamp_transaction_status_(session, components::cursor::make_cursor(resource(), std::move(error)));
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        if (!function) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"register_udf: no function to register", resource()}};
        }
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());

        auto schema = co_await place_schema_of_(session);
        if (schema.has_error()) {
            co_return schema.error();
        }
        const auto schema_oid = schema.value();

        auto plan =
            boost::intrusive_ptr(new components::logical_plan::node_register_udf_t(resource(), std::move(function)));

        components::operators::operator_register_udf_t::executor_uids_t executor_uids(resource());
        executor_uids.reserve(executor_addresses_.size());
        std::pmr::vector<actor_zeta::unique_future<std::unique_ptr<collection::executor::function_result_t>>>
            ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(executor_addresses_[i],
                                                                  &collection::executor::executor_t::register_udf,
                                                                  session,
                                                                  plan->function()->get_copy(resource()),
                                                                  schema_oid);
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(fut));
        }
        core::error_t fanout_error = core::error_t::no_error();
        std::pmr::vector<std::pair<std::size_t, components::compute::function_uid>> registered(resource());
        registered.reserve(ack_futures.size());
        for (std::size_t i = 0; i < ack_futures.size(); ++i) {
            auto res = co_await std::move(ack_futures[i]);
            if (!res) {
                if (!fanout_error.contains_error()) {
                    fanout_error = core::error_t{core::error_code_t::function_registry_error,
                                                 std::pmr::string{"register_udf: executor " + std::to_string(i) +
                                                                      " of " + std::to_string(ack_futures.size()) +
                                                                      " returned no registration result",
                                                                  resource()}};
                }
                continue;
            }
            if (res->has_error()) {
                if (!fanout_error.contains_error()) {
                    fanout_error = res->error();
                }
                continue;
            }
            executor_uids.push_back(res->value());
            registered.emplace_back(i, res->value());
        }
        if (fanout_error.contains_error()) {
            error(log_, "dispatcher_t::register_udf: executor fan-out refused: {}", fanout_error.what);
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return fanout_error;
        }

        auto master_copy = plan->function()->get_copy(resource());
        const auto uid = executor_uids.front();
        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        auto op = services::planner::impl::create_plan_register_udf(cstor, plan, std::move(executor_uids), schema_oid);
        if (auto op_error = co_await run_catalog_op_(session, std::move(op), "register_udf");
            op_error.contains_error()) {
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return op_error;
        }
        if (auto added = function_registry_.add_function_with_uid(uid, std::move(master_copy)); added.has_error()) {
            error(log_, "dispatcher_t::register_udf: the master registry refused: {}", added.error().what);
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return added.error();
        }
        function_registry_.set_schema(uid, schema_oid);
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::run_catalog_op_(components::session::session_id_t session,
                                          components::operators::operator_ptr op,
                                          std::string_view what) {
        op->set_as_root();
        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &function_registry_,
                                             params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = components::table::transaction_data::committed();
        pctx.place = place_of(session);
        op->prepare();
        co_await op->await_async_and_resume(&pctx);
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }
        if (op->has_error()) {
            error(log_, "dispatcher_t::{}: {}", what, op->get_error().what);
            co_return op->get_error();
        }
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
    manager_dispatcher_t::place_schema_of_(components::session::session_id_t session) {
        components::logical_plan::catalog_resolves_t resolves;
        resolves.place = place_of(session);
        resolves.ensure(resource(), components::logical_plan::resolve_kind::namespace_)
            .add(components::logical_plan::resolve_entry_t{});
        auto op =
            boost::intrusive_ptr(new components::operators::operator_resolve_namespace_t(resource(),
                                                                                         log_.clone(),
                                                                                         resolves.namespaces.get()));
        if (auto failed = co_await run_catalog_op_(session, std::move(op), "place schema"); failed.contains_error()) {
            co_return failed;
        }
        if (auto missing = check_namespace_exists(resource(), &resolves, qualified_name_t{});
            missing.contains_error()) {
            co_return missing;
        }
        if (!resolves.place.connection.empty()) {
            std::pmr::string msg{"only tables can be created in a schema of connection \"", resource()};
            msg.append(resolves.place.connection);
            msg.append("\"");
            co_return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
        }
        co_return resolves.namespaces->entries().front().namespace_oid;
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::unwind_udf_fanout_(
        components::session::session_id_t session,
        std::pmr::vector<std::pair<std::size_t, components::compute::function_uid>> registered) {
        std::pmr::vector<actor_zeta::unique_future<bool>> acks(resource());
        acks.reserve(registered.size());
        for (const auto& [idx, uid] : registered) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(executor_addresses_[idx],
                                                                  &collection::executor::executor_t::unregister_udf_uid,
                                                                  session,
                                                                  uid);
            if (needs_sched && executors_[idx]) {
                scheduler_->enqueue(executors_[idx].get());
            }
            acks.push_back(std::move(fut));
        }
        for (std::size_t k = 0; k < acks.size(); ++k) {
            const bool dropped = co_await std::move(acks[k]);
            if (!dropped) {
                error(log_,
                      "dispatcher_t::register_udf unwind: executor {} did not hold uid {} it had just answered",
                      registered[k].first,
                      registered[k].second);
            }
        }
        co_return;
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

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<components::types::complex_logical_type> inputs,
                                         components::catalog::drop_behavior_t behavior) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);

        // A builtin is required by the system, as in PostgreSQL, which refuses to drop one.
        const auto named = function_registry_.find_functions(function_name);
        const bool builtin = std::any_of(named.begin(), named.end(), [&](components::compute::function_uid uid) {
            const auto* function = function_registry_.get_function(uid);
            if (!components::compute::is_builtin(uid) || function == nullptr) {
                return false;
            }
            const auto signatures = function->get_signatures();
            return std::any_of(signatures.begin(), signatures.end(), [&inputs](const auto& signature) {
                return signature.matches_inputs(inputs);
            });
        });
        if (builtin) {
            core::error_t refused{core::error_code_t::invalid_parameter,
                                  std::pmr::string{"unregister_udf: '" + function_name +
                                                       "' is a builtin function and cannot be unregistered",
                                                   resource()}};
            error(log_, "dispatcher_t::unregister_udf: {}", refused.what);
            co_return refused;
        }
        auto schema = co_await place_schema_of_(session);
        if (schema.has_error()) {
            co_return schema.error();
        }
        const auto schema_oid = schema.value();
        // Catalog first: every executor holds a copy of the master, so the master answers for them, and
        // nothing is dropped from any registry until the pg_proc purge below has succeeded.
        const auto overload = function_registry_.find_overload(function_name, inputs, schema_oid);
        const bool registered = overload != components::compute::invalid_function_uid;
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                core::function_name_t{std::move(function_name)},
                                                                std::move(inputs),
                                                                behavior,
                                                                schema_oid));

        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        auto planned = services::planner::create_plan(cstor,
                                                      function_registry_,
                                                      plan,
                                                      components::logical_plan::limit_t::all(),
                                                      /*params=*/nullptr);
        if (planned.has_error()) {
            co_return core::error_on(resource(), planned.error());
        }
        if (auto op_error = co_await run_catalog_op_(session, std::move(planned.value()), "unregister_udf");
            op_error.contains_error()) {
            co_return op_error;
        }
        // The rows of a function no process holds were all there was to drop.
        if (!registered) {
            co_return core::error_t::no_error();
        }
        [[maybe_unused]] const bool master_dropped = function_registry_.remove_function(overload);
        assert(master_dropped && "the overload checked above left the master registry");

        std::pmr::vector<actor_zeta::unique_future<bool>> ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(
                executor_addresses_[i],
                &collection::executor::executor_t::unregister_udf,
                session,
                core::function_name_t{plan->function_name()},
                std::pmr::vector<components::types::complex_logical_type>(plan->inputs(), resource()),
                schema_oid);
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(fut));
        }
        for (auto& ack : ack_futures) {
            // Executors copy the master, so a copy without the overload is a broken invariant, not a refusal.
            [[maybe_unused]] const bool dropped = co_await std::move(ack);
            assert(dropped && "an executor's registry diverged from the master");
        }
        co_return core::error_t::no_error();
    }

    namespace {
        components::logical_plan::execution_plan_t
        make_cast_resolve_plan(std::pmr::memory_resource* resource,
                               components::logical_plan::node_ptr leaf,
                               const components::types::complex_logical_type& source,
                               const components::types::complex_logical_type& target) {
            components::logical_plan::execution_plan_t plan{resource,
                                                            std::move(leaf),
                                                            components::logical_plan::make_parameter_node(resource)};
            std::vector<qualified_name_t> written;
            for (const auto* type : {&source, &target}) {
                if (type->type() == components::types::logical_type::UNKNOWN) {
                    written.push_back(components::sql::transform::type_reference_of(*type));
                }
            }
            components::sql::transform::register_catalog_resolve_types(resource, &plan.catalog_resolves, written);
            return plan;
        }
    } // namespace

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::register_cast(components::session::session_id_t session,
                                        components::types::complex_logical_type source,
                                        components::types::complex_logical_type target,
                                        components::casts::cast_entry entry) {
        trace(log_, "dispatcher_t::register_cast session: {}", session.data());

        auto leaf =
            boost::intrusive_ptr(new components::logical_plan::node_register_cast_t(resource(), source, target, entry));
        auto plan = make_cast_resolve_plan(resource(), leaf, source, target);
        co_await take_turn_(session, components::table::transaction_control_t::none);
        session_turn_t turn{this, session, components::table::transaction_control_t::none};
        auto resolved = create_session_context(session, &plan);
        if (resolved.has_error()) {
            co_return resolved.error();
        }
        auto session_ctx = std::move(resolved.value());
        const uint64_t statement_txn_id = session_ctx.txn.transaction_id;
        const std::size_t pool_idx = next_executor_index();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                              &collection::executor::executor_t::execute_plan_full,
                                                              session,
                                                              std::move(plan),
                                                              std::move(session_ctx));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto res = co_await std::move(fut);
        if (!res.cursor || res.cursor->is_error()) {
            co_await finish_failed_statement_(session, statement_txn_id);
        } else {
            end_statement_transaction_(session, statement_txn_id);
        }
        if (!res.cursor) {
            co_return core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"register_cast: the resolve pass returned no cursor", resource()}};
        }
        if (res.cursor->is_error()) {
            co_return res.cursor->get_error();
        }
        if (!res.resolved_cast) {
            co_return core::error_t{
                core::error_code_t::schema_error,
                std::pmr::string{"register_cast: the resolve pass returned no resolved (source, target) pair",
                                 resource()}};
        }
        const auto resolved_source = res.resolved_cast->first;
        const auto resolved_target = res.resolved_cast->second;

        // Registries set first: a crash before the pg_cast write can never leave a durable row with no live cast.
        std::pmr::vector<actor_zeta::unique_future<bool>> ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [ns, ack] = actor_zeta::otterbrix::send(executor_addresses_[i],
                                                         &collection::executor::executor_t::register_cast,
                                                         session,
                                                         resolved_source,
                                                         resolved_target,
                                                         entry);
            if (ns && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(ack));
        }
        core::error_t fanout_error = core::error_t::no_error();
        for (std::size_t i = 0; i < ack_futures.size(); ++i) {
            const bool accepted = co_await std::move(ack_futures[i]);
            if (!accepted && !fanout_error.contains_error()) {
                fanout_error = core::error_t{core::error_code_t::schema_error,
                                             std::pmr::string{"register_cast: executor " + std::to_string(i) + " of " +
                                                                  std::to_string(ack_futures.size()) +
                                                                  " refused the cast entry; pg_cast left unwritten",
                                                              resource()}};
            }
        }
        if (fanout_error.contains_error()) {
            error(log_, "dispatcher_t::register_cast: {}", fanout_error.what);
            co_return fanout_error;
        }
        if (auto err = cast_registry_.add(resolved_source, resolved_target, components::casts::cast_entry(entry));
            err.contains_error()) {
            error(log_, "register_cast: cast registry refused the entry: {}", err.what);
            co_return err;
        }

        auto write_leaf = boost::intrusive_ptr(
            new components::logical_plan::node_register_cast_t(resource(), resolved_source, resolved_target, entry));
        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        auto op = services::planner::impl::create_plan_register_cast(cstor, write_leaf);
        if (auto op_error = co_await run_catalog_op_(session, std::move(op), "register_cast");
            op_error.contains_error()) {
            co_return op_error;
        }
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::unregister_cast(components::session::session_id_t session,
                                          components::types::complex_logical_type source,
                                          components::types::complex_logical_type target) {
        trace(log_, "dispatcher_t::unregister_cast session: {}", session.data());

        auto leaf =
            boost::intrusive_ptr(new components::logical_plan::node_unregister_cast_t(resource(), source, target));
        auto plan = make_cast_resolve_plan(resource(), leaf, source, target);
        co_await take_turn_(session, components::table::transaction_control_t::none);
        session_turn_t turn{this, session, components::table::transaction_control_t::none};
        auto resolved = create_session_context(session, &plan);
        if (resolved.has_error()) {
            co_return resolved.error();
        }
        auto session_ctx = std::move(resolved.value());
        const uint64_t statement_txn_id = session_ctx.txn.transaction_id;
        const std::size_t pool_idx = next_executor_index();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                              &collection::executor::executor_t::execute_plan_full,
                                                              session,
                                                              std::move(plan),
                                                              std::move(session_ctx));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto res = co_await std::move(fut);
        if (!res.cursor || res.cursor->is_error()) {
            co_await finish_failed_statement_(session, statement_txn_id);
        } else {
            end_statement_transaction_(session, statement_txn_id);
        }
        if (!res.cursor) {
            co_return core::error_t{
                core::error_code_t::other_error,
                std::pmr::string{"unregister_cast: the resolve pass returned no cursor", resource()}};
        }
        if (res.cursor->is_error()) {
            co_return res.cursor->get_error();
        }
        if (!res.resolved_cast) {
            co_return core::error_t{
                core::error_code_t::schema_error,
                std::pmr::string{"unregister_cast: the resolve pass returned no resolved (source, target) pair",
                                 resource()}};
        }
        const auto resolved_source = res.resolved_cast->first;
        const auto resolved_target = res.resolved_cast->second;

        std::pmr::vector<actor_zeta::unique_future<bool>> ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [ns, ack] = actor_zeta::otterbrix::send(executor_addresses_[i],
                                                         &collection::executor::executor_t::unregister_cast,
                                                         session,
                                                         resolved_source,
                                                         resolved_target);
            if (ns && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(ack));
        }
        core::error_t fanout_error = core::error_t::no_error();
        for (std::size_t i = 0; i < ack_futures.size(); ++i) {
            const bool removed = co_await std::move(ack_futures[i]);
            if (!removed && !fanout_error.contains_error()) {
                fanout_error = core::error_t{core::error_code_t::schema_error,
                                             std::pmr::string{"unregister_cast: executor " + std::to_string(i) +
                                                                  " of " + std::to_string(ack_futures.size()) +
                                                                  " did not drop the cast; pg_cast row left in place",
                                                              resource()}};
            }
        }
        if (fanout_error.contains_error()) {
            error(log_, "dispatcher_t::unregister_cast: {}", fanout_error.what);
            co_return fanout_error;
        }
        if (!cast_registry_.remove(resolved_source, resolved_target)) {
            auto own_error = core::error_t{
                core::error_code_t::do_not_exists,
                std::pmr::string{"unregister_cast: the dispatcher's own cast registry did not hold the cast; "
                                 "pg_cast row left in place",
                                 resource()}};
            error(log_, "dispatcher_t::unregister_cast: {}", own_error.what);
            co_return own_error;
        }

        auto write_leaf = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_cast_t(resource(), resolved_source, resolved_target));
        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        auto op = services::planner::impl::create_plan_unregister_cast(cstor, write_leaf);
        if (auto op_error = co_await run_catalog_op_(session, std::move(op), "unregister_cast");
            op_error.contains_error()) {
            co_return op_error;
        }
        co_return core::error_t::no_error();
    }

    core::result_wrapper_t<txn_session_context_t>
    manager_dispatcher_t::create_session_context(components::session::session_id_t session,
                                                 components::logical_plan::execution_plan_t* plan) {
        if (failed_sessions_.contains(session)) {
            const auto control = transaction_control_of(*plan);
            if (control == components::table::transaction_control_t::none ||
                control == components::table::transaction_control_t::savepoint) {
                return core::error_t{core::error_code_t::transaction_finalized,
                                     std::pmr::string{"the transaction failed; only ROLLBACK, ROLLBACK TO SAVEPOINT or "
                                                      "COMMIT (which rolls back) is accepted until it ends",
                                                      resource()}};
            }
            // A ROLLBACK TO clears the failure only once it has undone the work (execute_plan).
            if (control != components::table::transaction_control_t::rollback_to) {
                failed_sessions_.erase(session);
            }
        }
        const auto settings = session_settings(session);
        const auto statement = settings_statement_of(*plan);
        // An ALTER SYSTEM opens no implicit transaction of autocommit off (psql's command_no_begin): it runs alone,
        // or is refused inside the block already open.
        const auto scope = settings.autocommit || statement.alter_system
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
        if (root == nullptr) {
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
    manager_dispatcher_t::take_turn_(components::session::session_id_t session,
                                     components::table::transaction_control_t control) {
        auto& order = session_order_.try_emplace(session, resource()).first->second;
        if (order.waiting.empty() && may_start_(session, order, control)) {
            start_(&order, control);
            co_return;
        }
        trace(log_, "manager_dispatcher_t::take_turn_: session {} waits for its running transaction", session.data());
#ifdef DEV_MODE
        g_statements_waited_turn.fetch_add(1);
#endif
        actor_zeta::promise<void> admitted(resource());
        auto turn = admitted.get_future();
        order.waiting.push_back(waiting_statement_t{control, std::move(admitted)});
        // The entry outlives this coroutine, and only the statement running in it touches the flag.
        in_flight_entry_t* entry = current_entry_;
        if (entry != nullptr) {
            entry->waiting = true;
        }
        co_await std::move(turn);
        if (entry != nullptr) {
            entry->waiting = false;
        }
    }

    bool manager_dispatcher_t::may_start_(components::session::session_id_t session,
                                          const session_order_t& order,
                                          components::table::transaction_control_t control) {
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
        return control == components::table::transaction_control_t::none;
    }

    void manager_dispatcher_t::start_(session_order_t* order, components::table::transaction_control_t control) {
        ++order->running;
        if (control != components::table::transaction_control_t::none) {
            order->closing = true;
        }
    }

    void manager_dispatcher_t::end_turn_(components::session::session_id_t session,
                                         components::table::transaction_control_t control) {
        auto it = session_order_.find(session);
        assert(it != session_order_.end() && it->second.running > 0);
        auto& order = it->second;
        --order.running;
        if (control != components::table::transaction_control_t::none) {
            order.closing = false;
        }
        while (!order.waiting.empty() && may_start_(session, order, order.waiting.front().control)) {
            auto next = std::move(order.waiting.front());
            order.waiting.pop_front();
            start_(&order, next.control);
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
                core::error_code_t::transaction_inactive,
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
        if (!txn_t->has_accumulated()) {
            // Empty COMMIT aborts instead of committing: it must not allocate a commit_id or advance the horizon. The
            // storages it read still get its end.
            out.txn = txn_t->data();
            out.participants = txn_t->drain_participants();
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
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
                core::error_code_t::transaction_inactive,
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
            return core::error_t{core::error_code_t::transaction_inactive, std::move(what)};
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
        txn_manager_.publish(commit_id);
        try_trigger_cleanup_if_horizon_advanced();
        co_return txn_manager_.compact_watermark();
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::txn_discard_msg(uint64_t commit_id) {
        trace(log_, "manager_dispatcher_t::txn_discard_msg, commit_id: {}", commit_id);
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
