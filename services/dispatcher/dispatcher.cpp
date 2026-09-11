#include "dispatcher.hpp"
#include <atomic>

#include <components/casts/default_casts.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_register_cast.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_cast.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_udf.hpp>
#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>

#include <services/collection/context_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <array>
#include <functional>

using namespace components::cursor;

namespace services::dispatcher {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_pump_hops{0};
    } // namespace
    uint64_t pump_hops() noexcept { return g_pump_hops.load(std::memory_order_relaxed); }
    void reset_pump_hops() noexcept { g_pump_hops.store(0, std::memory_order_relaxed); }
    void note_pump_hop() noexcept { g_pump_hops.fetch_add(1, std::memory_order_relaxed); }
#endif

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
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_cast>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_cast>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::set_explain_renderer>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_mark_explicit_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_commit_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_accumulate_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_publish_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_discard_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_compact_watermark_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_drop_resource_marked>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_subscriber_empty>,
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
                                               uint64_t dml_flush_row_threshold,
                                               planner::create_plan_rule_t create_plan_rule,
                                               components::planner::optimizer_pass_t optimizer_pass)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , create_plan_rule_(create_plan_rule)
        , optimizer_pass_(optimizer_pass)
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr)
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , txn_manager_(resource_ptr)
        , cast_registry_(resource_ptr)
        , pending_void_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");
        components::casts::register_default_casts(cast_registry_);

        executors_.reserve(executor_pool_size_);
        executor_addresses_.reserve(executor_pool_size_);
        for (std::size_t i = 0; i < executor_pool_size_; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            log_.clone(),
                                                                            dml_flush_row_threshold,
                                                                            create_plan_rule_,
                                                                            optimizer_pass_);
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_, "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses", executor_pool_size_);

        loop_thread_ = std::thread([this] {
            // ~20 hops at 100us floors a statement at ~3.5ms, so idle_wait must exceed in_flight_wait.
            constexpr auto in_flight_wait = pump_tuning_t::in_flight_wait;
            constexpr auto idle_wait = pump_tuning_t::idle_wait;
            constexpr uint32_t stale_tick_threshold = pump_tuning_t::stale_tick_threshold;

            std::pmr::list<in_flight_entry_t> in_flight(resource());
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
                            } else if (e.behavior && !e.behavior.done() && e.behavior.is_busy() && !e.waiting) {
                                ++e.stale_ticks;
                            }
                        }
                        if (cont) {
                            ready_slot->stale_ticks = 0;
                            ready_slot->poke_rounds = 0;
#ifdef DEV_MODE
                            note_pump_hop();
#endif
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

                // WATCHDOG for the actor-zeta lost-wakeup race (docs/actor-zeta-lost-wakeup.md): a
                // mailbox parked reader_blocked while its future is already ready never wakes on its own.
                bool any_stale = false;
                for (auto& e : in_flight)
                    if (e.behavior && !e.behavior.done() && e.behavior.is_busy() && !e.behavior.is_awaited_ready() &&
                        e.stale_ticks > stale_tick_threshold) {
                        any_stale = true;
                        break;
                    }
                if (any_stale) {
                    constexpr uint32_t escalate_poke_rounds = 256;
                    bool escalate = false;
                    for (auto& e : in_flight) {
                        if (e.behavior && !e.behavior.done() && e.behavior.is_busy() &&
                            !e.behavior.is_awaited_ready() && e.stale_ticks > stale_tick_threshold) {
                            if (++e.poke_rounds == escalate_poke_rounds) {
                                escalate = true;
                            }
                        }
                    }
                    if (escalate) {
                        warn(log_,
                             "dispatcher loop: await stale across {} poke rounds — possible stalled executor",
                             escalate_poke_rounds);
                    } else {
                        trace(log_, "dispatcher loop: stale await detected — poking executors");
                    }
                    for (auto& ex : executors_) {
                        if (ex) {
                            [[maybe_unused]] auto [ns, f] =
                                actor_zeta::send(ex.get(), &collection::executor::executor_t::poke_msg);
                            if (ns)
                                scheduler_->enqueue(ex.get());
                        }
                    }
                    for (auto& e : in_flight) e.stale_ticks = 0;
                }

                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk, in_flight.empty() ? idle_wait : in_flight_wait);
                }
            }
        });
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
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
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
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
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_abort_msg, msg);
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

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::on_subscriber_empty(uint8_t subscriber_kind) {
        if (subscriber_kind == DISK_KIND) {
            disk_has_dropped_ = false;
        } else if (subscriber_kind == INDEX_KIND) {
            index_has_dropped_ = false;
        }
        co_return;
    }

    void manager_dispatcher_t::cache_settings_sync(const components::catalog::session_catalog_t& settings) {
        default_settings_ = settings;
        trace(log_,
              "manager_dispatcher_t::cache_settings_sync");
    }

    void manager_dispatcher_t::seed_commit_clock_sync(uint64_t high_water) {
        // Restores BOTH halves of the commit clock: raising only the horizon left post-reopen
        // INSERTs reusing already-published commit-ids (symptom: SSB q1-1 returned 0 rows on reopen).
        if (high_water > 0) {
            txn_manager_.restore_commit_clock(high_water);
            trace(log_,
                  "manager_dispatcher_t::seed_commit_clock_sync , restored commit clock to frontier {}",
                  high_water);
        }
    }

    namespace {
        components::table::transaction_control_t
        transaction_control_of(const components::logical_plan::execution_plan_t& plan) {
            const auto* root = plan.sub_queries.back().get();
            if (root == nullptr || root->type() != components::logical_plan::node_type::transaction_t) {
                return components::table::transaction_control_t::none;
            }
            const auto op = static_cast<const components::logical_plan::node_transaction_t*>(root)->op();
            if (op == components::logical_plan::transaction_op::commit) {
                return components::table::transaction_control_t::commit;
            }
            if (op == components::logical_plan::transaction_op::abort) {
                return components::table::transaction_control_t::rollback;
            }
            return components::table::transaction_control_t::none;
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
        const auto control = transaction_control_of(plan);
        co_await take_turn_(session, control);
        session_turn_t turn{this, session, control};
        auto resolved = create_session_context(session, &plan);
        if (resolved.has_error()) {
            co_return components::cursor::make_cursor(resource(), resolved.error());
        }
        auto session_ctx = std::move(resolved.value());
        const uint64_t statement_txn_id = session_ctx.txn.transaction_id;
        const std::size_t pool_idx = next_executor_index();
        trace(log_, "manager_dispatcher_t::execute_plan: routing to executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan_full,
                                                                 session,
                                                                 std::move(plan),
                                                                 std::move(session_ctx));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto exec_result = co_await std::move(future);

        if (!exec_result.applied_setting_value.empty()) {
            const auto& setting_def = components::catalog::find_setting_by_id(exec_result.applied_setting);
            auto apply_err = components::catalog::set_setting(default_settings_,
                                                              exec_result.applied_setting,
                                                              exec_result.applied_setting_value,
                                                              resource());
            if (apply_err.contains_error()) {
                error(log_,
                      "manager_dispatcher_t::execute_plan: settings cache refused {} = '{}' AFTER it was "
                      "persisted to pg_settings: {}",
                      setting_def.sql_name,
                      exec_result.applied_setting_value,
                      apply_err.what);
                exec_result.cursor = components::cursor::make_cursor(resource(), std::move(apply_err));
            }
        }

        trace(log_,
              "manager_dispatcher_t::execute_plan: result received, success: {}",
              exec_result.cursor->is_success());
        if (exec_result.cursor && exec_result.cursor->is_error()) {
            co_await finish_failed_statement_(session, statement_txn_id);
        }
        co_return std::move(exec_result.cursor);
    }

    manager_dispatcher_t::unique_future<core::error_t>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        if (!function) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"register_udf: no function to register", resource()}};
        }
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());

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
                                                                  plan->function()->get_copy(resource()));
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

        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        auto op = services::planner::impl::create_plan_register_udf(cstor, plan, std::move(executor_uids));
        if (!op) {
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return core::error_t{core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"register_udf: node_register_udf_t could not be lowered into an "
                                                     "operator",
                                                     resource()}};
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = components::table::transaction_data::committed();

        op->prepare();
        co_await op->await_async_and_resume(&pctx);
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* ru = static_cast<components::operators::operator_register_udf_t*>(op.get());
        if (op->has_error()) {
            error(log_, "dispatcher_t::register_udf: {}", op->get_error().what);
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return op->get_error();
        }
        if (!ru->success()) {
            co_await unwind_udf_fanout_(session, std::move(registered));
            co_return core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"register_udf: the operator reported failure without naming a "
                                                     "reason",
                                                     resource()}};
        }
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::unwind_udf_fanout_(
        components::session::session_id_t session,
        std::pmr::vector<std::pair<std::size_t, components::compute::function_uid>> registered) {
        std::pmr::vector<actor_zeta::unique_future<bool>> acks(resource());
        acks.reserve(registered.size());
        for (const auto& [idx, uid] : registered) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(
                executor_addresses_[idx],
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
                                         std::pmr::vector<components::types::complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);

        std::pmr::vector<actor_zeta::unique_future<bool>> ack_futures(resource());
        ack_futures.reserve(executor_addresses_.size());
        for (std::size_t i = 0; i < executor_addresses_.size(); ++i) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(
                executor_addresses_[i],
                &collection::executor::executor_t::unregister_udf,
                session,
                function_name,
                std::pmr::vector<components::types::complex_logical_type>{inputs.begin(), inputs.end(), resource()});
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            ack_futures.push_back(std::move(fut));
        }
        core::error_t fanout_error = core::error_t::no_error();
        for (std::size_t i = 0; i < ack_futures.size(); ++i) {
            const bool dropped = co_await std::move(ack_futures[i]);
            if (!dropped && !fanout_error.contains_error()) {
                fanout_error = core::error_t{core::error_code_t::unrecognized_function,
                                             std::pmr::string{"unregister_udf: executor " + std::to_string(i) + " of " +
                                                                  std::to_string(ack_futures.size()) +
                                                                  " held no overload of '" + function_name +
                                                                  "' matching this signature; pg_proc left untouched",
                                                              resource()}};
            }
        }
        if (fanout_error.contains_error()) {
            error(log_, "dispatcher_t::unregister_udf: {}", fanout_error.what);
            co_return fanout_error;
        }

        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                core::function_name_t{std::move(function_name)},
                                                                std::move(inputs)));

        services::context_storage_t cstor{resource(), log_.clone(), session_settings(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            co_return core::error_t{core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"unregister_udf: node_unregister_udf_t could not be lowered into "
                                                     "an operator",
                                                     resource()}};
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = components::table::transaction_data::committed();

        op->prepare();
        co_await op->await_async_and_resume(&pctx);
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* uu = static_cast<components::operators::operator_unregister_udf_t*>(op.get());
        if (op->has_error()) {
            error(log_, "dispatcher_t::unregister_udf: {}", op->get_error().what);
            co_return op->get_error();
        }
        if (!uu->success()) {
            co_return core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"unregister_udf: the operator reported failure without naming a "
                                                     "reason",
                                                     resource()}};
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
            components::logical_plan::resolve_entry_t namespace_entry;
            namespace_entry.dbname = "public";
            plan.catalog_resolves.ensure(resource, components::logical_plan::resolve_kind::namespace_)
                .add(std::move(namespace_entry));
            for (const auto* type : {&source, &target}) {
                if (type->type() != components::types::logical_type::UNKNOWN) {
                    continue;
                }
                components::logical_plan::resolve_entry_t type_entry;
                type_entry.dbname = "public";
                type_entry.type_name = type->type_name();
                plan.catalog_resolves.ensure(resource, components::logical_plan::resolve_kind::type)
                    .add(std::move(type_entry));
            }
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
        } else if (auto* txn = statement_transaction_(session, statement_txn_id);
                   txn != nullptr && txn->scope() == components::table::transaction_scope_t::statement) {
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
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
        if (!op) {
            co_return core::error_t{core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"register_cast: node_register_cast_t could not be lowered into an "
                                                     "operator",
                                                     resource()}};
        }
        op->set_as_root();
        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = components::table::transaction_data::committed();
        op->prepare();
        co_await op->await_async_and_resume(&pctx);
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }
        auto* rc = static_cast<components::operators::operator_register_cast_t*>(op.get());
        if (op->has_error()) {
            error(log_, "dispatcher_t::register_cast: {}", op->get_error().what);
            co_return op->get_error();
        }
        if (!rc->success()) {
            co_return core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"register_cast: the operator reported failure without naming a "
                                                     "reason",
                                                     resource()}};
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
        } else if (auto* txn = statement_transaction_(session, statement_txn_id);
                   txn != nullptr && txn->scope() == components::table::transaction_scope_t::statement) {
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
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
        if (!op) {
            co_return core::error_t{core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"unregister_cast: node_unregister_cast_t could not be lowered "
                                                     "into an operator",
                                                     resource()}};
        }
        op->set_as_root();
        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = components::table::transaction_data::committed();
        op->prepare();
        co_await op->await_async_and_resume(&pctx);
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }
        auto* uc = static_cast<components::operators::operator_unregister_cast_t*>(op.get());
        if (op->has_error()) {
            error(log_, "dispatcher_t::unregister_cast: {}", op->get_error().what);
            co_return op->get_error();
        }
        if (!uc->success()) {
            co_return core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"unregister_cast: the operator reported failure without naming a "
                                                     "reason",
                                                     resource()}};
        }
        co_return core::error_t::no_error();
    }

    core::result_wrapper_t<txn_session_context_t>
    manager_dispatcher_t::create_session_context(components::session::session_id_t session,
                                                 components::logical_plan::execution_plan_t* plan) {
        const auto& settings = session_settings(session);
        const auto scope = settings.autocommit ? components::table::transaction_scope_t::statement
                                               : components::table::transaction_scope_t::until_commit;
        auto resolved = txn_manager_.resolve_transaction(session, scope, transaction_control_of(*plan));
        if (resolved.has_error()) {
            return resolved.error();
        }
        auto* txn = resolved.value();
        plan->commits_when_done = txn->scope() == components::table::transaction_scope_t::statement;
        txn_session_context_t context;
        context.txn = txn->data();
        context.settings = settings;
        context.lowest_active_start_time = txn_manager_.lowest_active_start_time();
        trace(log_,
              "manager_dispatcher_t::create_session_context, session: {}, txn: {}, ends with the statement: {}",
              session.data(),
              context.txn.transaction_id,
              plan->commits_when_done);
        return context;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::finish_failed_statement_(components::session::session_id_t session,
                                                   uint64_t transaction_id) {
        auto* txn = statement_transaction_(session, transaction_id);
        if (txn == nullptr || txn->state() != components::table::transaction_state_t::active) {
            co_return;
        }
        if (txn->scope() == components::table::transaction_scope_t::statement) {
            txn_manager_.abort(session);
            try_trigger_cleanup_if_horizon_advanced();
            co_return;
        }
        trace(log_,
              "manager_dispatcher_t::finish_failed_statement_: txn {} failed, session: {}",
              txn->transaction_id(),
              session.data());
        txn_manager_.fail(session);
        try_trigger_cleanup_if_horizon_advanced();
        co_await run_rollback_plan_(session, txn->data());
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::run_rollback_plan_(components::session::session_id_t session,
                                             components::table::transaction_data txn) {
        components::logical_plan::execution_plan_t plan{
            resource(),
            components::logical_plan::make_node_transaction(resource(),
                                                            components::logical_plan::transaction_op::abort),
            components::logical_plan::make_parameter_node(resource())};
        txn_session_context_t context;
        context.txn = txn;
        context.settings = session_settings(session);
        context.lowest_active_start_time = txn_manager_.lowest_active_start_time();
        const std::size_t pool_idx = next_executor_index();
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan_full,
                                                                 session,
                                                                 std::move(plan),
                                                                 std::move(context));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto result = co_await std::move(future);
        if (result.cursor && result.cursor->is_error()) {
            error(log_,
                  "manager_dispatcher_t::run_rollback_plan_: the failed transaction was not fully undone: {}",
                  result.cursor->get_error().what);
        }
    }

    components::table::transaction_t*
    manager_dispatcher_t::statement_transaction_(components::session::session_id_t session, uint64_t transaction_id) {
        auto* txn = txn_manager_.find_transaction(session);
        if (txn == nullptr || txn->transaction_id() != transaction_id) {
            return nullptr;
        }
        return txn;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::take_turn_(components::session::session_id_t session,
                                     components::table::transaction_control_t control) {
        auto& order = session_order_.try_emplace(session, resource()).first->second;
        if (order.waiting.empty() && may_start_(session, order, control)) {
            start_(&order, control);
            co_return;
        }
        trace(log_, "manager_dispatcher_t::take_turn_: session {} waits for its running transaction", session.data());
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
            co_return core::error_t{core::error_code_t::transaction_inactive,
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
        if (txn_t == nullptr) {
            out.refusal = core::error_t{
                core::error_code_t::transaction_inactive,
                std::pmr::string{"COMMIT: its transaction ended before COMMIT reached it", resource()}};
            co_return out;
        }
        if (txn_t->state() == components::table::transaction_state_t::failed) {
            // A statement failed it while this COMMIT ran; its work is already undone.
            txn_manager_.abort(session);
            out.refusal = core::error_t{
                core::error_code_t::transaction_finalized,
                std::pmr::string{"the transaction failed and was rolled back; nothing was committed", resource()}};
            co_return out;
        }
        if (!txn_t->has_accumulated()) {
            // Empty COMMIT aborts instead of committing: it must not allocate a commit_id or advance the horizon.
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
        // No publish barrier here — txn_publish_msg runs it after storage/WAL, so no snapshot sees it half-flipped.
        out.commit_id = txn_manager_.commit(session);
        co_return out;
    }

    txn_abort_drain_t manager_dispatcher_t::drain_for_abort_(components::table::transaction_t& txn) {
        txn_abort_drain_t out;
        out.txn = txn.data();
        txn.drain_pg_catalog_pending(out.swap_appends, out.pg_catalog_delete_tables);
        auto backfills_discarded = txn.drain_pg_attribute_commit_id_backfills();
        (void) backfills_discarded;
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
        return out;
    }

    manager_dispatcher_t::unique_future<txn_abort_drain_t>
    manager_dispatcher_t::txn_abort_drain_msg(components::session::session_id_t session, uint64_t transaction_id) {
        trace(log_, "manager_dispatcher_t::txn_abort_drain_msg, session: {}", session.data());
        txn_abort_drain_t out;
        auto* txn_t = statement_transaction_(session, transaction_id);
        if (txn_t == nullptr) {
            out.refusal = core::error_t{
                core::error_code_t::transaction_inactive,
                std::pmr::string{"ROLLBACK: its transaction ended before ROLLBACK reached it", resource()}};
            co_return out;
        }
        out = drain_for_abort_(*txn_t);
        // A failed transaction's work goes now; the transaction stays until its session ends it.
        if (txn_t->state() != components::table::transaction_state_t::failed) {
            txn_manager_.abort(session);
        }
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
        if (txn_t->state() == components::table::transaction_state_t::failed) {
            co_return core::error_t{
                core::error_code_t::transaction_finalized,
                std::pmr::string{"txn_accumulate_msg: the transaction failed while the statement ran, so its "
                                 "ranges are not parked",
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
        co_return core::error_t::no_error();
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_abort_msg(components::session::session_id_t session, uint64_t transaction_id) {
        trace(log_, "manager_dispatcher_t::txn_abort_msg, session: {}", session.data());
        if (statement_transaction_(session, transaction_id) == nullptr) {
            co_return;
        }
        txn_manager_.abort(session);
        try_trigger_cleanup_if_horizon_advanced();
        co_return;
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
