#include "dispatcher.hpp"

#include <components/context/context.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
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

    namespace {
        // subscriber-kind discriminator carried in the on_drop_resource_marked /
        // on_subscriber_empty / on_horizon_advanced messages between the
        // dispatcher and its drop-GC subscribers.
        constexpr uint8_t DISK_KIND = 1;
        constexpr uint8_t INDEX_KIND = 2;
    } // namespace

    // ---- behavior/dispatch_traits sync check ----
    // Ensures behavior() handles every method registered in dispatch_traits
    // (positional msg_id: a missed case = silent message loss). When adding a
    // method: dispatch_traits entry + behavior() case + kBehaviorHandledIds.
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
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_begin_session_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_mark_explicit_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_commit_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_drain_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_accumulate_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_msg>,
            actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_publish_msg>,
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
                                               log_t& log)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr)
        , txn_manager_(resource_ptr)
        , pending_void_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");

        // Event-loop-in-thread model. enqueue_impl (any sender thread) only
        // pushes into the lock-free inbox_ and notifies pump_cv_; this thread
        // owns ALL processing. The in-flight slot list is LOCAL to the loop, so
        // no mutex guards the phase logic (resource() is a thread-safe
        // synchronized_pool_resource).
        loop_thread_ = std::thread([this] {
            std::pmr::list<in_flight_entry_t> in_flight(resource());
            uint32_t loop_ticks = 0;
            while (loop_running_.load(std::memory_order_acquire)) {
                // Drain the inbox into local slots, re-wrapping each raw pointer
                // into a message_ptr. The behavior created below holds a raw
                // pointer into the message, so pending_msg must outlive it.
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                }

                bool progress = true;
                while (progress) {
                    progress = false;

                    // (a) Create behavior for the first slot that needs one
                    //     (marker: behavior handle still null). The coroutine
                    //     runs on this loop thread until its first co_await and
                    //     holds a raw pointer into pending_msg across suspension,
                    //     so pending_msg must STAY in the slot.
                    {
                        in_flight_entry_t* slot = nullptr;
                        for (auto& e : in_flight) {
                            if (e.pending_msg && !e.behavior) {
                                slot = &e;
                                break;
                            }
                        }
                        if (slot) {
                            slot->behavior = behavior(slot->pending_msg.get());
                            progress = true;
                            continue;
                        }
                    }

                    // (b) Resume the first ready behavior; reset its staleness.
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
                            } else if (e.behavior && !e.behavior.done() && e.behavior.is_busy()) {
                                ++e.stale_ticks;
                            }
                        }
                        if (cont) {
                            ready_slot->stale_ticks = 0;
                            cont.resume();
                            poll_pending();
                            progress = true;
                            continue;
                        }
                    }

                    // (c) Erase one done slot, then restart the pass (the erase
                    //     invalidates the iteration). behavior + pending_msg
                    //     destruct on this thread.
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }

                    poll_pending();
                }

                // WATCHDOG for the actor-zeta parking race on an executor
                // (docs/actor-zeta-lost-wakeup.md): the executor's mailbox can be
                // reader_blocked while its awaited future is READY, and
                // resume_impl's blocked-check precedes the busy/ready check, so it
                // never wakes. A mailbox PUSH unblocks it. A slot stuck busy &&
                // !ready past the staleness threshold signals this; we poke with
                // the dedicated no-op poke_msg. Firing early on a legitimately
                // long executor operation is harmless (an empty handler run).
                bool any_stale = false;
                for (auto& e : in_flight)
                    if (e.behavior && !e.behavior.done() && e.behavior.is_busy() && !e.behavior.is_awaited_ready() &&
                        e.stale_ticks > 20) {
                        any_stale = true;
                        break;
                    }
                if (any_stale) {
                    warn(log_,
                         "dispatcher loop: stale await detected — poking executors (see "
                         "docs/actor-zeta-lost-wakeup.md)");
                    for (auto& ex : executors_) {
                        if (ex) {
                            auto [ns, f] = actor_zeta::send(ex.get(), &collection::executor::executor_t::poke_msg);
                            if (ns)
                                scheduler_->enqueue(ex.get());
                            (void) f; // safe to drop: dealloc happens when the last of future/promise releases
                        }
                    }
                    for (auto& e : in_flight) e.stale_ticks = 0; // backoff: re-arm threshold
                }

                ++loop_ticks;
                (void) loop_ticks;
                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk, std::chrono::microseconds(100));
                }
                // NOTE: lock-free inbox trade — a push+notify may slip between
                // empty() and wait_for; bounded by the 100µs timeout
                // (staleness, not loss).
            }
            // Local in_flight destructs HERE on the loop thread: still-suspended
            // behaviors are destroyed safely (~behavior_t destroys suspended
            // frames; future/promise state freed on last release).
        });
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any leftover inbox_ raw pointers: re-wrap each into a
        // message_ptr temporary so its PMR memory is freed (the loop is gone).
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
        // Delivery only — ALL processing happens on loop_thread_. The lock-free
        // inbox takes ownership of the raw message* (release()); the loop
        // re-wraps it into a message_ptr. notify without holding mutex_ is fine
        // (the loop re-checks inbox_.empty() under the lock before sleeping).
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
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_begin_session_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_begin_session_msg, msg);
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

    void manager_dispatcher_t::sync(sync_pack pack) {
        wal_address_ = pack.wal;
        disk_address_ = pack.disk;
        index_address_ = pack.index;

        executors_.reserve(executor_pool_size_);
        executor_addresses_.reserve(executor_pool_size_);
        for (std::size_t i = 0; i < executor_pool_size_; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            log_.clone());
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_, "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses", executor_pool_size_);
    }

    void manager_dispatcher_t::try_trigger_cleanup_if_horizon_advanced() noexcept {
        // Commit-id value space: the subscribers' sweep compares
        // dropped_at_commit_id (remapped to the real commit_id by
        // storage_dropped_committed / table_dropped_committed) against this
        // horizon, so the broadcast must use the same space — the oldest
        // snapshot_horizon any live txn can still read below, NOT
        // lowest_active_start_time (txn start-time space; mixing the two kept
        // DROP-GC dead: 2^62-range ids never compared < small start-times).
        //
        // Remap-before-broadcast ordering proof. For the committing
        // txn the broadcast here can only carry a horizon that reclaims its own
        // tombstones AFTER its DROP-GC remap has stamped them, never before:
        //   1. operator_commit_transaction runs the storage/table dropped-committed
        //      remap by sending it to the disk/index managers PRE-publish and
        //      co_awaiting the result — the remap has LANDED before the operator
        //      proceeds.
        //   2. ONLY THEN does the operator send txn_publish_msg, whose handler
        //      calls publish() (advancing published_horizon_ to this commit_id)
        //      and then this function, enqueuing on_horizon_advanced to the SAME
        //      subscriber addresses.
        // Because (1)'s remap message and (2)'s broadcast target the same
        // subscriber mailbox, same-mailbox FIFO ordering per subscriber guarantees
        // the remap is dequeued first. The sweep therefore always sees the
        // commit_id-stamped tombstones before the horizon that would reclaim them.
        auto new_lowest = txn_manager_.lowest_active_snapshot_horizon();
        if (new_lowest > last_broadcast_horizon_) {
            last_broadcast_horizon_ = new_lowest;
            if (disk_has_dropped_ && disk_address_ != actor_zeta::address_t::empty_address()) {
                // Fire-and-forget (subscriber acks via on_subscriber_empty).
                // Parking the future on pending_void_ is just bookkeeping —
                // poll_pending() drains it via is_ready(); dropping it instead
                // would be memory-safe too.
                auto disk_send_result =
                    actor_zeta::send(disk_address_, &services::disk::manager_disk_t::on_horizon_advanced, new_lowest);
                pending_void_.emplace_back(std::move(disk_send_result.second));
            }
            if (index_has_dropped_ && index_address_ != actor_zeta::address_t::empty_address()) {
                auto index_send_result = actor_zeta::send(index_address_,
                                                          &services::index::manager_index_t::on_horizon_advanced,
                                                          new_lowest);
                pending_void_.emplace_back(std::move(index_send_result.second));
            }
        }
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

    void manager_dispatcher_t::set_replay_horizon_sync(uint64_t commit_id) {
        // publish() takes the manager's own lock_ — safe even outside a started
        // scheduler. publish() is monotonic (CAS keeps the max), so passing a
        // stale id is a no-op.
        if (commit_id > 0) {
            txn_manager_.publish(commit_id);
            trace(log_, "manager_dispatcher_t::set_replay_horizon_sync , advanced published_horizon to {}", commit_id);
        }
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       components::logical_plan::execution_plan_t plan) {
        trace(log_,
              "manager_dispatcher_t::execute_plan session: {}, {}",
              session.data(),
              plan.sub_queries.back()->to_string());

        // Pure session-hash routing — no plan inspection: the executor owns
        // optimize/resolve/validate/enrich/rewrites and the commit tails. The
        // hash gives every session a sticky executor, deterministically.
        assert(!executors_.empty());
        const std::size_t pool_idx = std::hash<components::session::session_id_t>{}(session) % executors_.size();
        trace(log_, "manager_dispatcher_t::execute_plan: routing to executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan_full,
                                                                 session,
                                                                 std::move(plan));
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto exec_result = co_await std::move(future);

        // The ONLY post-execute bookkeeping left on the dispatcher: a
        // successful SET TIMEZONE surfaces the persisted zone name by value;
        // refresh the solely-owned default_tz_cat_ so subsequent session_tz()
        // reads see it. Only this loop thread mutates the catalog.
        if (!exec_result.applied_timezone.empty()) {
            (void) default_tz_cat_.set_timezone(
                resource(),
                std::string_view{exec_result.applied_timezone.data(), exec_result.applied_timezone.size()});
        }

        trace(log_,
              "manager_dispatcher_t::execute_plan: result received, success: {}",
              exec_result.cursor->is_success());
        if (exec_result.cursor && exec_result.cursor->is_error()) {
            // Failure release — IMPLICIT txns only. Executor error paths that
            // co_return BEFORE their commit/abort tails (validation,
            // catalog-resolve and sub-query errors) leave the session txn begun
            // by txn_begin_session_msg ACTIVE forever. A leaked active txn pins
            // compact_watermark() at its snapshot horizon permanently, blocking
            // every later compact and (via the checkpoint gate) every per-table
            // checkpoint. The no-txn / already-ended cases fall through (the
            // executor's failed-DML tail aborts itself). EXPLICIT txns are left
            // alive on purpose: the client's ROLLBACK runs the abort-drain
            // revert cascade (e.g. un-stamping a DROP's catalog tombstones); a
            // bare abort() here would discard that revert state and leave dead
            // txn-id delete marks blocking any later re-DELETE of the same rows.
            if (auto* txn = txn_manager_.find_transaction(session); txn != nullptr && !txn->is_explicit()) {
                txn_manager_.abort(session);
                try_trigger_cleanup_if_horizon_advanced();
            }
        }
        co_return std::move(exec_result.cursor);
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());

        // Pool-admin operation: the dispatcher owns the executor addresses and
        // the scheduler, so it drives the per-executor registry fan-out ITSELF
        // (plain sequential sends — no callable indirection), then runs the
        // operator pipeline for the default-registry mirror + pg_proc rows.
        // The node owns the unique function payload; every fan-out send
        // carries a deep copy via get_copy().
        auto plan =
            boost::intrusive_ptr(new components::logical_plan::node_register_udf_t(resource(), std::move(function)));

        components::operators::operator_register_udf_t::executor_uids_t executor_uids(resource());
        executor_uids.reserve(executor_addresses_.size());
        // Two-phase fan-out: send register_udf to every executor first (each
        // send carries its own deep function copy, so the sends are mutually
        // independent), then await all the acks. Every future is drained even
        // after the first error so none is dropped; a single error fails the
        // whole registration.
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
        bool fanout_failed = false;
        for (auto& fut : ack_futures) {
            auto res = co_await std::move(fut);
            if (!res || res->has_error()) {
                fanout_failed = true;
                continue;
            }
            executor_uids.push_back(res->value());
        }
        if (fanout_failed) {
            co_return false;
        }

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        auto op = services::planner::impl::create_plan_register_udf(cstor, plan, std::move(executor_uids));
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting)
                break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* ru = static_cast<components::operators::operator_register_udf_t*>(op.get());
        co_return ru->success();
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<components::types::complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);

        // Operator-pipeline path. The logical leaf node_unregister_udf_t
        // carries the (name, inputs) signature; the operator probes
        // function_registry_t::get_default(), removes the matching overload,
        // and purges pg_proc + pg_depend rows.
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                core::function_name_t{std::move(function_name)},
                                                                std::move(inputs)));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting)
                break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* uu = static_cast<components::operators::operator_unregister_udf_t*>(op.get());
        co_return uu->success();
    }

    // ===== txn-state mailbox service =====
    // Every handler is a pure co_return over intra-actor txn_manager_ calls —
    // no awaits, no executor round-trips (anti-deadlock invariant).

    manager_dispatcher_t::unique_future<txn_session_context_t>
    manager_dispatcher_t::txn_begin_session_msg(components::session::session_id_t session) {
        // begin_transaction is idempotent per session (returns the existing
        // active txn), so a DML statement inside an explicit BEGIN joins it.
        auto& txn = txn_manager_.begin_transaction(session);
        txn_session_context_t out;
        out.txn = txn.data();
        out.session_tz = session_tz(session);
        out.is_explicit = txn.is_explicit();
        out.lowest_active_start_time = txn_manager_.lowest_active_start_time();
        trace(log_,
              "manager_dispatcher_t::txn_begin_session_msg, session: {}, txn: {}, explicit: {}",
              session.data(),
              out.txn.transaction_id,
              out.is_explicit);
        co_return out;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_mark_explicit_msg(components::session::session_id_t session) {
        // begin (idempotent) THEN mark — never a silent no-op on a missing txn.
        // A stray BEGIN inside an open txn reuses it (Postgres semantics).
        auto& txn = txn_manager_.begin_transaction(session);
        txn.mark_explicit();
        trace(log_,
              "manager_dispatcher_t::txn_mark_explicit_msg, session: {}, txn: {}",
              session.data(),
              txn.transaction_id());
        co_return;
    }

    manager_dispatcher_t::unique_future<txn_commit_drain_t>
    manager_dispatcher_t::txn_commit_drain_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_commit_drain_msg, session: {}", session.data());
        txn_commit_drain_t out;
        if (auto* txn_t = txn_manager_.find_transaction(session)) {
            if (!txn_t->has_accumulated()) {
                // Empty COMMIT — a bare COMMIT, a read-only explicit txn, or
                // zero-row DML accumulated nothing. Aborting is the MVCC-equivalent
                // end: an empty commit must NOT allocate a commit_id nor advance
                // the horizon (no rows to make visible, no tombstones to GC). out
                // stays default — commit_id 0, every drain field empty — so the
                // caller skips publish() and storage_publish_* entirely.
                txn_manager_.abort(session);
                // Ending the txn frees its snapshot horizon — let the DROP-GC
                // broadcast fire (parity with the abort-drain / abort handlers).
                try_trigger_cleanup_if_horizon_advanced();
                co_return out;
            }
            // Snapshot + drain BEFORE commit() purges the active map: base
            // appends remapped to pg_catalog_append_range_t, base deletes
            // collapsed to a table-oid set (loss-free: every drained range
            // carries the same explicit txn id).
            out.txn = txn_t->data();
            txn_t->drain_pg_catalog_pending(out.swap_appends, out.swap_deletes);
            out.swap_backfills = txn_t->drain_pg_attribute_commit_id_backfills();
            auto drained_appends = txn_t->drain_base_appends();
            out.base_appends.reserve(drained_appends.size());
            for (const auto& r : drained_appends) {
                out.base_appends.push_back(
                    components::pg_catalog_append_range_t{r.table_oid, r.row_start, r.row_count});
            }
            auto drained_deletes = txn_t->drain_base_deletes();
            for (const auto& d : drained_deletes) {
                out.base_delete_tables.insert(d.table_oid);
            }
            // Drained out so the commit operator's GC-remap can stamp these with
            // commit_id; non-empty here (NOT is_ddl_commit_) triggers that block.
            out.dropped_storage_oids = txn_t->drain_dropped_storages();
            // CREATE side, symmetric with the DROP drain above: the commit
            // operator publishes these storage oids / indexes at COMMIT.
            out.created_storage_oids = txn_t->drain_created_storages();
            out.created_indexes = txn_t->drain_created_indexes();
        }
        // Allocates the commit_id and leaves it in in_flight_commits_ (0 on a
        // missing txn). The ProcArray publish barrier deliberately does NOT run
        // here — the caller sends txn_publish_msg AFTER storage_publish_* / WAL,
        // so concurrent snapshots never observe a half-flipped pg_catalog.
        out.commit_id = txn_manager_.commit(session);
        co_return out;
    }

    manager_dispatcher_t::unique_future<txn_abort_drain_t>
    manager_dispatcher_t::txn_abort_drain_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_abort_drain_msg, session: {}", session.data());
        txn_abort_drain_t out;
        if (auto* txn_t = txn_manager_.find_transaction(session)) {
            out.txn = txn_t->data();
            // Keep the appends (their physical row slots need
            // storage_revert_appends). KEEP the pg_catalog delete-tables too: a
            // DROP inside this txn stamped delete marks on catalog heaps
            // (delete_id == txn_id). Those marks are invisible to readers (aborted
            // txn id), but they PERSIST on the heap and would block a future
            // re-DELETE of the same catalog row (chunk_vector_info::delete_rows
            // skips an already-marked slot). The abort operator un-stamps them via
            // storage_revert_deletes, mirroring the base-table delete revert.
            // The backfill markers are still discarded (their targets are in the
            // appends, reverted by storage_revert_appends).
            txn_t->drain_pg_catalog_pending(out.swap_appends, out.pg_catalog_delete_tables);
            auto backfills_discarded = txn_t->drain_pg_attribute_commit_id_backfills();
            (void) backfills_discarded;
            // Drain the parked base appends only to collect the UNIQUE table
            // oids they touched: the abort operator fans out
            // manager_index_t::revert_insert per oid to drop this txn's PENDING
            // in-memory index entries (parity with executor.cpp's failed-DML
            // revert). The ranges themselves are discarded — their physical row
            // slots ride in the pg_catalog/base storage_revert path, and the
            // index revert keys per (table_oid, txn_id), not per range. The base
            // DELETE ranges are collected the same way: only their UNIQUE table
            // oids matter, so the abort operator can fan out
            // manager_index_t::revert_delete per oid to clear this txn's PENDING
            // in-memory index DELETE markers (parity with executor.cpp's
            // failed-DML revert_delete). The ranges themselves are still dropped:
            // uncommitted tombstones (delete_id == txn_id) are invisible to every
            // reader and VACUUM reclaims them; only the index markers, which sit
            // outside the MVCC visibility filter, need the explicit revert.
            auto drained_appends = txn_t->drain_base_appends();
            for (const auto& r : drained_appends) {
                out.base_append_tables.insert(r.table_oid);
            }
            auto drained_deletes = txn_t->drain_base_deletes();
            for (const auto& d : drained_deletes) {
                out.base_delete_tables.insert(d.table_oid);
            }
            // Drain the DROP-retired storage oids too. Informational today: the
            // abort operator does not yet un-stamp them on abort; for now they ride
            // out for symmetry with the commit drain and to clear the accumulator.
            out.dropped_storage_oids = txn_t->drain_dropped_storages();
            // Drain the CREATE-brought storage oids / indexes so the abort
            // operator can drop the still-uncommitted artifacts — symmetric with
            // the commit drain and the DROP drain above.
            out.created_storage_oids = txn_t->drain_created_storages();
            out.created_indexes = txn_t->drain_created_indexes();
        }
        txn_manager_.abort(session);
        // Aborting removes the txn from the active set, so the lowest-active
        // horizon may advance — give the DROP-GC broadcast a chance to fire.
        try_trigger_cleanup_if_horizon_advanced();
        co_return out;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_accumulate_msg(components::session::session_id_t session,
                                             txn_accumulate_payload_t payload) {
        trace(log_, "manager_dispatcher_t::txn_accumulate_msg, session: {}", session.data());
        if (auto* txn_t = txn_manager_.find_transaction(session)) {
            // transaction_t's single-owner-thread invariant is structurally
            // enforced here: only this loop thread mutates the body.
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
        }
        co_return;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_abort_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_abort_msg, session: {}", session.data());
        txn_manager_.abort(session);
        // Abort ends an active txn — the horizon may have advanced.
        try_trigger_cleanup_if_horizon_advanced();
        co_return;
    }

    manager_dispatcher_t::unique_future<uint64_t> manager_dispatcher_t::txn_publish_msg(uint64_t commit_id) {
        trace(log_, "manager_dispatcher_t::txn_publish_msg, commit_id: {}", commit_id);
        txn_manager_.publish(commit_id);
        // The committed txn left the active set at commit(); after the publish
        // barrier the DROP-GC horizon broadcast is safe to evaluate.
        try_trigger_cleanup_if_horizon_advanced();
        // Return value is the COMPACT-WATERMARK (commit-id value space): the
        // visible-to-all horizon from txn_manager_.compact_watermark(). It rides
        // through maybe_cleanup_many into data_table_t::compact(), whose local
        // stamp scan refuses the rebuild when ANY version stamp is above it —
        // another active txn's snapshot, or a committed-but-unpublished commit
        // still sitting in in_flight_commits_, could otherwise lose versions it
        // must still see. Distinct from the commit-id-space GC horizon
        // broadcast above (lowest_active_snapshot_horizon), which bounds the
        // DROP-tombstone sweep, not version-history collapse.
        co_return txn_manager_.compact_watermark();
    }

    manager_dispatcher_t::unique_future<uint64_t> manager_dispatcher_t::txn_compact_watermark_msg() {
        // Pure intra-actor read for the checkpoint/vacuum compact paths (see
        // header). Monotone, so staleness across the mailbox hop is safe.
        const auto watermark = txn_manager_.compact_watermark();
        trace(log_, "manager_dispatcher_t::txn_compact_watermark_msg, watermark: {}", watermark);
        co_return watermark;
    }

} // namespace services::dispatcher
