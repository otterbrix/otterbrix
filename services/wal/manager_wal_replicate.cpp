#include "manager_wal_replicate.hpp"
#include <actor-zeta/spawn.hpp>
#include <chrono>
#include <thread>

namespace services::wal {

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* mr,
                                                     actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config,
                                                     log_t& log)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_t>()
        , resource_(mr)
        , scheduler_(scheduler)
        , config_(std::move(config))
        , log_(log.clone()) {
        trace(log_, "manager_wal_replicate_t start thread pool");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() { trace(log_, "delete manager_wal_replicate_t"); }

    auto manager_wal_replicate_t::make_type() const noexcept -> const char* { return "manager_wal"; }

    // Poll and clean up completed coroutines (per PROMISE_FUTURE_GUIDE.md)
    void manager_wal_replicate_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->available()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_load_.begin(); it != pending_load_.end();) {
            if (it->available()) {
                it = pending_load_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Custom enqueue_impl for SYNC actor with coroutine behavior()
    // This fixes the deadlock caused by actor_mixin::enqueue_impl discarding behavior_t
    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_wal_replicate_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Store the behavior coroutine (CRITICAL: prevents coroutine destruction!)
        current_behavior_ = behavior(msg.get());

        // Poll until behavior completes
        // Use sleep to allow scheduler worker threads to make progress
        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                // The deepest awaited future is ready - resume the coroutine chain
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                // Not ready - sleep briefly to let scheduler workers process messages
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_wal_replicate_t::behavior(actor_zeta::mailbox::message* msg) {
        std::lock_guard<spin_lock> guard(lock_);

        // Poll completed coroutines first (safe with actor-zeta 1.1.1)
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::load>: {
                // CRITICAL: Store pending coroutine!
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::create_database>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::create_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::drop_database>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::drop_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::create_collection>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::create_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::drop_collection>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::drop_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::insert_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::insert_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::insert_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::insert_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::delete_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::delete_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::delete_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::delete_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::update_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::update_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::update_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::update_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::create_index, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_wal_replicate_t::sync(address_pack pack) {
        manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
    }

    void manager_wal_replicate_t::create_wal_worker() {
        if (config_.sync_to_disk) {
            trace(log_, "manager_wal_replicate_t::create_wal_worker");
            auto worker = actor_zeta::spawn<wal_replicate_t>(resource(), this, log_, config_);
            dispatchers_.emplace_back(std::move(worker));
        } else {
            trace(log_, "manager_wal_replicate_t::create_wal_worker without disk");
            auto worker = actor_zeta::spawn<wal_replicate_without_disk_t>(resource(), this, log_, config_);
            dispatchers_.emplace_back(std::move(worker));
        }
    }

    manager_wal_replicate_t::unique_future<std::vector<record_t>> manager_wal_replicate_t::load(
        session_id_t session,
        services::wal::id_t wal_id) {
        trace(log_, "manager_wal_replicate_t::load, id: {}", wal_id);
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::load, session, wal_id);
        // Schedule wal_replicate if needs_sched (SYNC manager → ASYNC wal worker)
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    // WAL methods: send + enqueue + co_await

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::create_database(
        session_id_t session,
        components::logical_plan::node_create_database_ptr data) {
        trace(log_, "manager_wal_replicate_t::create_database {}", data->database_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::create_database, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::drop_database(
        session_id_t session,
        components::logical_plan::node_drop_database_ptr data) {
        trace(log_, "manager_wal_replicate_t::drop_database {}", data->database_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::drop_database, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::create_collection(
        session_id_t session,
        components::logical_plan::node_create_collection_ptr data) {
        trace(log_,
              "manager_wal_replicate_t::create_collection {}::{}",
              data->database_name(),
              data->collection_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::create_collection, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::drop_collection(
        session_id_t session,
        components::logical_plan::node_drop_collection_ptr data) {
        trace(log_, "manager_wal_replicate_t::drop_collection {}::{}", data->database_name(), data->collection_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::drop_collection, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::insert_one(
        session_id_t session,
        components::logical_plan::node_insert_ptr data) {
        trace(log_, "manager_wal_replicate_t::insert_one");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::insert_one, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::insert_many(
        session_id_t session,
        components::logical_plan::node_insert_ptr data) {
        trace(log_, "manager_wal_replicate_t::insert_many");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::insert_many, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::delete_one(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::delete_one");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::delete_one, session, std::move(data), std::move(params));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::delete_many(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::delete_many");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::delete_many, session, std::move(data), std::move(params));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::update_one(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::update_one");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::update_one, session, std::move(data), std::move(params));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::update_many(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::update_many");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::update_many, session, std::move(data), std::move(params));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::create_index(
        session_id_t session,
        components::logical_plan::node_create_index_ptr data) {
        trace(log_, "manager_wal_replicate_t::create_index");
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[0].get(), &wal_replicate_t::create_index, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_empty_t::manager_wal_replicate_empty_t(std::pmr::memory_resource* mr,
                                                                 actor_zeta::scheduler::sharing_scheduler* scheduler,
                                                                 log_t& log)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_empty_t>()
        , resource_(mr)
        , scheduler_(scheduler)
        , log_(log) {
        trace(log, "manager_wal_replicate_empty_t");
    }

    auto manager_wal_replicate_empty_t::make_type() const noexcept -> const char* { return "manager_wal_empty"; }

    actor_zeta::behavior_t manager_wal_replicate_empty_t::behavior(actor_zeta::mailbox::message* msg) {
        // Clean up completed futures first
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(),
                           [](const auto& f) { return f.available(); }),
            pending_void_.end());

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::load>:
                // load returns vector<record_t>, no need to store - it's immediately available
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::load, msg);
                break;
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::create_database>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::create_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::drop_database>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::drop_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::create_collection>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::create_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::drop_collection>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::drop_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::insert_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::insert_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::insert_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::insert_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::delete_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::delete_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::delete_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::delete_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::update_one>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::update_one, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::update_many>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::update_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::create_index, msg);
                break;
            }
            default:
                break;
        }
    }

    // Sync methods - called directly after constructor (no-op for empty version)
    void manager_wal_replicate_empty_t::sync(address_pack pack) {
        (void)pack;
        trace(log_, "manager_wal_replicate_empty_t::sync - no-op");
    }

    void manager_wal_replicate_empty_t::create_wal_worker() {
        trace(log_, "manager_wal_replicate_empty_t::create_wal_worker - no-op");
    }

    // Coroutine implementations - all return id=0 immediately (empty stub)
    manager_wal_replicate_empty_t::unique_future<std::vector<record_t>> manager_wal_replicate_empty_t::load(
        session_id_t session, services::wal::id_t wal_id) {
        (void)session; (void)wal_id;
        trace(log_, "manager_wal_replicate_empty_t::load - return empty records");
        co_return std::vector<record_t>{};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::create_database(
        session_id_t session, components::logical_plan::node_create_database_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::create_database - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::drop_database(
        session_id_t session, components::logical_plan::node_drop_database_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::drop_database - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::create_collection(
        session_id_t session, components::logical_plan::node_create_collection_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::create_collection - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::drop_collection(
        session_id_t session, components::logical_plan::node_drop_collection_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::drop_collection - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::insert_one(
        session_id_t session, components::logical_plan::node_insert_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::insert_one - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::insert_many(
        session_id_t session, components::logical_plan::node_insert_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::insert_many - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::delete_one(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        (void)session; (void)data; (void)params;
        trace(log_, "manager_wal_replicate_empty_t::delete_one - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::delete_many(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        (void)session; (void)data; (void)params;
        trace(log_, "manager_wal_replicate_empty_t::delete_many - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::update_one(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        (void)session; (void)data; (void)params;
        trace(log_, "manager_wal_replicate_empty_t::update_one - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::update_many(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params) {
        (void)session; (void)data; (void)params;
        trace(log_, "manager_wal_replicate_empty_t::update_many - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::create_index(
        session_id_t session, components::logical_plan::node_create_index_ptr data) {
        (void)session; (void)data;
        trace(log_, "manager_wal_replicate_empty_t::create_index - return success");
        co_return services::wal::id_t{0};
    }

} //namespace services::wal
