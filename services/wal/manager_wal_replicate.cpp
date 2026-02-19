#include "manager_wal_replicate.hpp"
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <chrono>
#include <thread>

namespace services::wal {

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                                     actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config,
                                                     log_t& log)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , pending_void_(resource)
        , pending_load_(resource) {
        create_wal_worker(config_.agent);
        trace(log_, "manager_wal_replicate_t start thread pool");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() { trace(log_, "delete manager_wal_replicate_t"); }

    auto manager_wal_replicate_t::make_type() const noexcept -> const char* { return "manager_wal"; }

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

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_wal_replicate_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<spin_lock> guard(lock_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_wal_replicate_t::behavior(actor_zeta::mailbox::message* msg) {

        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::write_physical_insert>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::write_physical_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::write_physical_delete>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::write_physical_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::write_physical_update>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::write_physical_update, msg);
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

    void manager_wal_replicate_t::create_wal_worker(int count_worker) {
        for (int i = 0; i < count_worker; ++i) {
            if (config_.sync_to_disk) {
                trace(log_, "manager_wal_replicate_t::create_wal_worker index={} count={}", i, count_worker);
                auto worker = actor_zeta::spawn<wal_replicate_t>(resource(), this, log_, config_, i, count_worker);
                dispatchers_.emplace_back(std::move(worker));
            } else {
                trace(log_, "manager_wal_replicate_t::create_wal_worker without disk index={}", i);
                auto worker = actor_zeta::spawn<wal_replicate_without_disk_t>(resource(), this, log_, config_, i, count_worker);
                dispatchers_.emplace_back(std::move(worker));
            }
        }
    }

    manager_wal_replicate_t::unique_future<std::vector<record_t>> manager_wal_replicate_t::load(
        session_id_t session,
        services::wal::id_t wal_id) {
        trace(log_, "manager_wal_replicate_t::load, id: {}, workers: {}", wal_id, dispatchers_.size());
        std::vector<record_t> all_records;
        for (std::size_t i = 0; i < dispatchers_.size(); ++i) {
            auto [needs_sched, future] = actor_zeta::send(dispatchers_[i].get(), &wal_replicate_t::load, session, wal_id);
            if (needs_sched) {
                scheduler_->enqueue(dispatchers_[i].get());
            }
            auto records = co_await std::move(future);
            all_records.insert(all_records.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
        }
        std::sort(all_records.begin(), all_records.end(),
            [](const record_t& a, const record_t& b) { return a.id < b.id; });
        co_return all_records;
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::create_index(
        session_id_t session,
        components::logical_plan::node_create_index_ptr data) {
        trace(log_, "manager_wal_replicate_t::create_index");
        auto idx = worker_index_for(data->collection_full_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[idx].get(), &wal_replicate_t::create_index, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[idx].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::drop_index(
        session_id_t session,
        components::logical_plan::node_drop_index_ptr data) {
        trace(log_, "manager_wal_replicate_t::drop_index");
        auto idx = worker_index_for(data->collection_full_name());
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[idx].get(), &wal_replicate_t::drop_index, session, std::move(data));
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[idx].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::commit_txn(
        session_id_t session,
        uint64_t transaction_id) {
        trace(log_, "manager_wal_replicate_t::commit_txn txn_id={}", transaction_id);
        // Write commit marker to all workers (any worker might have the DML records)
        services::wal::id_t last_id{0};
        for (std::size_t i = 0; i < dispatchers_.size(); ++i) {
            auto [needs_sched, future] = actor_zeta::send(dispatchers_[i].get(), &wal_replicate_t::commit_txn, session, transaction_id);
            if (needs_sched) {
                scheduler_->enqueue(dispatchers_[i].get());
            }
            last_id = co_await std::move(future);
        }
        co_return last_id;
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::write_physical_insert(
        session_id_t session,
        std::string database,
        std::string collection,
        std::unique_ptr<components::vector::data_chunk_t> data_chunk,
        uint64_t row_start,
        uint64_t row_count,
        uint64_t txn_id) {
        trace(log_, "manager_wal_replicate_t::write_physical_insert {}::{}", database, collection);
        auto coll_name = collection_full_name_t(database, collection);
        auto idx = worker_index_for(coll_name);
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[idx].get(), &wal_replicate_t::write_physical_insert,
            session, std::move(database), std::move(collection), std::move(data_chunk), row_start, row_count, txn_id);
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[idx].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::write_physical_delete(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        uint64_t count,
        uint64_t txn_id) {
        trace(log_, "manager_wal_replicate_t::write_physical_delete {}::{}", database, collection);
        auto coll_name = collection_full_name_t(database, collection);
        auto idx = worker_index_for(coll_name);
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[idx].get(), &wal_replicate_t::write_physical_delete,
            session, std::move(database), std::move(collection), std::move(row_ids), count, txn_id);
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[idx].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_t::unique_future<services::wal::id_t> manager_wal_replicate_t::write_physical_update(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        std::unique_ptr<components::vector::data_chunk_t> new_data,
        uint64_t count,
        uint64_t txn_id) {
        trace(log_, "manager_wal_replicate_t::write_physical_update {}::{}", database, collection);
        auto coll_name = collection_full_name_t(database, collection);
        auto idx = worker_index_for(coll_name);
        auto [needs_sched, future] = actor_zeta::send(dispatchers_[idx].get(), &wal_replicate_t::write_physical_update,
            session, std::move(database), std::move(collection), std::move(row_ids), std::move(new_data), count, txn_id);
        if (needs_sched) {
            scheduler_->enqueue(dispatchers_[idx].get());
        }
        co_return co_await std::move(future);
    }

    manager_wal_replicate_empty_t::manager_wal_replicate_empty_t(std::pmr::memory_resource* resource,
                                                                 actor_zeta::scheduler::sharing_scheduler* /*scheduler*/,
                                                                 log_t& log)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_empty_t>()
        , resource_(resource)
        , log_(log)
        , pending_void_(resource) {
        trace(log, "manager_wal_replicate_empty_t");
    }

    auto manager_wal_replicate_empty_t::make_type() const noexcept -> const char* { return "manager_wal_empty"; }

    actor_zeta::behavior_t manager_wal_replicate_empty_t::behavior(actor_zeta::mailbox::message* msg) {
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(),
                           [](const auto& f) { return f.available(); }),
            pending_void_.end());

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::load>:
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::load, msg);
                break;
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::write_physical_insert>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::write_physical_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::write_physical_delete>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::write_physical_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_empty_t, &manager_wal_replicate_empty_t::write_physical_update>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_empty_t::write_physical_update, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_wal_replicate_empty_t::sync(address_pack /*pack*/) {
        trace(log_, "manager_wal_replicate_empty_t::sync - no-op");
    }

    void manager_wal_replicate_empty_t::create_wal_worker(int) {
        trace(log_, "manager_wal_replicate_empty_t::create_wal_worker - no-op");
    }

    manager_wal_replicate_empty_t::unique_future<std::vector<record_t>> manager_wal_replicate_empty_t::load(
        session_id_t /*session*/, services::wal::id_t /*wal_id*/) {
        trace(log_, "manager_wal_replicate_empty_t::load - return empty records");
        co_return std::vector<record_t>{};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::create_index(
        session_id_t /*session*/, components::logical_plan::node_create_index_ptr /*data*/) {
        trace(log_, "manager_wal_replicate_empty_t::create_index - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::drop_index(
        session_id_t /*session*/, components::logical_plan::node_drop_index_ptr /*data*/) {
        trace(log_, "manager_wal_replicate_empty_t::drop_index - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::commit_txn(
        session_id_t /*session*/, uint64_t /*transaction_id*/) {
        trace(log_, "manager_wal_replicate_empty_t::commit_txn - return success");
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::write_physical_insert(
        session_id_t /*session*/, std::string /*database*/, std::string /*collection*/,
        std::unique_ptr<components::vector::data_chunk_t> /*data_chunk*/,
        uint64_t /*row_start*/, uint64_t /*row_count*/, uint64_t /*txn_id*/) {
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::write_physical_delete(
        session_id_t /*session*/, std::string /*database*/, std::string /*collection*/,
        std::pmr::vector<int64_t> /*row_ids*/, uint64_t /*count*/, uint64_t /*txn_id*/) {
        co_return services::wal::id_t{0};
    }

    manager_wal_replicate_empty_t::unique_future<services::wal::id_t> manager_wal_replicate_empty_t::write_physical_update(
        session_id_t /*session*/, std::string /*database*/, std::string /*collection*/,
        std::pmr::vector<int64_t> /*row_ids*/, std::unique_ptr<components::vector::data_chunk_t> /*new_data*/,
        uint64_t /*count*/, uint64_t /*txn_id*/) {
        co_return services::wal::id_t{0};
    }

} //namespace services::wal
