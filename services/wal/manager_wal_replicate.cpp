#include "manager_wal_replicate.hpp"

#include <algorithm>
#include <filesystem>
#include <mutex>

#include <actor-zeta/spawn.hpp>
#include <core/executor.hpp>
#include <services/wal/wal_page_reader.hpp>

namespace services::wal {

    // -----------------------------------------------------------------------
    // Constructor / Destructor
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                                     actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config,
                                                     log_t& log)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , enabled_(config_.on)
        , manager_disk_(actor_zeta::address_t::empty_address())
        , manager_dispatcher_(actor_zeta::address_t::empty_address()) {
        trace(log_, "manager_wal_replicate start, enabled={}", enabled_);
        if (enabled_ && !config_.path.empty()) {
            std::filesystem::create_directories(config_.path);
            // Discover existing database directories, recover global_id_, create workers.
            wal::id_t max_recovered_id = 0;
            for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
                if (!entry.is_directory()) {
                    continue;
                }
                auto db_name = entry.path().filename().string();
                trace(log_, "manager_wal_replicate: recovering database '{}'", db_name);

                // Scan segments to find max wal_id (via reader, no actor messaging).
                for (const auto& seg : std::filesystem::directory_iterator(entry.path())) {
                    if (!seg.is_regular_file()) {
                        continue;
                    }
                    wal_page_reader_t reader(seg.path());
                    auto records = reader.read_all_records(0);
                    for (const auto& r : records) {
                        if (r.is_valid() && r.id > max_recovered_id) {
                            max_recovered_id = r.id;
                        }
                    }
                }

                get_or_create_worker(db_name);
            }
            global_id_.store(max_recovered_id, std::memory_order_relaxed);
        }
        trace(log_, "manager_wal_replicate finish");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() {
        trace(log_, "delete manager_wal_replicate_t");
    }

    // -----------------------------------------------------------------------
    // Actor infrastructure
    // -----------------------------------------------------------------------

    std::pmr::memory_resource* manager_wal_replicate_t::resource() const noexcept {
        return resource_;
    }

    const char* manager_wal_replicate_t::make_type() const noexcept {
        return "manager_wal_replicate";
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_wal_replicate_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_wal_replicate_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::truncate_before>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::truncate_before, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::current_wal_id>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::current_wal_id, msg);
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

    // -----------------------------------------------------------------------
    // sync: receive disk and dispatcher addresses
    // -----------------------------------------------------------------------

    void manager_wal_replicate_t::sync(address_pack pack) {
        auto [disk_addr, dispatcher_addr] = pack;
        manager_disk_ = std::move(disk_addr);
        manager_dispatcher_ = std::move(dispatcher_addr);
        trace(log_, "manager_wal_replicate::sync done");
    }

    // -----------------------------------------------------------------------
    // Global WAL ID
    // -----------------------------------------------------------------------

    wal::id_t manager_wal_replicate_t::next_wal_id() {
        return ++global_id_;
    }

    // -----------------------------------------------------------------------
    // Worker management
    // -----------------------------------------------------------------------

    wal_worker_t* manager_wal_replicate_t::get_or_create_worker(const std::string& database) {
        auto it = wal_actors_.find(database);
        if (it != wal_actors_.end()) {
            return it->second.get();
        }

        trace(log_, "manager_wal_replicate: spawning worker for database '{}'", database);
        auto worker = actor_zeta::spawn<wal_worker_t>(resource_, this, log_, config_, database);
        auto* ptr = worker.get();
        wal_actors_.emplace(database, std::move(worker));
        return ptr;
    }

    // -----------------------------------------------------------------------
    // Contract: load
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<std::vector<record_t>>
    manager_wal_replicate_t::load(session_id_t session, wal::id_t wal_id) {
        if (!enabled_) {
            co_return std::vector<record_t>{};
        }

        // Collect records from ALL workers, merge-sort by wal_id.
        std::vector<record_t> merged;
        for (auto& [db_name, worker] : wal_actors_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                              &wal_worker_t::load,
                                              session,
                                              wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            auto records = co_await std::move(fut);
            merged.insert(merged.end(),
                          std::make_move_iterator(records.begin()),
                          std::make_move_iterator(records.end()));
        }

        std::sort(merged.begin(), merged.end(),
                  [](const record_t& a, const record_t& b) { return a.id < b.id; });

        co_return std::move(merged);
    }

    // -----------------------------------------------------------------------
    // Contract: commit_txn
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::commit_txn(session_id_t session,
                                        uint64_t txn_id,
                                        wal_sync_mode sync_mode,
                                        std::string database_name) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database_name);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                          &wal_worker_t::commit_txn,
                                          session,
                                          txn_id,
                                          sync_mode,
                                          wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return result;
    }

    // -----------------------------------------------------------------------
    // Contract: truncate_before
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<void>
    manager_wal_replicate_t::truncate_before(session_id_t session, wal::id_t checkpoint_wal_id) {
        if (!enabled_) {
            co_return;
        }

        // Send to ALL workers.
        for (auto& [db_name, worker] : wal_actors_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                              &wal_worker_t::truncate_before,
                                              session,
                                              checkpoint_wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            co_await std::move(fut);
        }
        co_return;
    }

    // -----------------------------------------------------------------------
    // Contract: current_wal_id
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::current_wal_id(session_id_t session) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        // Take max across all workers.
        wal::id_t max_id = 0;
        for (auto& [db_name, worker] : wal_actors_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                              &wal_worker_t::current_wal_id,
                                              session);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            auto wid = co_await std::move(fut);
            if (wid > max_id) {
                max_id = wid;
            }
        }
        co_return max_id;
    }

    // -----------------------------------------------------------------------
    // Contract: write_physical_insert
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::write_physical_insert(
        session_id_t session,
        std::string database,
        std::string collection,
        std::unique_ptr<components::vector::data_chunk_t> data_chunk,
        uint64_t row_start,
        uint64_t row_count,
        uint64_t txn_id) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                          &wal_worker_t::write_physical_insert,
                                          session,
                                          std::move(database),
                                          std::move(collection),
                                          std::move(data_chunk),
                                          row_start,
                                          row_count,
                                          txn_id,
                                          wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return result;
    }

    // -----------------------------------------------------------------------
    // Contract: write_physical_delete
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::write_physical_delete(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        uint64_t count,
        uint64_t txn_id) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                          &wal_worker_t::write_physical_delete,
                                          session,
                                          std::move(database),
                                          std::move(collection),
                                          std::move(row_ids),
                                          count,
                                          txn_id,
                                          wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return result;
    }

    // -----------------------------------------------------------------------
    // Contract: write_physical_update
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::write_physical_update(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        std::unique_ptr<components::vector::data_chunk_t> new_data,
        uint64_t count,
        uint64_t txn_id) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                          &wal_worker_t::write_physical_update,
                                          session,
                                          std::move(database),
                                          std::move(collection),
                                          std::move(row_ids),
                                          std::move(new_data),
                                          count,
                                          txn_id,
                                          wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return result;
    }

} // namespace services::wal
