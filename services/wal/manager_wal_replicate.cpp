#include "manager_wal_replicate.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
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
            // Discover existing database directories (named after database_oid).
            // Recover global_id_, create workers.
            wal::id_t max_recovered_id = 0;
            for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
                if (!entry.is_directory()) {
                    continue;
                }
                auto db_dir_name = entry.path().filename().string();
                // Parse directory name as database_oid. Skip non-numeric directories
                // (legacy / unrelated content).
                components::catalog::oid_t db_oid;
                try {
                    db_oid = static_cast<components::catalog::oid_t>(std::stoul(db_dir_name));
                } catch (...) {
                    trace(log_, "manager_wal_replicate: skip non-oid directory '{}'", db_dir_name);
                    continue;
                }
                trace(log_, "manager_wal_replicate: recovering database_oid={}", static_cast<unsigned>(db_oid));

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

                get_or_create_worker(db_oid);
            }
            global_id_.store(max_recovered_id, std::memory_order_relaxed);
        }
        trace(log_, "manager_wal_replicate finish");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() { trace(log_, "delete manager_wal_replicate_t"); }

    // -----------------------------------------------------------------------
    // Actor infrastructure
    // -----------------------------------------------------------------------

    std::pmr::memory_resource* manager_wal_replicate_t::resource() const noexcept { return resource_; }

    const char* manager_wal_replicate_t::make_type() const noexcept { return "manager_wal_replicate"; }

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
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::auto_checkpoint_wal_id>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::auto_checkpoint_wal_id, msg);
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
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::register_active_build>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::register_active_build, msg);
                break;
            }
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::unregister_active_build>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::unregister_active_build, msg);
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
    // Retention guard: active CREATE INDEX build registration.
    //
    // No locking: assumed called from the operator-pipeline thread BEFORE the
    // wal_worker has finished the previous batch — single-threaded relative to
    // the dispatcher / wal contracts. TODO: re-evaluate under multi-DB.
    // -----------------------------------------------------------------------

    void manager_wal_replicate_t::register_active_build_sync(wal::id_t build_start_wal_position) {
        active_build_start_positions_.emplace(build_start_wal_position);
        trace(log_,
              "manager_wal_replicate::register_active_build_sync wal_id={} active_builds={}",
              build_start_wal_position,
              active_build_start_positions_.size());
    }

    void manager_wal_replicate_t::unregister_active_build_sync(wal::id_t build_start_wal_position) {
        auto erased = active_build_start_positions_.erase(build_start_wal_position);
        // Invariant: unregister must match a prior register; otherwise we have a
        // lifecycle bug in operator_create_index that would silently leak retention.
        assert(erased == 1 && "unregister_active_build_sync called without matching register");
        if (erased != 1) {
            std::abort();
        }
        trace(log_,
              "manager_wal_replicate::unregister_active_build_sync wal_id={} active_builds={}",
              build_start_wal_position,
              active_build_start_positions_.size());
    }

    // -----------------------------------------------------------------------
    // Mailbox-handler twins of the _sync helpers above. Called by
    // operator_create_index_backfill which runs inside the executor actor —
    // rule 11 forbids sync inter-actor calls there, so we route the same
    // in-place set mutation through the manager's mailbox. The handler body is
    // intentionally thin: dispatch into the actor's own coroutine, run the sync
    // helper (we are now on the manager's thread per actor-zeta single-consumer
    // mailbox model), co_return.
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<void>
    manager_wal_replicate_t::register_active_build(session_id_t /*session*/, wal::id_t build_start_wal_position) {
        register_active_build_sync(build_start_wal_position);
        co_return;
    }

    manager_wal_replicate_t::unique_future<void>
    manager_wal_replicate_t::unregister_active_build(session_id_t /*session*/, wal::id_t build_start_wal_position) {
        unregister_active_build_sync(build_start_wal_position);
        co_return;
    }

    // -----------------------------------------------------------------------
    // Global WAL ID
    // -----------------------------------------------------------------------

    wal::id_t manager_wal_replicate_t::next_wal_id() { return ++global_id_; }

    // -----------------------------------------------------------------------
    // Worker management
    // -----------------------------------------------------------------------

    wal_worker_t* manager_wal_replicate_t::get_or_create_worker(components::catalog::oid_t database_oid) {
        auto it = wal_actors_.find(database_oid);
        if (it != wal_actors_.end()) {
            return it->second.get();
        }

        trace(log_, "manager_wal_replicate: spawning worker for database_oid={}", static_cast<unsigned>(database_oid));
        auto worker = actor_zeta::spawn<wal_worker_t>(resource_, log_, config_, database_oid);
        auto* ptr = worker.get();
        wal_actors_.emplace(database_oid, std::move(worker));
        return ptr;
    }

    // -----------------------------------------------------------------------
    // Contract: load
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<std::vector<record_t>> manager_wal_replicate_t::load(session_id_t session,
                                                                                                wal::id_t wal_id) {
        if (!enabled_) {
            co_return std::vector<record_t>{};
        }

        // Collect records from ALL workers, merge-sort by wal_id.
        std::vector<record_t> merged;
        for (auto& [db_oid, worker] : wal_actors_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(worker->address(), &wal_worker_t::load, session, wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            auto records = co_await std::move(fut);
            merged.insert(merged.end(),
                          std::make_move_iterator(records.begin()),
                          std::make_move_iterator(records.end()));
        }

        std::sort(merged.begin(), merged.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        co_return std::move(merged);
    }

    // -----------------------------------------------------------------------
    // Contract: commit_txn
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::commit_txn(session_id_t session,
                                        uint64_t txn_id,
                                        wal_sync_mode sync_mode,
                                        components::catalog::oid_t database_oid,
                                        uint64_t commit_id) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::commit_txn,
                                                              session,
                                                              txn_id,
                                                              sync_mode,
                                                              wal_id,
                                                              commit_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        // Track WAL bytes for auto-checkpoint threshold (atomic).
        wal_bytes_since_checkpoint_.store(total_wal_bytes(), std::memory_order_relaxed);
        co_return result;
    }

    std::uintmax_t manager_wal_replicate_t::total_wal_bytes() const noexcept {
        if (!enabled_ || config_.path.empty())
            return 0;
        std::uintmax_t total = 0;
        std::error_code ec;
        for (const auto& db_entry : std::filesystem::directory_iterator(config_.path, ec)) {
            if (ec || !db_entry.is_directory(ec)) {
                ec.clear();
                continue;
            }
            for (const auto& seg : std::filesystem::directory_iterator(db_entry.path(), ec)) {
                if (ec) {
                    ec.clear();
                    continue;
                }
                if (!seg.is_regular_file(ec)) {
                    ec.clear();
                    continue;
                }
                auto sz = std::filesystem::file_size(seg.path(), ec);
                if (!ec)
                    total += sz;
                ec.clear();
            }
        }
        return total;
    }

    // -----------------------------------------------------------------------
    // Contract: truncate_before
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<void> manager_wal_replicate_t::truncate_before(session_id_t session,
                                                                                          wal::id_t checkpoint_wal_id) {
        if (!enabled_) {
            co_return;
        }

        // Clamp to min(active_build_start_positions_) so any in-flight CREATE
        // INDEX Phase 2.5 catchup still has its records. Empty set means no
        // active builds, so no clamp is necessary. std::set is ordered
        // ascending — .begin() is the minimum.
        if (!active_build_start_positions_.empty()) {
            auto earliest = *active_build_start_positions_.begin();
            if (earliest < checkpoint_wal_id) {
                trace(log_,
                      "manager_wal_replicate::truncate_before clamped from {} to {} due to active build retention",
                      checkpoint_wal_id,
                      earliest);
                checkpoint_wal_id = earliest;
            }
        }

        // Send to ALL workers.
        for (auto& [db_oid, worker] : wal_actors_) {
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

    manager_wal_replicate_t::unique_future<wal::id_t> manager_wal_replicate_t::current_wal_id(session_id_t session) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        // Take max across all workers.
        wal::id_t max_id = 0;
        for (auto& [db_oid, worker] : wal_actors_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(worker->address(), &wal_worker_t::current_wal_id, session);
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
    // Contract: auto_checkpoint_wal_id
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::auto_checkpoint_wal_id(session_id_t /*session*/) {
        if (!needs_auto_checkpoint()) {
            co_return wal::id_t{0};
        }
        reset_auto_checkpoint_bytes();
        co_return global_id_.load(std::memory_order_relaxed);
    }

    // -----------------------------------------------------------------------
    // Contract: write_physical_insert
    //
    // Callers pass `table_oid` directly. Worker keying uses `main_database`
    // (single-worker for all WAL traffic). Once multi-database support arrives
    // the routing key will move to per-table namespace_oid resolution.
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<wal::id_t>
    manager_wal_replicate_t::write_physical_insert(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                                                   uint64_t row_start,
                                                   uint64_t row_count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_insert,
                                                              session,
                                                              table_oid,
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
    manager_wal_replicate_t::write_physical_delete(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_delete,
                                                              session,
                                                              table_oid,
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
    manager_wal_replicate_t::write_physical_update(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   std::unique_ptr<components::vector::data_chunk_t> new_data,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return wal::id_t{0};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_update,
                                                              session,
                                                              table_oid,
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
