#include "manager_wal_replicate.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <thread>

#include <actor-zeta/spawn.hpp>
#include <core/executor.hpp>
#include <services/wal/wal_page_reader.hpp>

// Needed for the auto-checkpoint orchestration (run_auto_checkpoint): the WAL
// manager drives flush_all_indexes on the index manager and checkpoint_all on
// the disk manager (+ the compact-watermark fetch from the dispatcher).
// wal_contract.hpp stays free of these to avoid the cycle
// (manager_disk.hpp / manager_index.hpp pull only services/wal/base.hpp back).
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

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
        , manager_dispatcher_(actor_zeta::address_t::empty_address())
        , manager_index_(actor_zeta::address_t::empty_address()) {
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

        // Start the event loop only after all WAL recovery above has completed,
        // so the loop never races with the (single-threaded) recovery scan.
        loop_thread_ = std::thread([this] {
            // in_flight lives on the loop thread for the whole loop lifetime and
            // owns every in-flight message_ptr and behavior_t.
            // this->resource() is qualified because the ctor param `resource` shadows the member fn.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());

            while (loop_running_.load(std::memory_order_acquire)) {
                // Drain the lock-free inbox, re-wrapping each raw message* into a message_ptr.
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr(raw);
                }

                bool made_progress = false;

                // Unlike manager_dispatcher_t, all sends here are co_await'ed
                // inline, so there are no pending_<T>_ containers / poll_pending step.

                // (a) Create a behavior for the next entry that needs one. pending_msg
                //     STAYS in its slot: the coroutine holds a raw pointer to the
                //     message across suspension points, so it must outlive the behavior.
                for (auto& e : in_flight) {
                    if (e.pending_msg && !e.behavior) {
                        e.behavior = behavior(e.pending_msg.get());
                        made_progress = true;
                        break;
                    }
                }
                if (made_progress) {
                    continue;
                }

                // (b) Resume any behavior whose awaited result is ready.
                {
                    actor_zeta::detail::coroutine_handle<> cont{};
                    for (auto& e : in_flight) {
                        if (e.behavior.is_awaited_ready()) {
                            cont = e.behavior.take_awaited_continuation();
                            if (cont) {
                                break;
                            }
                        }
                    }
                    if (cont) {
                        cont.resume();
                        continue;
                    }
                }

                // (c) Erase one done entry per pass (destroying its behavior_t and
                //     message_ptr on the loop thread). Done = behavior created AND completed.
                for (auto it = in_flight.begin(); it != in_flight.end();) {
                    if (it->behavior && it->behavior.done()) {
                        it = in_flight.erase(it);
                        made_progress = true;
                        break;
                    } else {
                        ++it;
                    }
                }
                if (made_progress) {
                    continue;
                }

                // Reap completed fire-and-forget auto-checkpoint futures before
                // idling. Cheap no-op when no checkpoint is in flight.
                poll_auto_checkpoint_();

                // Idle: wait for an enqueue notify, or a bounded staleness window
                // to re-check the inbox / behaviors that became ready off-thread.
                std::unique_lock<std::mutex> lock(mutex_);
                pump_cv_.wait_for(lock, std::chrono::microseconds(100));
            }
            // in_flight (and every message_ptr / behavior_t it owns) is destroyed
            // here, on the loop thread — never on a sender thread.
        });
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() {
        trace(log_, "delete manager_wal_replicate_t");
        // Stop the loop and join before tearing down members.
        loop_running_.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> guard(mutex_);
            pump_cv_.notify_one();
        }
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain messages that arrived after the final loop pass so their promise
        // sides release cleanly.
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drained(raw);
        }
    }

    // -----------------------------------------------------------------------
    // Actor infrastructure
    // -----------------------------------------------------------------------

    std::pmr::memory_resource* manager_wal_replicate_t::resource() const noexcept { return resource_; }

    const char* manager_wal_replicate_t::make_type() const noexcept { return "manager_wal_replicate"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_wal_replicate_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Senders only deliver: hand the raw message to the lock-free inbox and
        // wake the loop. ALL processing runs on loop_thread_, never here.
        inbox_.push(msg.release());
        pump_cv_.notify_one();
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
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::run_auto_checkpoint>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::run_auto_checkpoint, msg);
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

    void manager_wal_replicate_t::sync(wal_sync_pack_t pack) {
        manager_disk_ = std::move(pack.disk);
        manager_dispatcher_ = std::move(pack.dispatcher);
        manager_index_ = std::move(pack.index);
        trace(log_, "manager_wal_replicate::sync done");
    }

    // Retention guard: active CREATE INDEX build registration. Unlocked — see
    // the single-threaded assumption documented on the declarations.

    void manager_wal_replicate_t::register_active_build_sync(wal::id_t build_start_wal_position) {
        active_build_start_positions_.emplace(build_start_wal_position);
        trace(log_,
              "manager_wal_replicate::register_active_build_sync wal_id={} active_builds={}",
              build_start_wal_position,
              active_build_start_positions_.size());
    }

    void manager_wal_replicate_t::unregister_active_build_sync(wal::id_t build_start_wal_position) {
        auto erased = active_build_start_positions_.erase(build_start_wal_position);
        // Invariant: every unregister matches a prior register; a mismatch is an
        // operator_create_index lifecycle bug that would silently leak retention.
        assert(erased == 1 && "unregister_active_build_sync called without matching register");
        if (erased != 1) {
            std::abort();
        }
        trace(log_,
              "manager_wal_replicate::unregister_active_build_sync wal_id={} active_builds={}",
              build_start_wal_position,
              active_build_start_positions_.size());
    }

    // Mailbox twins of the _sync helpers (see declarations). The body runs on
    // the manager's thread, so it may call the sync helper directly.

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
        // Track WAL bytes for auto-checkpoint threshold.
        wal_bytes_since_checkpoint_.store(total_wal_bytes(), std::memory_order_relaxed);

        // Auto-checkpoint trigger. needs_auto_checkpoint() compares WAL bytes
        // written SINCE the last checkpoint (not total WAL size) against the
        // configured auto_checkpoint_threshold_bytes. auto_checkpoint_in_flight_
        // dedups: a burst of threshold-tripping commits must not stack concurrent
        // checkpoints. We reset the byte counter HERE (not in the handler) so any
        // commits racing in behind this one accumulate against a fresh window
        // toward the NEXT checkpoint instead of re-tripping the same one.
        //
        // Fire-and-forget self-send: the checkpoint (index flush + storage
        // checkpoint + WAL truncate) is heavy and must NOT extend the committer's
        // commit_txn latency. The self-sent message lands in inbox_ and the loop
        // runs run_auto_checkpoint as an independent in-flight entry after this
        // coroutine returns its wal_id to the caller.
        if (needs_auto_checkpoint() && !auto_checkpoint_in_flight_) {
            auto_checkpoint_in_flight_ = true;
            reset_auto_checkpoint_bytes();
            auto [_ac, ac_fut] = actor_zeta::send(address(), &manager_wal_replicate_t::run_auto_checkpoint, session);
            // needs_sched is always false: enqueue_impl only pushes to inbox_ and
            // wakes the loop (no scheduler hop). Park the [[nodiscard]] future;
            // the loop drains it once ready (poll_auto_checkpoint_).
            pending_auto_checkpoint_.emplace_back(std::move(ac_fut));
        }
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

        // Clamp to min(active_build_start_positions_) so an in-flight CREATE
        // INDEX backfill catchup still finds its records. Empty => no clamp.
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
    // Contract: run_auto_checkpoint
    //
    // Self-orchestrated analogue of the CHECKPOINT statement operator
    // (operator_checkpoint.cpp): flush indexes -> snapshot current wal id ->
    // checkpoint_all storage -> truncate the WAL below the returned checkpoint id.
    // Triggered fire-and-forget from commit_txn when WAL growth since the last
    // checkpoint trips the threshold; auto_checkpoint_in_flight_ dedups so only
    // one runs at a time.
    //
    // M1.1 interaction (INVARIANT). Truncation only removes WAL segments whose
    // records sit AT OR BELOW the checkpoint wal id (everything those records
    // describe is already durable in the storage checkpoint). The bitcask index
    // txn-log frames are a SEPARATE durability channel: they are consumed eagerly
    // at commit time and gated on the WAL committed-transaction set during
    // recovery (M1.1). So the committed-set gate never needs COMMIT markers that
    // live in a truncated segment — those segments only describe rows already
    // folded into the checkpoint, never the txn-id provenance the index recover
    // gate reads. Truncating here cannot strand an index frame's commit decision.
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<void> manager_wal_replicate_t::run_auto_checkpoint(session_id_t session) {
        // Always clear the in-flight guard on every exit path below so a future
        // threshold trip can launch the next checkpoint. enabled_ is implied: the
        // trigger only fires inside the enabled commit_txn path.

        // (a) Flush dirty index btrees first so a post-recovery rebuild starts
        //     from a consistent on-disk index state (mirrors operator_checkpoint).
        if (manager_index_ != actor_zeta::address_t::empty_address()) {
            auto [_fi, fi_fut] =
                actor_zeta::send(manager_index_, &services::index::manager_index_t::flush_all_indexes, session);
            co_await std::move(fi_fut);
        }

        // (b) No disk manager => no storage to checkpoint against. This is the
        //     no-disk test topology, NOT a fallback: without a disk checkpoint
        //     there is no safe truncation boundary, so we clear the guard and stop.
        if (manager_disk_ == actor_zeta::address_t::empty_address()) {
            auto_checkpoint_in_flight_ = false;
            co_return;
        }

        // Snapshot the current WAL id BEFORE the checkpoint so the per-table
        // snapshot pins a known recovery boundary. global_id_ is the monotonic
        // allocator behind next_wal_id(); its current value is the latest issued
        // wal id — the local equivalent of the operator's current_wal_id round-trip.
        const wal::id_t wal_max_id = global_id_.load(std::memory_order_relaxed);

        // Compact watermark for checkpoint_inner's MVCC-gated compact: the
        // dispatcher's visible-to-all horizon. Monotone, so the value going
        // stale across the mailbox hops only DEFERS a compact, never unsafely
        // allows one. 0 when no dispatcher is wired (test topologies): compacts
        // and the affected per-table checkpoints are then skipped this round.
        uint64_t compact_watermark = 0;
        if (manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            auto [_wm, wm_fut] = actor_zeta::send(
                manager_dispatcher_,
                &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wm_fut);
        }

        // (c) checkpoint_all returns the wal id up to which storage is now durable.
        auto [_cp, cp_fut] = actor_zeta::send(manager_disk_,
                                              &services::disk::manager_disk_t::checkpoint_all,
                                              session,
                                              wal_max_id,
                                              compact_watermark);
        const wal::id_t checkpoint_wal_id = co_await std::move(cp_fut);

        // (d) Truncate WAL below the checkpoint boundary. We are already on the WAL
        //     actor, so invoke truncate_before's body directly (co_await the member
        //     coroutine) instead of self-sending another message — the clamp to
        //     active CREATE INDEX build retention runs inside it.
        if (checkpoint_wal_id > wal::id_t{0}) {
            co_await truncate_before(session, checkpoint_wal_id);
        }

        // (e) Release the dedup guard.
        auto_checkpoint_in_flight_ = false;
        co_return;
    }

    // Drop ready fire-and-forget auto-checkpoint futures (loop-thread only).
    // Mirrors manager_dispatcher_t::poll_pending() for pending_void_.
    void manager_wal_replicate_t::poll_auto_checkpoint_() {
        pending_auto_checkpoint_.erase(std::remove_if(pending_auto_checkpoint_.begin(),
                                                      pending_auto_checkpoint_.end(),
                                                      [](unique_future<void>& f) { return f.is_ready(); }),
                                       pending_auto_checkpoint_.end());
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
