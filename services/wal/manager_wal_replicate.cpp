#include "manager_wal_replicate.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <mutex>
#include <thread>

#include <actor-zeta/spawn.hpp>
#include <core/executor.hpp>
#include <services/wal/wal_page_reader.hpp>

// Kept out of wal_contract.hpp to avoid an include cycle (these pull back only services/wal/base.hpp).
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/index_rebuild_driver.hpp>
#include <services/index/manager_index.hpp>

namespace services::wal {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_auto_checkpoint_rounds{0};
    } // namespace

    uint64_t auto_checkpoint_rounds() noexcept { return g_auto_checkpoint_rounds.load(std::memory_order_relaxed); }
    void reset_auto_checkpoint_rounds() noexcept { g_auto_checkpoint_rounds.store(0, std::memory_order_relaxed); }
#endif

    namespace {
        uint64_t batch_row_count(const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
            uint64_t total = 0;
            for (const auto& chunk : chunks) {
                total += chunk.size();
            }
            return total;
        }
    } // namespace

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* resource,
                                                     actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config,
                                                     log_t& log,
                                                     actor_zeta::address_t disk_address,
                                                     actor_zeta::address_t index_address)
        : actor_zeta::actor::actor_mixin<manager_wal_replicate_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , manager_disk_(std::move(disk_address))
        , manager_dispatcher_(actor_zeta::address_t::empty_address())
        , manager_index_(std::move(index_address))
        , recovery_error_(core::error_t::no_error()) {
        trace(log_, "manager_wal_replicate start");
        if (!config_.path.empty()) {
            std::filesystem::create_directories(config_.path);
            wal::id_t max_recovered_id = 0;
            for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
                if (!entry.is_directory()) {
                    continue;
                }
                auto db_dir_name = entry.path().filename().string();
                // parse_database_dir_name, not std::stoul+catch (half-parses "9zz"->9), matches wal_reader_t's walk.
                components::catalog::oid_t db_oid;
                if (!parse_database_dir_name(db_dir_name, db_oid)) {
                    warn(log_,
                         "manager_wal_replicate: '{}' under the WAL root is not a database oid directory , "
                         "skipping it (the engine never writes this name)",
                         db_dir_name);
                    continue;
                }
                trace(log_, "manager_wal_replicate: recovering database_oid={}", static_cast<unsigned>(db_oid));

                bool has_wal_segment = false;

                for (const auto& seg : std::filesystem::directory_iterator(entry.path())) {
                    if (!seg.is_regular_file()) {
                        continue;
                    }
                    // Same wal_ prefix as wal_worker_t::discover_segments/wal_reader_t, else a stray file could
                    // poison the allocator.
                    const auto seg_name = seg.path().filename().string();
                    if (seg_name.size() < 4 || seg_name.compare(0, 4, "wal_") != 0) {
                        continue;
                    }
                    has_wal_segment = true;

                    wal_page_reader_t reader(resource_, seg.path());
                    if (!reader.is_open()) {
                        // This scan sets global_id_; an unread segment's ids get reissued, so the refusal
                        // latches every write/commit/truncate below.
                        recovery_error_ = reader.open_error();
                        error(log_,
                              "manager_wal_replicate: segment '{}' could not be read at startup , the WAL "
                              "REFUSES every write until it can be: {}",
                              seg.path().filename().string(),
                              recovery_error_.what);
                        break;
                    }

                    // scan_pages(), not read_all_records(0), which stops at the first broken page (STOP-A) and
                    // hides later ids.
                    const auto scan = reader.scan_pages();
                    if (scan.highest_page_end_lsn > max_recovered_id) {
                        max_recovered_id = scan.highest_page_end_lsn;
                    }
                }
                if (recovery_error_.contains_error()) {
                    break;
                }

                if (!has_wal_segment) {
                    trace(log_,
                          "manager_wal_replicate: '{}' under the WAL root holds no wal segment — a storage "
                          "namespace directory, not a database; no worker spawned",
                          db_dir_name);
                    continue;
                }

                get_or_create_worker(db_oid);
            }
            global_id_.store(max_recovered_id, std::memory_order_relaxed);
        }
        trace(log_, "manager_wal_replicate finish");

        // The loop starts only after recovery above finishes, so it never races the single-threaded scan.
        loop_thread_ = std::thread([this] {
            // this->resource() is qualified since the ctor param `resource` shadows the member function.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());

            while (loop_running_.load(std::memory_order_acquire)) {
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr(raw);
                }

                bool made_progress = false;

                // Unlike manager_dispatcher_t, every send is co_await'ed inline: no pending_<T>_/poll_pending step.

                // pending_msg stays in its slot: the coroutine holds a raw pointer to it across suspension points.
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
#ifdef DEV_MODE
                        services::dispatcher::note_pump_hop();
#endif
                        cont.resume();
                        continue;
                    }
                }

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

                poll_auto_checkpoint_();

                std::unique_lock<std::mutex> lock(mutex_);
                // pump_cv_ only notifies from enqueue_impl, so this timeout IS the per-hop latency while busy (~20
                // hops/stmt); shortening it or the idle tick burns CPU or reopens the push-notify race.
                pump_cv_.wait_for(lock,
                                  in_flight.empty() ? std::chrono::microseconds(100) : std::chrono::microseconds(5));
            }
            // in_flight (and its message_ptr/behavior_t) is destroyed here, on the loop thread, never a sender thread.
        });
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() {
        trace(log_, "delete manager_wal_replicate_t");
        loop_running_.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> guard(mutex_);
            pump_cv_.notify_one();
        }
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drained(raw);
        }
    }

    std::pmr::memory_resource* manager_wal_replicate_t::resource() const noexcept { return resource_; }

    const char* manager_wal_replicate_t::make_type() const noexcept { return "manager_wal_replicate"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_wal_replicate_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
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
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::write_physical_grow>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::write_physical_grow, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_wal_replicate_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        manager_dispatcher_ = std::move(address);
        trace(log_, "manager_wal_replicate::set_manager_dispatcher_sync done");
    }

    wal::id_t manager_wal_replicate_t::next_wal_id() { return ++global_id_; }

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

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    manager_wal_replicate_t::load(session_id_t session, wal::id_t wal_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<std::vector<record_t>>{recovery_error_};
        }

        std::vector<record_t> merged;
        core::error_t first_refusal = core::error_t::no_error();
        for (auto& [db_oid, worker] : wal_actors_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(worker->address(), &wal_worker_t::load, session, wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            // Every worker is drained before the first refusal, so no reply is left addressed to a finished frame.
            auto records = co_await std::move(fut);
            if (records.has_error()) {
                if (!first_refusal.contains_error()) {
                    first_refusal = records.error();
                }
                continue;
            }
            merged.insert(merged.end(),
                          std::make_move_iterator(records.value().begin()),
                          std::make_move_iterator(records.value().end()));
        }
        if (first_refusal.contains_error()) {
            co_return core::result_wrapper_t<std::vector<record_t>>{std::move(first_refusal)};
        }

        std::sort(merged.begin(), merged.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        co_return core::result_wrapper_t<std::vector<record_t>>{std::move(merged)};
    }

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::commit_txn(session_id_t session,
                                        uint64_t txn_id,
                                        wal_sync_mode sync_mode,
                                        components::catalog::oid_t database_oid,
                                        uint64_t commit_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
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
        if (result.has_error()) {
            // The commit marker isn't durable yet (missing from the journal, or under FULL not on the device);
            // returning wal_id would be exactly the lie this channel prevents, so auto-checkpoint is skipped too.
            co_return core::result_wrapper_t<wal::id_t>{result.error()};
        }
        {
            const auto total = total_wal_bytes();
            auto base = wal_bytes_at_last_checkpoint_.load(std::memory_order_relaxed);
            if (total < base) {
                // Directory shrank outside this window; re-baseline avoids under-reporting (test_ssb_load_scaling).
                wal_bytes_at_last_checkpoint_.store(total, std::memory_order_relaxed);
                base = total;
            }
            wal_bytes_since_checkpoint_.store(total - base, std::memory_order_relaxed);
        }

        // The byte counter resets here (trigger time), not at completion. This self-sends rather
        // than routing through the pipeline so the checkpoint never sits on a committer's latency
        // path (services/wal/wal_contract.hpp) -- a statement IS above this frame
        // (operator_commit_transaction sends commit_txn and awaits it), so the refusal is logged
        // and the round abandoned instead of travelling back up.
        auto trigger_auto_checkpoint = [&] {
            if (needs_auto_checkpoint() && !auto_checkpoint_in_flight_) {
                auto_checkpoint_in_flight_ = true;
                reset_auto_checkpoint_bytes();
                auto [_ac, ac_fut] =
                    actor_zeta::otterbrix::send(address(), &manager_wal_replicate_t::run_auto_checkpoint, session);
                // needs_sched is always false (enqueue_impl just wakes the loop); the [[nodiscard]] future is
                // parked for poll_auto_checkpoint_.
                pending_auto_checkpoint_.emplace_back(std::move(ac_fut));
            }
        };
        trigger_auto_checkpoint();
        co_return core::result_wrapper_t<wal::id_t>{result.value()};
    }

    std::uintmax_t manager_wal_replicate_t::total_wal_bytes() const noexcept {
        if (config_.path.empty())
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
                const auto seg_name = seg.path().filename().string();
                if (seg_name.size() < 4 || seg_name.compare(0, 4, "wal_") != 0) {
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

    manager_wal_replicate_t::unique_future<core::error_t>
    manager_wal_replicate_t::truncate_before(session_id_t session, wal::id_t checkpoint_wal_id) {
        if (recovery_error_.contains_error()) {
            co_return recovery_error_;
        }

        core::error_t first_refusal = core::error_t::no_error();
        for (auto& [db_oid, worker] : wal_actors_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                                  &wal_worker_t::truncate_before,
                                                                  session,
                                                                  checkpoint_wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            if (auto worker_error = co_await std::move(fut);
                worker_error.contains_error() && !first_refusal.contains_error()) {
                first_refusal = worker_error;
            }
        }
        co_return first_refusal;
    }

    // Mirrors CHECKPOINT (operator_checkpoint.cpp); truncation never touches the txn-id provenance the index recover
    // gate reads. No second trigger: no statement above has an error channel, and compaction renumbers row ids.
    manager_wal_replicate_t::unique_future<void> manager_wal_replicate_t::run_auto_checkpoint(session_id_t session) {
        // Every exit below must call end_auto_checkpoint_round(); skipping it would suppress every later round.

        // flush_all_indexes arms the durable rebuild_marker_path_ guard, the only pre-rebuild report on index state.
        if (manager_index_ != actor_zeta::address_t::empty_address()) {
            auto [_fi, fi_fut] = actor_zeta::otterbrix::send(manager_index_,
                                                             &services::index::manager_index_t::flush_all_indexes,
                                                             session);
            // Logged, not propagated (nothing above this frame is a statement); the round stops here rather than
            // dropping WAL segments that are the index's only remaining copy, and the guard releases for the next trip.
            if (auto flush_error = co_await std::move(fi_fut); flush_error.contains_error()) {
                error(log_,
                      "manager_wal_replicate_t::run_auto_checkpoint: index flush did not reach the disk, "
                      "the round is abandoned rather than truncating the WAL behind it: {}",
                      flush_error.what);
                end_auto_checkpoint_round();
                co_return;
            }
        }

        // No disk manager means the no-disk topology, not a fallback: no safe truncation boundary, so the round stops.
        if (manager_disk_ == actor_zeta::address_t::empty_address()) {
            end_auto_checkpoint_round();
            co_return;
        }

        const wal::id_t wal_max_id = global_id_.load(std::memory_order_relaxed);

        // The compact watermark is the dispatcher's visible-to-all horizon; being monotone, mailbox-hop staleness only
        // defers a compact, never allows one unsafely (0 with no dispatcher wired).
        uint64_t compact_watermark = 0;
        if (manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            auto [_wm, wm_fut] =
                actor_zeta::otterbrix::send(manager_dispatcher_,
                                            &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wm_fut);
        }

        auto [_cp, cp_fut] = actor_zeta::otterbrix::send(manager_disk_,
                                                         &services::disk::manager_disk_t::checkpoint_all,
                                                         session,
                                                         wal_max_id,
                                                         compact_watermark);
        const wal::id_t checkpoint_wal_id = co_await std::move(cp_fut);

        // A compact renumbers every surviving row, so skipping the rebuild leaves indexes silently wrong. Must precede
        // the truncate below (test_checkpoint_rebuild_before_truncate): a refused rebuild leaves index state unknown,
        // and truncation is irreversible. A persistent refusal grows the journal unbounded (deliberate); DROP INDEX on
        // the offender is the way out (test_auto_checkpoint_rebuild_refusal).
        if (manager_index_ != actor_zeta::address_t::empty_address()) {
            auto rebuild_error = co_await services::index::repopulate_indexes_after_compaction(
                resource(),
                manager_disk_,
                manager_index_,
                session,
                services::index::committed_rows_snapshot(),
                core::date::timezone_offset_t{});
            if (rebuild_error.contains_error()) {
                error(log_,
                      "manager_wal_replicate_t::run_auto_checkpoint: index rebuild after the compacting round "
                      "failed, so some indexes may still name pre-compact row ids and which ones is unknown; "
                      "the round is abandoned without truncating, because truncation is its irreversible step "
                      "and a round that failed its last recoverable one must not take it: {}",
                      rebuild_error.what);
                end_auto_checkpoint_round();
                co_return;
            }
        }

        if (checkpoint_wal_id > wal::id_t{0}) {
            // Logged, not propagated (same reason as above); segments stay in place (the safe side), so the next round
            // retries.
            if (auto truncate_error = co_await truncate_before(session, checkpoint_wal_id);
                truncate_error.contains_error()) {
                error(log_,
                      "manager_wal_replicate_t::run_auto_checkpoint: the WAL truncate was refused , the "
                      "segments are left in place: {}",
                      truncate_error.what);
            }
        }

        // The rebase happens here, not at trigger time, since the truncate above shrinks the directory; the
        // pre-truncate size would re-trip or stay suppressed too long.
        end_auto_checkpoint_round();
        co_return;
    }

    void manager_wal_replicate_t::end_auto_checkpoint_round() noexcept {
        rebase_auto_checkpoint_window();
        auto_checkpoint_in_flight_ = false;
#ifdef DEV_MODE
        g_auto_checkpoint_rounds.fetch_add(1, std::memory_order_relaxed);
#endif
    }

    // Loop-thread only; mirrors manager_dispatcher_t::poll_pending().
    void manager_wal_replicate_t::poll_auto_checkpoint_() {
        pending_auto_checkpoint_.erase(std::remove_if(pending_auto_checkpoint_.begin(),
                                                      pending_auto_checkpoint_.end(),
                                                      [](unique_future<void>& f) { return f.is_ready(); }),
                                       pending_auto_checkpoint_.end());
    }

    manager_wal_replicate_t::unique_future<wal::id_t> manager_wal_replicate_t::current_wal_id(session_id_t session) {
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

    // Worker keying uses database_oid as a single main_database today; table_oid isn't used for routing yet.
    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_insert(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                   uint64_t row_start,
                                                   uint64_t row_count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        // An empty batch is a legitimate no-op, not a refusal; refusals travel in result_wrapper_t.
        if (batch_row_count(chunks) == 0) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_insert,
                                                              session,
                                                              table_oid,
                                                              std::move(chunks),
                                                              row_start,
                                                              row_count,
                                                              txn_id,
                                                              wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return std::move(result);
    }

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_delete(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
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
        co_return std::move(result);
    }

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_update(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   std::pmr::vector<components::vector::data_chunk_t> new_data,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (batch_row_count(new_data) == 0) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
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
        co_return std::move(result);
    }

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_grow(session_id_t session,
                                                 components::catalog::oid_t table_oid,
                                                 std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                                 uint64_t column_count,
                                                 std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                 uint64_t row_start,
                                                 uint64_t row_count,
                                                 uint64_t txn_id,
                                                 components::catalog::oid_t database_oid) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }

        auto* worker = get_or_create_worker(database_oid);
        const auto add_column_id = next_wal_id();
        const auto insert_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_grow,
                                                              session,
                                                              table_oid,
                                                              std::move(schema_chunk),
                                                              column_count,
                                                              std::move(chunks),
                                                              row_start,
                                                              row_count,
                                                              txn_id,
                                                              add_column_id,
                                                              insert_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return std::move(result);
    }
} // namespace services::wal
