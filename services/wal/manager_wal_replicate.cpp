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
#include <core/pipeline_bypass.hpp>
#include <services/wal/wal_page_reader.hpp>

// Needed for the auto-checkpoint orchestration (run_auto_checkpoint): the WAL
// manager drives flush_all_indexes on the index manager and checkpoint_all on
// the disk manager (+ the compact-watermark fetch from the dispatcher).
// wal_contract.hpp stays free of these to avoid the cycle
// (manager_disk.hpp / manager_index.hpp pull only services/wal/base.hpp back).
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
        // Total rows across a chunk batch — used to skip writing an empty record.
        uint64_t batch_row_count(const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
            uint64_t total = 0;
            for (const auto& chunk : chunks) {
                total += chunk.size();
            }
            return total;
        }
    } // namespace

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
        , manager_index_(actor_zeta::address_t::empty_address())
        , recovery_error_(core::error_t::no_error()) {
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
                // A DATABASE DIRECTORY IS NAMED std::to_string(oid) AND NOTHING ELSE — the
                // worker's own constructor is the only writer of these names. std::stoul under
                // catch (...) both used exceptions as control flow and HALF-PARSED foreign
                // names: "9zz" answered 9, and a worker was spawned over directory "9" — a
                // DIFFERENT path from the one the files are in, splitting the journal in two.
                // parse_database_dir_name (base.hpp) is THE classification, shared with
                // wal_reader_t's replay walk; everything foreign is skipped LOUDLY
                // (nothing the engine wrote is ever skipped by this).
                components::catalog::oid_t db_oid;
                if (!parse_database_dir_name(db_dir_name, db_oid)) {
                    warn(log_,
                         "manager_wal_replicate: '{}' under the WAL root is not a database oid directory , "
                         "skipping it (the engine never writes this name)",
                         db_dir_name);
                    continue;
                }
                trace(log_, "manager_wal_replicate: recovering database_oid={}", static_cast<unsigned>(db_oid));

                // Scan segments to find max wal_id (via reader, no actor messaging).
                for (const auto& seg : std::filesystem::directory_iterator(entry.path())) {
                    if (!seg.is_regular_file()) {
                        continue;
                    }
                    // SEGMENTS ONLY, by the same prefix wal_worker_t::discover_segments and
                    // wal_reader_t use. The scan below trusts a verified page header, so an
                    // unrelated file that happened to checksum-clean would poison the
                    // allocator; the record decode this replaced could not be poisoned that
                    // way, so the filter arrives with it.
                    const auto seg_name = seg.path().filename().string();
                    if (seg_name.size() < 4 || seg_name.compare(0, 4, "wal_") != 0) {
                        continue;
                    }

                    wal_page_reader_t reader(resource_, seg.path());
                    if (!reader.is_open()) {
                        // THIS SCAN IS WHAT SETS global_id_. A segment it could not read holds
                        // ids it will not see, so max_recovered_id lands below them and
                        // next_wal_id() starts handing the same ids out again — over records
                        // that are still on disk. Latch the refusal: every write, commit and
                        // truncate below answers with it rather than issuing an id that
                        // collides with the journal.
                        recovery_error_ = reader.open_error();
                        error(log_,
                              "manager_wal_replicate: segment '{}' could not be read at startup , the WAL "
                              "REFUSES every write until it can be: {}",
                              seg.path().filename().string(),
                              recovery_error_.what);
                        break;
                    }

                    // THE ALLOCATOR BOUND IS A PROPERTY OF THE FILES, NOT OF THE REPLAY.
                    // Decoding every record with read_all_records(0) stops at the first broken
                    // page (STOP-A) — so ids living in the pages AFTER a corruption point are
                    // invisible here and next_wal_id() reissues them over records that are
                    // still on disk. scan_pages() reads every page and takes page_end_lsn from
                    // the ones whose checksum still vouches for it, which also spares startup
                    // a full decode of the journal.
                    const auto scan = reader.scan_pages();
                    if (scan.highest_page_end_lsn > max_recovered_id) {
                        max_recovered_id = scan.highest_page_end_lsn;
                    }
                }
                if (recovery_error_.contains_error()) {
                    break;
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
#ifdef DEV_MODE
                        services::dispatcher::note_pump_hop();
#endif
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

                std::unique_lock<std::mutex> lock(mutex_);
                // A suspended coroutine's future is completed on ANOTHER thread and
                // notifies nobody — pump_cv_ is signalled from enqueue_impl alone — so
                // readiness is discovered by this wait TIMING OUT. While work is in
                // flight that expiry IS the per-hop latency, and a statement crosses
                // ~20 hops. Idle is the opposite case: only a new message can arrive
                // and that DOES notify, so the idle tick is left alone — shortening it
                // burns CPU for nothing, lengthening it would expose the documented
                // push-notify race to the first statement after a pause.
                pump_cv_.wait_for(lock,
                                  in_flight.empty() ? std::chrono::microseconds(100) : std::chrono::microseconds(5));
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
            case actor_zeta::msg_id<manager_wal_replicate_t, &manager_wal_replicate_t::write_physical_add_column>: {
                co_await actor_zeta::dispatch(this, &manager_wal_replicate_t::write_physical_add_column, msg);
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
        // Invariant: every unregister matches a prior register; a mismatch is an
        // operator_create_index lifecycle bug. IT IS REPORTED, NOT EXECUTED: this path is fed
        // by messages from ANOTHER actor, and aborting here turned a bookkeeping bug into a
        // process death. The harm an unmatched unregister could actually do — releasing a
        // clamp some other build still holds — is prevented structurally: exactly ONE entry
        // is erased (never every entry at the value; the container is a multiset so paired
        // register/unregister at the same position stay balanced), and an unmatched one
        // erases nothing at all.
        auto it = active_build_start_positions_.find(build_start_wal_position);
        if (it == active_build_start_positions_.end()) {
            error(log_,
                  "manager_wal_replicate::unregister_active_build_sync wal_id={} has NO matching register , "
                  "ignored — a create-index lifecycle bug, and the retention set is left as it stands",
                  build_start_wal_position);
            return;
        }
        active_build_start_positions_.erase(it);
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

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    manager_wal_replicate_t::load(session_id_t session, wal::id_t wal_id) {
        if (!enabled_) {
            co_return core::result_wrapper_t<std::vector<record_t>>{std::vector<record_t>{}};
        }
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<std::vector<record_t>>{recovery_error_};
        }

        // Collect records from ALL workers, merge-sort by wal_id.
        std::vector<record_t> merged;
        core::error_t first_refusal = core::error_t::no_error();
        for (auto& [db_oid, worker] : wal_actors_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(worker->address(), &wal_worker_t::load, session, wal_id);
            if (needs_sched) {
                scheduler_->enqueue(worker.get());
            }
            // Drain EVERY worker before taking the first refusal: abandoning a future leaves a
            // reply addressed to a frame that has already finished.
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

    // -----------------------------------------------------------------------
    // Contract: commit_txn
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::commit_txn(session_id_t session,
                                        uint64_t txn_id,
                                        wal_sync_mode sync_mode,
                                        components::catalog::oid_t database_oid,
                                        uint64_t commit_id) {
        if (!enabled_) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
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
            // The COMMIT MARKER is not in the journal (or, under FULL, not on the device).
            // Returning the wal_id here is exactly the durability lie this channel exists to
            // stop, and the auto-checkpoint below must not run either: it would truncate the
            // WAL behind a commit that did not land.
            co_return core::result_wrapper_t<wal::id_t>{result.error()};
        }
        // Storing the total WAL directory size here instead of the growth since the last
        // checkpoint left the threshold tripped forever once crossed.
        {
            const auto total = total_wal_bytes();
            auto base = wal_bytes_at_last_checkpoint_.load(std::memory_order_relaxed);
            if (total < base) {
                // The directory shrank behind us — something truncated the WAL without going through
                // the checkpoint that owns this window. Re-baseline instead of measuring against a
                // size that no longer exists, which would under-report growth until the WAL climbed
                // back past it. No live path does this today (test_ssb_load_scaling covers the
                // explicit-checkpoint case), but the subtraction has to be defined for an input
                // that can occur.
                wal_bytes_at_last_checkpoint_.store(total, std::memory_order_relaxed);
                base = total;
            }
            wal_bytes_since_checkpoint_.store(total - base, std::memory_order_relaxed);
        }

        // Auto-checkpoint trigger. needs_auto_checkpoint() compares WAL bytes written SINCE the
        // last checkpoint (not total WAL size) against the configured
        // auto_checkpoint_threshold_bytes; auto_checkpoint_in_flight_ dedups, so a burst of
        // threshold-tripping commits cannot stack concurrent checkpoints. The byte counter is reset
        // HERE, at trigger time, so commits racing in behind this one accumulate against a fresh
        // window toward the NEXT checkpoint instead of re-tripping the same one. The self-send is
        // fire-and-forget because the round (index flush + storage checkpoint + WAL truncate) is
        // heavy and must NOT extend the committer's commit_txn latency: it lands in inbox_ and the
        // loop runs run_auto_checkpoint as an independent in-flight entry after this coroutine
        // returns its wal_id to the caller.
        //
        // BYPASS (3) OF 3, DECLARED — see core/pipeline_bypass.hpp for the rule and the whole list.
        // It starts a full checkpoint (index flush, storage compaction that RENUMBERS physical row
        // ids, index clear-and-rebuild, WAL segment unlink) with no logical plan, no planner, no
        // optimizer and no operator behind it. Unlike the other two it does NOT run where the
        // pipeline is unusable: the engine is fully up, and the same work has a statement form
        // (operator_checkpoint, which ~base_otterbrix_t reaches by building a node_checkpoint plan).
        // So this declaration records a bypass that exists, not one judged unavoidable; settling it
        // means either routing this trigger through a node_checkpoint plan or accepting the
        // self-send deliberately. What it already costs: with no statement above the frame there is
        // no error channel, so run_auto_checkpoint LOGS a refusal and ABANDONS the round where an
        // operator would fail its statement (see (c2) there).
        auto trigger_auto_checkpoint =
            core::maintenance::pipeline_bypass<core::maintenance::bypass_site::wal_auto_checkpoint>([&] {
                if (needs_auto_checkpoint() && !auto_checkpoint_in_flight_) {
                    auto_checkpoint_in_flight_ = true;
                    reset_auto_checkpoint_bytes();
                    auto [_ac, ac_fut] =
                        actor_zeta::otterbrix::send(address(), &manager_wal_replicate_t::run_auto_checkpoint, session);
                    // needs_sched is always false: enqueue_impl only pushes to inbox_ and
                    // wakes the loop (no scheduler hop). Park the [[nodiscard]] future;
                    // the loop drains it once ready (poll_auto_checkpoint_).
                    pending_auto_checkpoint_.emplace_back(std::move(ac_fut));
                }
            });
        trigger_auto_checkpoint();
        co_return core::result_wrapper_t<wal::id_t>{result.value()};
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
                // SEGMENTS ONLY, same prefix filter as discover_segments and the startup
                // scan: the journal and the table tree share this root on purpose, so a
                // neighbour that is not wal_* must not widen the auto-checkpoint window.
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

    // -----------------------------------------------------------------------
    // Contract: truncate_before
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<core::error_t>
    manager_wal_replicate_t::truncate_before(session_id_t session, wal::id_t checkpoint_wal_id) {
        if (!enabled_) {
            co_return core::error_t::no_error();
        }
        if (recovery_error_.contains_error()) {
            co_return recovery_error_;
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

        // Send to ALL workers. Every future is drained before the first refusal is taken:
        // abandoning one leaves a reply addressed to a frame that has already finished.
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

    // -----------------------------------------------------------------------
    // Contract: run_auto_checkpoint
    //
    // Self-orchestrated analogue of the CHECKPOINT statement operator
    // (operator_checkpoint.cpp): flush indexes -> snapshot current wal id ->
    // checkpoint_all storage -> truncate the WAL below the returned checkpoint id.
    // Triggered fire-and-forget from commit_txn when WAL growth since the last
    // checkpoint trips the threshold; auto_checkpoint_in_flight_ dedups.
    //
    // INTERACTION WITH THE INDEX RECOVER GATE (INVARIANT). Truncation removes only segments whose
    // records sit AT OR BELOW the checkpoint wal id — rows already durable in the storage
    // checkpoint, never the txn-id provenance the gate reads. The bitcask index txn-log frames are a
    // SEPARATE durability channel: consumed eagerly at commit time and gated on the WAL
    // committed-transaction set during recovery. Truncating here cannot strand an index frame's
    // commit decision.
    // -----------------------------------------------------------------------

    // Body of declared bypass (3) — core/pipeline_bypass.hpp lists it; the declaration itself sits
    // at the only trigger in the tree, the threshold test in commit_txn above. Do NOT add a second
    // trigger: a checkpoint that no statement asked for has no error channel to fail into, and its
    // compaction renumbers physical row ids under every index of every table it touches.
    manager_wal_replicate_t::unique_future<void> manager_wal_replicate_t::run_auto_checkpoint(session_id_t session) {
        // EVERY EXIT BELOW GOES THROUGH end_auto_checkpoint_round(), which rebases the byte
        // window and clears the in-flight guard so a future threshold trip can launch the next
        // round. That is not bookkeeping: three of the four exits ABANDON the round, and an
        // abandoned round is only an acceptable answer here because the next one repeats it.
        // A new exit that returns without it would suppress every later round forever.
        // enabled_ is implied: the trigger only fires inside the enabled commit_txn path.

        // (a) Flush the indexes. TWO THINGS HAPPEN IN THIS ONE MESSAGE, and neither is "so a
        //     post-recovery rebuild starts from a consistent on-disk index state" — there is no
        //     post-recovery rebuild, that pass was removed from base_spaces as a proven no-op.
        //       * manager_index_t::flush_all_indexes ARMS the durable "these indexes are about to be
        //         renumbered and are not yet rebuilt" guard (manager_index_t::rebuild_marker_path_).
        //         This handler is the first step of both compacting orchestrations and is sent from
        //         nowhere else, so it is where "a compacting round is starting" can be written down;
        //         a guard that could not be made durable comes back as the refusal below and ends
        //         the round BEFORE it renumbers anything;
        //       * the flush is the only REPORT on the health of the indexes' existing durable state,
        //         taken before step (c2)'s rebuild clears and re-creates the stores (see
        //         operator_checkpoint.cpp for the measurement).
        if (manager_index_ != actor_zeta::address_t::empty_address()) {
            auto [_fi, fi_fut] = actor_zeta::otterbrix::send(manager_index_,
                                                             &services::index::manager_index_t::flush_all_indexes,
                                                             session);
            // A refusal is logged rather than propagated, for the same reason the rebuild
            // refusal below is: nothing above this frame is a statement that could carry it.
            // But the round STOPS here — the truncate in (d) would otherwise drop the WAL
            // segments that are the only remaining copy of what the index failed to write.
            // The guard is released so the next threshold trip retries the whole round.
            if (auto flush_error = co_await std::move(fi_fut); flush_error.contains_error()) {
                error(log_,
                      "manager_wal_replicate_t::run_auto_checkpoint: index flush did not reach the disk, "
                      "the round is abandoned rather than truncating the WAL behind it: {}",
                      flush_error.what);
                end_auto_checkpoint_round();
                co_return;
            }
        }

        // (b) No disk manager => no storage to checkpoint against. This is the
        //     no-disk test topology, NOT a fallback: without a disk checkpoint
        //     there is no safe truncation boundary, so we clear the guard and stop.
        if (manager_disk_ == actor_zeta::address_t::empty_address()) {
            end_auto_checkpoint_round();
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
            auto [_wm, wm_fut] =
                actor_zeta::otterbrix::send(manager_dispatcher_,
                                            &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wm_fut);
        }

        // (c) checkpoint_all returns the wal id up to which storage is now durable.
        auto [_cp, cp_fut] = actor_zeta::otterbrix::send(manager_disk_,
                                                         &services::disk::manager_disk_t::checkpoint_all,
                                                         session,
                                                         wal_max_id,
                                                         compact_watermark);
        const wal::id_t checkpoint_wal_id = co_await std::move(cp_fut);

        // (c2) INDEX REBUILD, without which the round silently invalidates every index of every
        //      table it compacts: checkpoint_all compacts every entry the MVCC gate lets it
        //      (agent_disk_t::checkpoint_inner -> data_table_t::compact), and a compact rebuilds the
        //      table at row id 0, handing every survivor a NEW physical id. An index entry stores
        //      that id, so once the round commits its header every index of a compacted table is
        //      wrong -- silently: an id that names no row group is dropped by collection_t::fetch (a
        //      short answer), an id that now belongs to a different survivor is gathered as the
        //      match (a wrong answer, observed as `WHERE k = <key of row A>` returning row B).
        //
        //      It calls the SAME driver as the CHECKPOINT statement operator, which is also what
        //      DISARMS the guard step (a) armed: repopulate_table clears a table's marker only after
        //      that table's agents have published and force_flushed. A round abandoned below leaves
        //      the guard STANDING, so the next start declines to wire those indexes instead of
        //      answering from stores that may name pre-compact rows.
        //
        //      IT MUST STAY AHEAD OF THE TRUNCATE AT (d): truncation is the only step of the round
        //      that destroys anything, so a refused rebuild has to be able to end the round with the
        //      journal still intact. Pinned by test_checkpoint_rebuild_before_truncate; the rule also
        //      lives on the driver's own declaration so a third orchestration inherits it.
        //
        //      The snapshot is committed_rows_snapshot(): this is not a statement and owns no
        //      transaction, and an index holds every committed row (the TABLE decides what a reader
        //      may see, so the index answers a superset and never filters by visibility).
        //
        //      A refusal is LOGGED (nothing above this frame is a statement that could carry it) and
        //      the round ENDS HERE rather than falling through into (d). Not because truncating
        //      would destroy anything recoverable -- it removes only segments at or below the
        //      checkpoint id, and the rebuild reads STORAGE, never the journal (index_rebuild_driver.cpp
        //      pulls rows through storage_fetch_next_batch and never touches the WAL) -- but because a
        //      refused rebuild leaves the index state UNKNOWN: the driver stops at the first oid it
        //      could not rebuild, so some indexes of this round are current and others pre-compact
        //      and the round cannot say which. Truncation is its POINT OF NO RETURN, and a round
        //      that failed its last recoverable step must not take an irreversible one.
        //
        //      Abandoning is only an answer because it REPEATS: end_auto_checkpoint_round() rebases
        //      the byte window on the journal as it stands (un-truncated, so larger) and releases
        //      the dedup guard, so the next threshold trip launches a fresh round; the compaction
        //      this round committed is durable and is not redone. THE PRICE, NAMED: that answers a
        //      TRANSIENT refusal, which is what the test injects. Under a PERSISTENT one the journal
        //      grows by the threshold every round and NOTHING trims it — both callers of the driver
        //      return above truncate_before and there is no third. Deliberate (an unbounded journal
        //      is recoverable, an unknown index is not), but a real cost; the way out is DROP INDEX
        //      on the index that cannot be rebuilt. Pinned by test_auto_checkpoint_rebuild_refusal.
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

        // (d) Truncate WAL below the checkpoint boundary. We are already on the WAL
        //     actor, so invoke truncate_before's body directly (co_await the member
        //     coroutine) instead of self-sending another message — the clamp to
        //     active CREATE INDEX build retention runs inside it.
        if (checkpoint_wal_id > wal::id_t{0}) {
            // Logged, not propagated, for the same reason (a) and (c2) are: nothing above this
            // frame is a statement that could carry it. A refused truncate leaves the segments
            // in place, which is the SAFE side of this operation — the next round retries.
            if (auto truncate_error = co_await truncate_before(session, checkpoint_wal_id);
                truncate_error.contains_error()) {
                error(log_,
                      "manager_wal_replicate_t::run_auto_checkpoint: the WAL truncate was refused , the "
                      "segments are left in place: {}",
                      truncate_error.what);
            }
        }

        // (e) Rebase the byte window on the post-truncate size and release the dedup guard. The
        //     rebase must happen HERE rather than at trigger time: the truncate above is what
        //     shrinks the directory, and a window based on the pre-truncate size would either
        //     re-trip immediately or stay suppressed for a whole extra checkpoint's worth of WAL.
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

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_insert(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                   uint64_t row_start,
                                                   uint64_t row_count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        // A zero id from these two legs means "nothing was asked to be written" and stays a
        // legitimate no-op; the wrapper is what a REFUSAL travels in, so the two can no
        // longer be confused at a call site.
        if (!enabled_) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
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

    // -----------------------------------------------------------------------
    // Contract: write_physical_delete
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_delete(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
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

    // -----------------------------------------------------------------------
    // Contract: write_physical_update
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_update(session_id_t session,
                                                   components::catalog::oid_t table_oid,
                                                   std::pmr::vector<int64_t> row_ids,
                                                   std::pmr::vector<components::vector::data_chunk_t> new_data,
                                                   uint64_t count,
                                                   uint64_t txn_id,
                                                   components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
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

    // -----------------------------------------------------------------------
    // Contract: write_physical_add_column
    // -----------------------------------------------------------------------

    manager_wal_replicate_t::unique_future<core::result_wrapper_t<wal::id_t>>
    manager_wal_replicate_t::write_physical_add_column(session_id_t session,
                                                       components::catalog::oid_t table_oid,
                                                       std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                                       uint64_t column_count,
                                                       uint64_t txn_id,
                                                       components::catalog::oid_t database_oid) {
        if (!enabled_) {
            co_return core::result_wrapper_t<wal::id_t>{wal::id_t{0}};
        }
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }

        auto* worker = get_or_create_worker(database_oid);
        auto wal_id = next_wal_id();
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(worker->address(),
                                                              &wal_worker_t::write_physical_add_column,
                                                              session,
                                                              table_oid,
                                                              std::move(schema_chunk),
                                                              column_count,
                                                              txn_id,
                                                              wal_id);
        if (needs_sched) {
            scheduler_->enqueue(worker);
        }
        auto result = co_await std::move(fut);
        co_return std::move(result);
    }

} // namespace services::wal
