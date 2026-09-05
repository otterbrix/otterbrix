#include "transaction_manager.hpp"

#include <algorithm>

namespace components::table {

    transaction_manager_t::transaction_manager_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

    transaction_t& transaction_manager_t::begin_transaction(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto key = session.data();
        if (active_.find(key) != active_.end()) {
            return *active_[key];
        }
        auto txn_id = next_transaction_id_.fetch_add(1);
        auto start_time = current_timestamp_.fetch_add(1);
        // resource_ backs the per-txn pmr containers.
        auto txn = std::make_unique<transaction_t>(txn_id, start_time, session, resource_);
        // Capture + cache the MVCC snapshot under lock_ so later data() reads
        // need not re-lock the manager.
        auto horizon = published_horizon_.load(std::memory_order_relaxed);
        std::pmr::vector<uint64_t> in_flight(in_flight_commits_.begin(), in_flight_commits_.end(), resource_);
        txn->set_snapshot(horizon, std::move(in_flight));
        auto& ref = *txn;
        active_[key] = std::move(txn);
        active_start_times_.insert(start_time);
        return ref;
    }

    uint64_t transaction_manager_t::commit(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto key = session.data();
        auto it = active_.find(key);
        if (it == active_.end()) {
            return 0;
        }
        auto commit_id = current_timestamp_.fetch_add(1);
        it->second->set_commit_id(commit_id);
        it->second->mark_committed();
        active_start_times_.erase(it->second->start_time());
        active_.erase(it);
        // The commit_id is allocated here but not yet visible to snapshots: it
        // becomes visible only when a matching publish(commit_id) runs at the
        // end of the commit pipeline (after WAL fsync + storage_publish_*).
        in_flight_commits_.insert(commit_id);
        return commit_id;
    }

    void transaction_manager_t::restore_commit_clock(uint64_t frontier) {
        // Bootstrap-time, single-threaded (schedulers not started): raise BOTH
        // halves of the monotonic commit clock together from one durable frontier.
        //   * current_timestamp_ → max(current, frontier + 1): the next
        //     begin_transaction's start_time = fetch_add() therefore exceeds every
        //     persisted added_at_commit_id AND every published commit-id, so
        //     post-reopen INSERTs never reuse the durable band; resolve_table keeps
        //     persisted columns visible and reader snapshots judge fresh rows
        //     correctly.
        //   * published_horizon_ → max(current, frontier): post-recovery snapshots
        //     see the persisted commits as published.
        // Raising them in lockstep preserves the invariant
        // current_timestamp_ >= published_horizon_ + 1. Both reopen sites — WAL
        // COMMIT-marker frontier and checkpointed pg_attribute frontier — funnel
        // through here so they cannot disagree.
        // frontier + 1 cannot overflow in practice (commit ids start at 1 and a
        // realistic frontier is far below UINT64_MAX).
        auto cur_ts = current_timestamp_.load(std::memory_order_relaxed);
        if (frontier + 1 > cur_ts) {
            current_timestamp_.store(frontier + 1, std::memory_order_relaxed);
        }
        auto cur_horizon = published_horizon_.load(std::memory_order_relaxed);
        if (frontier > cur_horizon) {
            published_horizon_.store(frontier, std::memory_order_release);
        }
    }

    void transaction_manager_t::publish(uint64_t commit_id) {
        std::lock_guard guard(lock_);
        in_flight_commits_.erase(commit_id);
        // Monotonic advance of published_horizon_ — multiple commits may publish
        // out of allocation order; we keep the max ever published. Snapshots
        // taken after the CAS see the new horizon.
        auto current = published_horizon_.load(std::memory_order_relaxed);
        while (commit_id > current && !published_horizon_.compare_exchange_weak(current,
                                                                                commit_id,
                                                                                std::memory_order_release,
                                                                                std::memory_order_relaxed)) {
            // current updated by CAS on failure — retry
        }
    }

    void transaction_manager_t::discard(uint64_t commit_id) {
        // Only the existing lock_: no new lock, no new ordering edge. No CAS on
        // purpose -- published_horizon_ must not move, or discarding a txn would publish it
        // by the act of forgetting it. See the header for why the erase itself is sound
        // (nothing durable or reader-visible carries a discarded id).
        std::lock_guard guard(lock_);
        in_flight_commits_.erase(commit_id);
    }

    transaction_manager_t::snapshot_t transaction_manager_t::take_snapshot(std::pmr::memory_resource* resource) const {
        std::lock_guard guard(lock_);
        snapshot_t snap{resource};
        snap.snapshot_horizon = published_horizon_.load(std::memory_order_relaxed);
        snap.in_flight_snapshot.assign(in_flight_commits_.begin(), in_flight_commits_.end());
        return snap;
    }

    void transaction_manager_t::abort(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto key = session.data();
        auto it = active_.find(key);
        if (it == active_.end()) {
            return;
        }
        it->second->mark_aborted();
        active_start_times_.erase(it->second->start_time());
        active_.erase(it);
    }

    transaction_t* transaction_manager_t::find_transaction(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto it = active_.find(session.data());
        if (it == active_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    bool transaction_manager_t::has_active_transaction(session::session_id_t session) const {
        std::lock_guard guard(lock_);
        return active_.find(session.data()) != active_.end();
    }

    uint64_t transaction_manager_t::lowest_active_start_time() const {
        std::lock_guard guard(lock_);
        uint64_t lowest = active_start_times_.empty() ? current_timestamp_.load() : *active_start_times_.begin();
        // The vacuum gate must honour the procarray, not just start times: this value feeds
        // cleanup_versions -> chunk_info::cleanup, which collapses every version slot at or
        // below it into "visible to all". Two commit-id populations sit below the lowest
        // active start time and are still not visible to everybody:
        //   * committed-but-unpublished: commit() already erased the txn from active_/
        //     active_start_times_, but until publish() a snapshot taken now still carries
        //     the id in in_flight_snapshot and must not see its rows.
        //   * ids a live snapshot still rejects: one in flight when a reader began stays in
        //     that reader's in_flight_snapshot even after publish() removes it globally.
        // Both clamps mirror visible_to_all_locked(); ids start at 1, so -1 cannot underflow.
        // Same lock, no new ordering edge.
        if (!in_flight_commits_.empty()) {
            lowest = std::min(lowest, *in_flight_commits_.begin() - 1);
        }
        for (const auto& [key, txn] : active_) {
            const auto data = txn->data();
            if (!data.in_flight_snapshot.empty()) {
                // in_flight_snapshot is sorted ascending (copied from a std::set).
                lowest = std::min(lowest, data.in_flight_snapshot.front() - 1);
            }
        }
        return lowest;
    }

    bool transaction_manager_t::has_active_transactions() const {
        std::lock_guard guard(lock_);
        return !active_.empty();
    }

    uint64_t transaction_manager_t::lowest_active_snapshot_horizon() const {
        std::lock_guard guard(lock_);
        // Both public names answer the SAME question — see visible_to_all_locked(). The
        // weaker answer (published_horizon_ when active_ is empty, else min snapshot_horizon
        // alone) is NOT enough: it ignores in_flight_commits_, so it's blind to a SMALLER
        // commit-id still in flight while publish() has already advanced past it (publish()
        // keeps the max ever published; commit() pipelines finish in any order). A snapshot
        // taken in that window still reads the smaller id's rows while the broadcast already
        // licensed the index sweep to erase them — the index then silently answers a subset
        // of the table.
        //
        // Ordering is on READING the horizon, not on publish() (no commit waits for another).
        // Two names are kept for different CONSUMER contracts — DROP tombstone
        // reclaim vs. version-history collapse — not two sources of truth (one computation below
        // feeds both).
        return visible_to_all_locked();
    }

    uint64_t transaction_manager_t::compact_watermark() const {
        std::lock_guard guard(lock_);
        return visible_to_all_locked();
    }

    // Callers hold lock_ (not recursive: both public wrappers take it once, then call here).
    //
    // MONOTONE IN THE SAFE DIRECTION — the property dispatcher.cpp's `new_lowest >
    // last_broadcast_horizon_` gate rests on. Each term only rises: published_horizon_ never
    // decreases (publish() CASes upward); min(in_flight_commits_) never decreases since ids
    // are issued strictly increasing and publish() removes one already-issued id; an active
    // txn's contribution only disappears (raising the min) when it ends.
    uint64_t transaction_manager_t::visible_to_all_locked() const {
        uint64_t watermark = published_horizon_.load(std::memory_order_relaxed);
        // Committed-but-unpublished ids: every snapshot taken from now on carries
        // them in in_flight_snapshot, so nothing at/above the lowest one is
        // visible-to-all yet. Ids start at 1, the -1 cannot underflow.
        if (!in_flight_commits_.empty()) {
            watermark = std::min(watermark, *in_flight_commits_.begin() - 1);
        }
        for (const auto& [key, txn] : active_) {
            const auto data = txn->data();
            watermark = std::min(watermark, data.snapshot_horizon);
            if (!data.in_flight_snapshot.empty()) {
                // in_flight_snapshot is sorted ascending (copied from a std::set).
                watermark = std::min(watermark, data.in_flight_snapshot.front() - 1);
            }
        }
        return watermark;
    }

} // namespace components::table
