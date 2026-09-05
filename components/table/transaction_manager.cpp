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
        if (active_start_times_.empty()) {
            return current_timestamp_.load();
        }
        return *active_start_times_.begin();
    }

    bool transaction_manager_t::has_active_transactions() const {
        std::lock_guard guard(lock_);
        return !active_.empty();
    }

    uint64_t transaction_manager_t::lowest_active_snapshot_horizon() const {
        std::lock_guard guard(lock_);
        // BOTH public names answer the SAME question — see visible_to_all_locked().
        //
        // This used to compute its own, weaker answer: published_horizon_ when
        // active_ was empty, otherwise the min over snapshot_horizon alone. Both
        // branches ignored in_flight_commits_, and the old safety argument here
        // proved only a special case — that a COMMITTING txn cannot reclaim its OWN
        // tombstones early (its remap runs pre-publish, so the horizon reaches its
        // id only after it). It was blind to ANOTHER, SMALLER commit-id still in
        // flight: publish() keeps the MAXIMUM ever published, while commit()
        // pipelines finish in any order, so published_horizon_ routinely sits ABOVE
        // an unpublished id. A snapshot taken in that window carries the smaller id
        // in in_flight_snapshot and still reads its rows, while this broadcast had
        // already licensed the index sweep to erase their entries — the index then
        // answers a subset of the table, silently.
        //
        // Ordering is imposed on READING the horizon, not on publish(): no commit
        // waits for another, so no mutual exclusion is introduced (rule 12).
        //
        // Two names are kept because they carry different CONSUMER contracts —
        // DROP tombstone reclaim vs. version-history collapse — not two sources of
        // truth (rule 16 is about the single computation below, not a single name).
        return visible_to_all_locked();
    }

    uint64_t transaction_manager_t::compact_watermark() const {
        std::lock_guard guard(lock_);
        return visible_to_all_locked();
    }

    // Callers hold lock_. It is NOT recursive: the two public wrappers above each
    // take it exactly once and call in here, so no path locks twice and no new edge
    // is added to the lock order.
    //
    // MONOTONE IN THE SAFE DIRECTION — the property dispatcher.cpp's
    // `new_lowest > last_broadcast_horizon_` gate rests on. Each of the three terms
    // only ever rises: published_horizon_ never decreases (publish() CASes upward);
    // min(in_flight_commits_) never decreases, because ids are handed out strictly
    // increasing and publish() always removes one of the already-issued ids; and an
    // active txn's contribution only disappears (raising the min) when it ends.
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
