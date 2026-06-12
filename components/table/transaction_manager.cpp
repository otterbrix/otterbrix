#include "transaction_manager.hpp"

#include <limits>
#include <stdexcept>

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
        if (active_.empty()) {
            // Empty active_ returns published_horizon_, NOT an in-flight
            // commit id. A commit that ALLOCATED a commit_id but has not yet
            // published sits in in_flight_commits_ (commit() inserts it; publish()
            // erases it and only THEN bumps published_horizon_). With active_ empty
            // this function therefore returns published_horizon_ < that pending
            // commit_id. That cannot reclaim the pending txn's own tombstones early:
            //   * the DROP-GC remap (operator_commit_transaction) stamps the
            //     tombstones' dropped_at == commit_id and runs PRE-publish;
            //   * the agents' sweep keeps a tombstone only while
            //     dropped_at == commit_id < horizon;
            //   * horizon only reaches commit_id AFTER this same txn's publish()
            //     (which is co_awaited AFTER the remap).
            // So during the pre-publish window horizon < commit_id, the sweep's
            // strict `<` fails, and the txn's fresh tombstones survive until its
            // own publish makes the rows visible. No early-reclaim race exists.
            return published_horizon_.load(std::memory_order_acquire);
        }
        // Oldest commit-id horizon any live snapshot can still read below.
        // Same value space as commit_id / published_horizon_ — used by the
        // DROP-GC broadcast so the agents' `dropped_at_commit_id < horizon`
        // sweep compares like with like.
        uint64_t lowest = std::numeric_limits<uint64_t>::max();
        for (const auto& [key, txn] : active_) {
            const auto h = txn->data().snapshot_horizon;
            if (h < lowest) {
                lowest = h;
            }
        }
        return lowest;
    }

    uint64_t transaction_manager_t::compact_watermark() const {
        std::lock_guard guard(lock_);
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
