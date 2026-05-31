#include "transaction_manager.hpp"

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
        // Block C §3.5 dec 22 + Block E: pass the manager's resource so the
        // per-txn pmr containers (in_flight_snapshot + pending_base_*) can
        // allocate from it.
        auto txn = std::make_unique<transaction_t>(txn_id, start_time, session, resource_);
        // Block E ProcArray (Pass 9 dec 46): snapshot captured atomically under
        // lock_, then cached on the transaction_t. Subsequent transaction_t::data()
        // calls return the cached snapshot by value-copy (BLOCKER 8 mitigation —
        // the vector copy is O(active in-flight commits), typically <100).
        auto horizon = published_horizon_.load(std::memory_order_relaxed);
        std::pmr::vector<uint64_t> in_flight(in_flight_commits_.begin(),
                                             in_flight_commits_.end(),
                                             resource_);
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
        // Block E ProcArray: the commit_id is allocated but not yet visible. It
        // becomes visible only after a matching publish(commit_id) is called by
        // executor / operator_commit_transaction at the end of the commit
        // pipeline (after WAL fsync + storage_publish_*).
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
        while (commit_id > current &&
               !published_horizon_.compare_exchange_weak(current,
                                                         commit_id,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed)) {
            // current updated by CAS on failure — retry
        }
    }

    transaction_manager_t::snapshot_t
    transaction_manager_t::take_snapshot(std::pmr::memory_resource* resource) const {
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

} // namespace components::table
