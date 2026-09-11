#include "transaction_manager.hpp"

#include <algorithm>
#include <cassert>

namespace components::table {

    transaction_manager_t::transaction_manager_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

    transaction_t& transaction_manager_t::begin_transaction(session::session_id_t session, transaction_scope_t scope) {
        std::lock_guard guard(lock_);
        assert(active_.find(session) == active_.end() && "begin_transaction opens; resolve_transaction joins");
        return open_locked(session, scope);
    }

    core::result_wrapper_t<transaction_t*> transaction_manager_t::resolve_transaction(session::session_id_t session,
                                                                                      transaction_scope_t scope,
                                                                                      transaction_control_t control) {
        std::lock_guard guard(lock_);
        auto it = active_.find(session);
        if (it == active_.end()) {
            return &open_locked(session, scope);
        }
        auto& txn = *it->second;
        if (txn.state() == transaction_state_t::failed) {
            if (control == transaction_control_t::none) {
                return core::error_t{core::error_code_t::transaction_finalized,
                                     std::pmr::string{"the transaction failed; only ROLLBACK is accepted until it ends",
                                                      resource_}};
            }
            // Its work was undone when it failed; ending it is all that is left.
            txn.mark_aborted();
            active_.erase(it);
            if (control == transaction_control_t::commit) {
                return core::error_t{core::error_code_t::transaction_finalized,
                                     std::pmr::string{"the transaction failed and was rolled back; nothing was "
                                                      "committed",
                                                      resource_}};
            }
            return &open_locked(session, scope);
        }
        if (txn.scope() == transaction_scope_t::statement) {
            return core::error_t{core::error_code_t::other_error,
                                 std::pmr::string{"the session's transaction belongs to a statement that has not "
                                                  "ended yet",
                                                  resource_}};
        }
        return &txn;
    }

    transaction_t& transaction_manager_t::open_locked(session::session_id_t session, transaction_scope_t scope) {
        auto txn_id = next_transaction_id_.fetch_add(1);
        auto start_time = current_timestamp_.fetch_add(1);
        auto txn = std::make_unique<transaction_t>(txn_id, start_time, session, scope, resource_);
        auto horizon = published_horizon_.load(std::memory_order_relaxed);
        std::pmr::vector<uint64_t> in_flight(in_flight_commits_.begin(), in_flight_commits_.end(), resource_);
        txn->set_snapshot(horizon, std::move(in_flight));
        auto& ref = *txn;
        active_[session] = std::move(txn);
        active_start_times_.insert(start_time);
        return ref;
    }

    uint64_t transaction_manager_t::commit(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto it = active_.find(session);
        if (it == active_.end()) {
            return 0;
        }
        auto commit_id = current_timestamp_.fetch_add(1);
        it->second->set_commit_id(commit_id);
        it->second->mark_committed();
        active_start_times_.erase(it->second->start_time());
        active_.erase(it);
        in_flight_commits_.insert(commit_id);
        return commit_id;
    }

    void transaction_manager_t::restore_commit_clock(uint64_t frontier) {
        // Bootstrap-time, single-threaded: raises current_timestamp_ (-> frontier + 1) and
        // published_horizon_ (-> frontier) together so post-reopen commits can't reuse the durable band.
        // Both reopen sites -- the WAL COMMIT-marker frontier and the checkpointed pg_attribute frontier --
        // funnel through here, so they cannot disagree.
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
        // CAS loop: multiple commits may publish out of order; we keep the max ever published.
        auto current = published_horizon_.load(std::memory_order_relaxed);
        while (commit_id > current && !published_horizon_.compare_exchange_weak(current,
                                                                                commit_id,
                                                                                std::memory_order_release,
                                                                                std::memory_order_relaxed)) {
        }
    }

    void transaction_manager_t::discard(uint64_t commit_id) {
        // No CAS: published_horizon_ must stay put -- nothing durable or reader-visible carries a discarded id.
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
        auto it = active_.find(session);
        if (it == active_.end()) {
            return;
        }
        it->second->mark_aborted();
        active_start_times_.erase(it->second->start_time());
        active_.erase(it);
    }

    void transaction_manager_t::fail(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto it = active_.find(session);
        if (it == active_.end()) {
            return;
        }
        it->second->mark_failed();
        active_start_times_.erase(it->second->start_time());
    }

    transaction_t* transaction_manager_t::find_transaction(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto it = active_.find(session);
        if (it == active_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    bool transaction_manager_t::has_active_transaction(session::session_id_t session) const {
        std::lock_guard guard(lock_);
        return active_.find(session) != active_.end();
    }

    uint64_t transaction_manager_t::lowest_active_start_time() const {
        std::lock_guard guard(lock_);
        uint64_t lowest = active_start_times_.empty() ? current_timestamp_.load() : *active_start_times_.begin();
        // Must honour the procarray, not just start times (feeds cleanup_versions -> chunk_info::cleanup):
        // same two clamps as visible_to_all_locked() below, since both populations sit below the lowest start time.
        if (!in_flight_commits_.empty()) {
            lowest = std::min(lowest, *in_flight_commits_.begin() - 1);
        }
        for (const auto& [session, txn] : active_) {
            if (txn->state() == transaction_state_t::failed) {
                continue;
            }
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
        // The naive answer (published_horizon_ if active_ empty, else min snapshot_horizon) ignores
        // in_flight_commits_, so it can miss a smaller commit-id still in flight after publish() advances past it.
        return visible_to_all_locked();
    }

    uint64_t transaction_manager_t::compact_watermark() const {
        std::lock_guard guard(lock_);
        return visible_to_all_locked();
    }

    // Monotone in the safe direction, which dispatcher.cpp's `new_lowest > last_broadcast_horizon_`
    // gate relies on: each term (published_horizon_, min in-flight id, active txn floor) only rises.
    uint64_t transaction_manager_t::visible_to_all_locked() const {
        uint64_t watermark = published_horizon_.load(std::memory_order_relaxed);
        // Committed-but-unpublished ids stay invisible until the lowest one; ids start at 1, so -1 cannot underflow.
        if (!in_flight_commits_.empty()) {
            watermark = std::min(watermark, *in_flight_commits_.begin() - 1);
        }
        for (const auto& [session, txn] : active_) {
            if (txn->state() == transaction_state_t::failed) {
                continue;
            }
            const auto data = txn->data();
            watermark = std::min(watermark, data.snapshot_horizon);
            if (!data.in_flight_snapshot.empty()) {
                watermark = std::min(watermark, data.in_flight_snapshot.front() - 1);
            }
        }
        return watermark;
    }

} // namespace components::table
