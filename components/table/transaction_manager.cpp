#include "transaction_manager.hpp"

#include <stdexcept>

namespace components::table {

    transaction_manager_t::transaction_manager_t() = default;

    transaction_t& transaction_manager_t::begin_transaction(session::session_id_t session) {
        std::lock_guard guard(lock_);
        auto key = session.data();
        if (active_.find(key) != active_.end()) {
            return *active_[key];
        }
        auto txn_id = next_transaction_id_.fetch_add(1);
        auto start_time = current_timestamp_.fetch_add(1);
        auto txn = std::make_unique<transaction_t>(txn_id, start_time, session);
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
        return commit_id;
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
