#pragma once

#include <atomic>
#include <components/session/session.hpp>
#include <components/table/transaction.hpp>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

namespace components::table {

    class transaction_manager_t {
    public:
        transaction_manager_t();

        transaction_t& begin_transaction(session::session_id_t session);
        uint64_t commit(session::session_id_t session);
        void abort(session::session_id_t session);

        transaction_t* find_transaction(session::session_id_t session);
        bool has_active_transaction(session::session_id_t session) const;

        uint64_t lowest_active_start_time() const;
        bool has_active_transactions() const;

    private:
        std::atomic<uint64_t> next_transaction_id_{TRANSACTION_ID_START};
        std::atomic<uint64_t> current_timestamp_{1};
        mutable std::mutex lock_;
        std::unordered_map<uint64_t, std::unique_ptr<transaction_t>> active_;
        std::set<uint64_t> active_start_times_;
    };

} // namespace components::table
