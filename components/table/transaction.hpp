#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <cstdint>
#include <set>
#include <vector>

namespace components::table {

    class transaction_t {
    public:
        transaction_t(uint64_t transaction_id, uint64_t start_time, session::session_id_t session);

        transaction_data data() const { return {transaction_id_, start_time_}; }
        uint64_t transaction_id() const { return transaction_id_; }
        uint64_t start_time() const { return start_time_; }
        uint64_t commit_id() const { return commit_id_; }
        session::session_id_t session() const { return session_; }

        bool is_active() const { return !committed_ && !aborted_; }
        bool is_committed() const { return committed_; }
        bool is_aborted() const { return aborted_; }

        void set_commit_id(uint64_t id);
        void mark_committed();
        void mark_aborted();

        struct append_info {
            int64_t row_start;
            uint64_t count;
        };
        void add_append(int64_t row_start, uint64_t count);
        const std::vector<append_info>& appends() const { return appends_; }

        // Aggregated across all execute_plan_impl calls within this txn.
        // Snapshotted by operator_commit_transaction_t / operator_abort_transaction_t
        // before txn_manager_.commit()/abort() to drive storage_commit_appends /
        // storage_revert_appends after the swap point.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;

    private:
        session::session_id_t session_;
        uint64_t transaction_id_;
        uint64_t start_time_;
        uint64_t commit_id_{0};
        bool committed_{false};
        bool aborted_{false};
        std::vector<append_info> appends_;
    };

} // namespace components::table
