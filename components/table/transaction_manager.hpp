#pragma once

#include <atomic>
#include <components/session/session.hpp>
#include <components/table/transaction.hpp>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <set>
#include <unordered_map>

namespace components::table {

    class transaction_manager_t {
    public:
        // resource backs the in_flight_snapshot vector in every snapshot handed
        // out. Required, not defaulted, so each snapshot stays valid when moved.
        explicit transaction_manager_t(std::pmr::memory_resource* resource);

        transaction_t& begin_transaction(session::session_id_t session);
        uint64_t commit(session::session_id_t session);
        void abort(session::session_id_t session);

        transaction_t* find_transaction(session::session_id_t session);
        bool has_active_transaction(session::session_id_t session) const;

        uint64_t lowest_active_start_time() const;
        bool has_active_transactions() const;

        // Oldest snapshot_horizon among active transactions (published_horizon_
        // when none are active). Commit-id value space — feeds the DROP-GC
        // horizon broadcast whose sweep compares dropped_at_commit_id (also
        // commit-id space after the dropped-committed remap) against it.
        uint64_t lowest_active_snapshot_horizon() const;

        // Visible-to-all horizon for data_table_t::compact(): every commit_id at
        // or below the returned value is visible to EVERY current snapshot and to
        // every snapshot taken later. Computed atomically as the min of
        //   * published_horizon_ (floor for future snapshots),
        //   * min(in_flight_commits_) - 1 (committed-unpublished ids and anything
        //     newer stay protected — future snapshots reject them via
        //     in_flight_snapshot),
        //   * per active txn: min(snapshot_horizon, min(in_flight_snapshot) - 1).
        // Monotonic in the safe direction: a value computed now is never above a
        // value computed later, so it can ride actor messages without re-checks.
        uint64_t compact_watermark() const;

        // ProcArray atomic publish barrier: moves a committed txn out of
        // in_flight_commits_ and advances published_horizon_. MUST be called at
        // the end of the commit pipeline (after WAL fsync + storage_publish_*),
        // so a fresh snapshot captures the txn as visible.
        void publish(uint64_t commit_id);

        // Capture an MVCC snapshot atomically. Caller supplies the resource for
        // the in_flight_snapshot vector so the result can be moved without dangling.
        struct snapshot_t {
            uint64_t snapshot_horizon;
            std::pmr::vector<uint64_t> in_flight_snapshot;

            explicit snapshot_t(std::pmr::memory_resource* resource)
                : snapshot_horizon(0)
                , in_flight_snapshot(resource) {}
        };
        snapshot_t take_snapshot(std::pmr::memory_resource* resource) const;

        uint64_t published_horizon() const noexcept { return published_horizon_.load(std::memory_order_acquire); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        std::pmr::memory_resource* resource_;
        std::atomic<uint64_t> next_transaction_id_{TRANSACTION_ID_START};
        std::atomic<uint64_t> current_timestamp_{1};
        mutable std::mutex lock_;
        std::unordered_map<uint64_t, std::unique_ptr<transaction_t>> active_;
        std::set<uint64_t> active_start_times_;
        // ProcArray fields: commit_ids allocated by commit() but not yet
        // visible until publish(). Snapshots captured during this window must
        // reject these ids.
        std::set<uint64_t> in_flight_commits_;
        std::atomic<uint64_t> published_horizon_{0};
    };

} // namespace components::table
