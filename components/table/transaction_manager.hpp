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
        // resource backs in_flight_snapshot in every snapshot; required so a moved snapshot stays valid.
        explicit transaction_manager_t(std::pmr::memory_resource* resource);

        transaction_t& begin_transaction(session::session_id_t session, transaction_scope_t scope);

        transaction_t& resolve_transaction(session::session_id_t session, transaction_scope_t scope);

        uint64_t commit(session::session_id_t session);
        void abort(session::session_id_t session);

        transaction_t* find_transaction(session::session_id_t session);
        bool has_active_transaction(session::session_id_t session) const;

        uint64_t lowest_active_start_time() const;
        bool has_active_transactions() const;

        // Same as compact_watermark(); kept as a separate name for the DROP-GC/index-delete contract.
        uint64_t lowest_active_snapshot_horizon() const;

        // Every commit_id at or below the result is visible to every snapshot, current or future; monotonic.
        uint64_t compact_watermark() const;

        // Must run at the end of the commit pipeline, after WAL fsync + storage_publish_*.
        void publish(uint64_t commit_id);

        // publish() minus the CAS: erases an id orphaned by a dead pipeline; doesn't advance published_horizon_.
        void discard(uint64_t commit_id);

        // Caller supplies the resource for in_flight_snapshot so the snapshot can move without dangling.
        struct snapshot_t {
            uint64_t snapshot_horizon;
            std::pmr::vector<uint64_t> in_flight_snapshot;

            explicit snapshot_t(std::pmr::memory_resource* resource)
                : snapshot_horizon(0)
                , in_flight_snapshot(resource) {}
        };
        snapshot_t take_snapshot(std::pmr::memory_resource* resource) const;

        uint64_t published_horizon() const noexcept { return published_horizon_.load(std::memory_order_acquire); }

        // Raises both halves of the commit clock from one frontier, so they can't disagree; idempotent.
        void restore_commit_clock(uint64_t frontier);

        // Seeds the clock after reopen, or persisted pg_attribute columns could look not-yet-added.
        void seed_commit_clock(uint64_t high_water) { restore_commit_clock(high_water); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        // Backs both public horizon readers; requires lock_ held by the caller (not recursive).
        uint64_t visible_to_all_locked() const;
        transaction_t& open_locked(session::session_id_t session, transaction_scope_t scope);

        std::pmr::memory_resource* resource_;
        // NOT seeded from the journal, unlike the commit clock -- deliberate (cost a durability bug once):
        // txn ids are within-process only; filter_committed_records's wal-order check guards journal replay.
        std::atomic<uint64_t> next_transaction_id_{TRANSACTION_ID_START};
        std::atomic<uint64_t> current_timestamp_{1};
        mutable std::mutex lock_;
        std::unordered_map<session::session_id_t, std::unique_ptr<transaction_t>> active_;
        std::set<uint64_t> active_start_times_;
        // commit_ids allocated by commit() but not yet visible until publish(); snapshots must reject them.
        std::set<uint64_t> in_flight_commits_;
        std::atomic<uint64_t> published_horizon_{0};
    };

} // namespace components::table
