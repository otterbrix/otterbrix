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

        // Horizon broadcast to the DROP-GC / deferred-index-delete sweeps.
        // Commit-id value space, so the agents' `dropped_at_commit_id < horizon`
        // and `entry->commit_id <= horizon` sweeps compare like with like.
        // Same value as compact_watermark() — both ask "what is visible to
        // EVERY snapshot, present and future"; the two names are kept because
        // they carry different consumer contracts.
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

        // publish() MINUS THE CAS — and the missing CAS is the whole of it.
        //
        // A commit pipeline that dies after commit() allocated the id but before the
        // barrier leaves that id in in_flight_commits_ with nobody left to remove it:
        // commit() has already erased the txn from active_, so find_transaction()
        // answers nullptr and neither ROLLBACK nor the dispatcher's failure-release
        // net can reach it. The id then floors visible_to_all_locked() at
        // commit_id - 1 for the life of the process, which stops
        // data_table_t::compact(), the DROP-GC tombstone sweep and the deferred
        // index-delete sweep — all three read that one number and none of them ever
        // sees a snapshot.
        //
        // WHY IT CANNOT PUBLISH ANYTHING. published_horizon_ is NOT advanced here, so
        // no commit becomes visible by being forgotten. What makes the erase itself
        // safe is an ordering rule enforced by operator_commit_transaction_t — no step
        // that can fail may run after the first step that stamps the commit_id — so a
        // discarded id is stamped on nothing: no row version, no pg_attribute column,
        // no deferred index-delete entry, no WAL marker. There is nothing left for a
        // reader to have to hide, which is why the id may leave the set outright
        // instead of moving into a second "discarded" set that the floor would then
        // have to honour anyway (that set gives back exactly what it took).
        //
        // Monotone in the safe direction, the property every horizon reader rests on:
        // erasing a member of in_flight_commits_ only RAISES min(), never lowers it.
        // Idempotent, and a no-op for an id that was never in flight.
        void discard(uint64_t commit_id);

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

        // Reopen restores BOTH halves of the commit clock from a SINGLE durable
        // frontier. The one entry point every reopen restore path must funnel
        // through, so the two halves can never disagree:
        //   * current_timestamp_  → max(current, frontier + 1): the fetch_add source
        //     of every new start_time/commit_id. Raising it past the frontier means
        //     post-reopen INSERTs draw commit-ids ABOVE the durable band — they no
        //     longer collide with already-published ids, so a reader that snapshots
        //     them in-flight and later sees them published judges them visible (and
        //     persisted added_at_commit_id stay in the past).
        //   * published_horizon_  → max(current, frontier): post-recovery snapshots
        //     see every persisted commit as published.
        // Maintains the invariant current_timestamp_ >= published_horizon_ + 1.
        // Idempotent — NEVER lowers either value. Called single-threaded at
        // bootstrap, before schedulers start, so plain store(max(...)) under no
        // contention suffices.
        void restore_commit_clock(uint64_t frontier);

        // Reopen restores the commit-id horizon so persisted catalog columns stay
        // visible. pg_attribute stamps every column with an added_at_commit_id from
        // the prior session's clock; a reopened manager starts its clock at {1,0},
        // so without this seed every new txn's start_time would fall BELOW those
        // persisted ids and resolve_table's visibility filter would judge all
        // columns "added after my snapshot" → "column not found".
        void seed_commit_clock(uint64_t high_water) { restore_commit_clock(high_water); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        // The ONE computation of "visible to all": the greatest commit_id that
        // every live snapshot AND every future snapshot already sees. Requires
        // lock_ to be held by the caller (it does not take it — lock_ is not
        // recursive). Both public horizon readers are thin wrappers over this.
        uint64_t visible_to_all_locked() const;

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
