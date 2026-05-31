#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <cstdint>
#include <memory_resource>
#include <set>
#include <vector>

namespace components::table {

    // Central accumulation: explicit BEGIN..COMMIT txns
    // accumulate DML ranges across statements instead of publishing per-statement.
    // The dispatcher / operator_commit_transaction drains these vectors in
    // batched storage_publish_* calls at COMMIT, producing single-atom visibility
    // for the whole transaction. Implicit txns (each statement is its own txn)
    // continue to publish per-statement and never touch these fields.
    struct dml_append_range_t {
        catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    class transaction_t {
    public:
        transaction_t(uint64_t transaction_id,
                      uint64_t start_time,
                      session::session_id_t session,
                      std::pmr::memory_resource* resource = std::pmr::null_memory_resource());

        // + BLOCKER 8): data() returns the cached snapshot
        // by value-copy. The snapshot is set once by transaction_manager during
        // begin_transaction(); subsequent reads avoid re-locking the manager. The
        // vector copy is O(active in-flight commits) — typically <100, amortized
        // by the txn's pmr arena.
        transaction_data data() const {
            transaction_data td(transaction_id_, start_time_);
            td.snapshot_horizon = snapshot_horizon_;
            td.in_flight_snapshot = in_flight_snapshot_;
            return td;
        }
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

        // ): transaction_manager calls this during
        // begin_transaction after capturing the snapshot under its lock. The
        // vector is moved in — the caller transfers ownership of the resource.
        void set_snapshot(uint64_t horizon, std::pmr::vector<uint64_t> in_flight) {
            snapshot_horizon_ = horizon;
            in_flight_snapshot_ = std::move(in_flight);
        }

        // Central accumulation API. operator_begin_transaction
        // marks an explicit txn; executor consults is_explicit() in the commit
        // phase to decide between per-statement publish (implicit) and accumulate
        // (explicit). operator_commit_transaction drains the vectors at COMMIT.
        void mark_explicit() noexcept { is_explicit_ = true; }
        bool is_explicit() const noexcept { return is_explicit_; }

        void accumulate_base_append(dml_append_range_t range) {
            pending_base_appends_.push_back(range);
        }
        void accumulate_base_delete(dml_delete_range_t range) {
            pending_base_deletes_.push_back(range);
        }

        std::pmr::vector<dml_append_range_t> drain_base_appends() {
            std::pmr::vector<dml_append_range_t> out(std::move(pending_base_appends_));
            pending_base_appends_ = std::pmr::vector<dml_append_range_t>(pending_base_appends_.get_allocator());
            return out;
        }
        std::pmr::vector<dml_delete_range_t> drain_base_deletes() {
            std::pmr::vector<dml_delete_range_t> out(std::move(pending_base_deletes_));
            pending_base_deletes_ = std::pmr::vector<dml_delete_range_t>(pending_base_deletes_.get_allocator());
            return out;
        }

        // EXTENSION — pg_catalog accumulation for explicit
        // BEGIN..COMMIT. Each per-statement fragment inside an explicit txn
        // produces a vector of pg_catalog append-ranges and a set of
        // delete-tables (from operator_primitive_write_t / register_udf /
        // create_collection / etc.). Implicit (auto-commit) statements
        // publish these inline in the executor's commit phase; explicit
        // statements PARK them onto the transaction_t here so
        // operator_commit_transaction_t drains them into a single batched
        // storage_publish_commits / storage_publish_deletes at COMMIT.
        //
        // Storage shape mirrors the existing public `pg_catalog_appends` /
        // `pg_catalog_delete_tables` vectors (already drained by
        // operator_commit_transaction.cpp:90-91). The accumulate_*/drain_*
        // API just wraps them so callers don't reach into the raw fields.
        void accumulate_pg_catalog_pending(
            std::vector<components::pg_catalog_append_range_t>&& appends,
            std::set<components::catalog::oid_t>&& delete_tables) {
            for (auto& a : appends) {
                pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : delete_tables) {
                pg_catalog_delete_tables.insert(std::move(d));
            }
        }
        void drain_pg_catalog_pending(
            std::vector<components::pg_catalog_append_range_t>& out_appends,
            std::set<components::catalog::oid_t>& out_delete_tables) {
            out_appends = std::move(pg_catalog_appends);
            out_delete_tables = std::move(pg_catalog_delete_tables);
            pg_catalog_appends.clear();
            pg_catalog_delete_tables.clear();
        }

        // accumulation/drain for explicit BEGIN..ALTER..COMMIT. Each per-
        // statement fragment that runs an ALTER COLUMN inside the explicit
        // txn parks its markers here; operator_commit_transaction_t drains
        // them after commit_id allocation and patches the rows.
        void accumulate_pg_attribute_commit_id_backfills(
            std::vector<components::pg_attribute_commit_id_backfill_t>&& backfills) {
            for (auto& b : backfills) {
                pg_attribute_commit_id_backfills.push_back(b);
            }
        }
        std::vector<components::pg_attribute_commit_id_backfill_t>
        drain_pg_attribute_commit_id_backfills() {
            std::vector<components::pg_attribute_commit_id_backfill_t> out(
                std::move(pg_attribute_commit_id_backfills));
            pg_attribute_commit_id_backfills.clear();
            return out;
        }

        struct append_info {
            int64_t row_start;
            uint64_t count;
        };
        void add_append(int64_t row_start, uint64_t count);
        const std::vector<append_info>& appends() const { return appends_; }

        // Aggregated across all execute_plan_impl calls within this txn.
        // Snapshotted by operator_commit_transaction_t / operator_abort_transaction_t
        // before txn_manager_.commit()/abort() to drive storage_publish_commits /
        // storage_revert_appends after the swap point.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        // Drained by operator_commit_transaction_t at COMMIT.
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;

    private:
        session::session_id_t session_;
        uint64_t transaction_id_;
        uint64_t start_time_;
        uint64_t commit_id_{0};
        bool committed_{false};
        bool aborted_{false};
        bool is_explicit_{false};
        std::vector<append_info> appends_;

        // ProcArray cached snapshot — set once by transaction_manager
        // during begin_transaction; never mutated thereafter. Returned by value
        // from data() each call.
        uint64_t snapshot_horizon_{0};
        std::pmr::vector<uint64_t> in_flight_snapshot_{std::pmr::null_memory_resource()};

        // Central accumulation: explicit BEGIN..COMMIT txns
        // park DML ranges here until COMMIT drains them in a single atomic
        // publish batch. Implicit txns never touch these (is_explicit_ false).
        std::pmr::vector<dml_append_range_t> pending_base_appends_{std::pmr::null_memory_resource()};
        std::pmr::vector<dml_delete_range_t> pending_base_deletes_{std::pmr::null_memory_resource()};
    };

} // namespace components::table
