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

    // DML range parked by an explicit BEGIN..COMMIT txn so COMMIT can publish all
    // statements in one atomic batch. Implicit (per-statement) txns publish inline
    // and never use these.
    struct dml_append_range_t {
        catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    // An index a CREATE INDEX in this txn brought into being, identified by the
    // owning table oid + index name. Parked until COMMIT publishes it / ABORT
    // un-marks it (the rollback path drops the still-uncommitted index). Mirrors
    // dml_delete_range_t's shape — a plain value struct that crosses no mailbox
    // itself but rides the txn accumulate/drain payloads.
    struct created_index_t {
        components::catalog::oid_t table_oid;
        std::string name;
    };

    class transaction_t {
    public:
        // resource is REQUIRED, not defaulted: the per-txn pmr containers allocate
        // from it, and a global/null default would leak across the txn boundary.
        transaction_t(uint64_t transaction_id,
                      uint64_t start_time,
                      session::session_id_t session,
                      std::pmr::memory_resource* resource);

        // Value-copy of the cached snapshot so reads avoid re-locking the manager
        // (the snapshot is captured once in begin_transaction). Copy is O(in-flight
        // commits), typically <100.
        transaction_data data() const {
            return transaction_data(transaction_id_, start_time_, snapshot_horizon_, in_flight_snapshot_);
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

        // Called by transaction_manager during begin_transaction after capturing
        // the snapshot under its lock; in_flight is moved in.
        void set_snapshot(uint64_t horizon, std::pmr::vector<uint64_t> in_flight) {
            snapshot_horizon_ = horizon;
            in_flight_snapshot_ = std::move(in_flight);
        }

        // The executor's commit phase reads is_explicit() to choose per-statement
        // publish (implicit) vs accumulate-until-COMMIT (explicit).
        void mark_explicit() noexcept { is_explicit_ = true; }
        bool is_explicit() const noexcept { return is_explicit_; }

        void accumulate_base_append(dml_append_range_t range) { pending_base_appends_.push_back(range); }
        void accumulate_base_delete(dml_delete_range_t range) { pending_base_deletes_.push_back(range); }

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

        // pg_catalog accumulation for explicit txns: park append-ranges and
        // delete-tables so COMMIT drains them into one batched publish.
        void accumulate_pg_catalog_pending(std::vector<components::pg_catalog_append_range_t>&& appends,
                                           std::set<components::catalog::oid_t>&& delete_tables) {
            for (auto& a : appends) {
                pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : delete_tables) {
                pg_catalog_delete_tables.insert(std::move(d));
            }
        }
        void drain_pg_catalog_pending(std::vector<components::pg_catalog_append_range_t>& out_appends,
                                      std::set<components::catalog::oid_t>& out_delete_tables) {
            out_appends = std::move(pg_catalog_appends);
            out_delete_tables = std::move(pg_catalog_delete_tables);
            pg_catalog_appends.clear();
            pg_catalog_delete_tables.clear();
        }

        // ALTER COLUMN backfill markers parked inside an explicit txn;
        // operator_commit_transaction_t drains them post-commit_id and patches the rows.
        void accumulate_pg_attribute_commit_id_backfills(
            std::vector<components::pg_attribute_commit_id_backfill_t>&& backfills) {
            for (auto& b : backfills) {
                pg_attribute_commit_id_backfills.push_back(b);
            }
        }
        std::vector<components::pg_attribute_commit_id_backfill_t> drain_pg_attribute_commit_id_backfills() {
            std::vector<components::pg_attribute_commit_id_backfill_t> out(std::move(pg_attribute_commit_id_backfills));
            pg_attribute_commit_id_backfills.clear();
            return out;
        }

        // Storage oids whose backing files a DROP TABLE / DROP INDEX in this txn
        // retired. Parked until COMMIT so the GC-remap (operator_commit_transaction)
        // can stamp them with the real commit_id; ABORT drains them too. Mirrors
        // the accumulate/drain pairs above.
        void accumulate_dropped_storage(components::catalog::oid_t oid) { dropped_storage_oids_.push_back(oid); }
        std::vector<components::catalog::oid_t> drain_dropped_storages() {
            std::vector<components::catalog::oid_t> out(std::move(dropped_storage_oids_));
            dropped_storage_oids_.clear();
            return out;
        }

        // Storage oids whose backing files a CREATE TABLE / CREATE INDEX in this
        // txn brought into being, and the indexes those statements created.
        // Parked until COMMIT publishes them; ABORT drains them to drop the
        // still-uncommitted storage / index (a CREATE inside a txn must be
        // revertible until COMMIT). Mirror the dropped_storage_oids_ pair above.
        void accumulate_created_storage(components::catalog::oid_t oid) { created_storage_oids_.push_back(oid); }
        std::vector<components::catalog::oid_t> drain_created_storages() {
            std::vector<components::catalog::oid_t> out(std::move(created_storage_oids_));
            created_storage_oids_.clear();
            return out;
        }
        void accumulate_created_index(created_index_t index) { created_indexes_.push_back(std::move(index)); }
        std::vector<created_index_t> drain_created_indexes() {
            std::vector<created_index_t> out(std::move(created_indexes_));
            created_indexes_.clear();
            return out;
        }

        // True when ANY accumulator parked by this txn is non-empty: pending base
        // appends/deletes, pg_catalog appends/delete-tables, backfills, or dropped
        // storage oids. The commit-drain handler reads it to ABORT an empty COMMIT
        // (a bare COMMIT, a read-only explicit txn, or zero-row DML) instead of
        // allocating a commit_id and advancing the horizon for a no-op.
        bool has_accumulated() const {
            return !pending_base_appends_.empty() || !pending_base_deletes_.empty() || !pg_catalog_appends.empty() ||
                   !pg_catalog_delete_tables.empty() || !pg_attribute_commit_id_backfills.empty() ||
                   !dropped_storage_oids_.empty() || !created_storage_oids_.empty() || !created_indexes_.empty();
        }

        struct append_info {
            int64_t row_start;
            uint64_t count;
        };
        void add_append(int64_t row_start, uint64_t count);
        const std::vector<append_info>& appends() const { return appends_; }

        // Aggregated across executor statements; drained by the commit/abort
        // operators before commit()/abort() to drive storage_publish/revert.
        //
        // THREADING INVARIANT: this transaction_t BODY (these plain containers) is
        // single-owner-thread per session. transaction_manager_t::lock_ guards only
        // the session map; find_transaction() hands back this object raw. The
        // executor worker (accumulate_*) and the dispatcher loop thread (merge/drain)
        // both mutate it but NEVER concurrently: the dispatcher co_awaits the
        // executor result before touching the txn (release/acquire), and wait_future
        // serializes statements per session. Concurrent mutation is FORBIDDEN —
        // route new cross-thread writes through a txn_*_msg mailbox handler.
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

        // ProcArray cached snapshot — set once by transaction_manager during
        // begin_transaction, never mutated after; returned by value from data().
        // The pmr members below have no default initializer on purpose: the sole
        // ctor inits them from its required resource, so none binds to a global default.
        uint64_t snapshot_horizon_{0};
        std::pmr::vector<uint64_t> in_flight_snapshot_;

        // Explicit txns park DML ranges here until COMMIT drains them in one
        // atomic publish batch; implicit txns never touch them.
        std::pmr::vector<dml_append_range_t> pending_base_appends_;
        std::pmr::vector<dml_delete_range_t> pending_base_deletes_;

        // Storage oids retired by DROP in this txn; plain std::vector — it crosses
        // no mailbox itself, but matches the catalog-oid sibling accumulators and
        // is drained into txn_commit_drain_t / txn_abort_drain_t (plain std too).
        std::vector<components::catalog::oid_t> dropped_storage_oids_;

        // Storage oids / indexes brought into being by CREATE in this txn; plain
        // std (sibling style to dropped_storage_oids_). Drained into
        // txn_commit_drain_t / txn_abort_drain_t so COMMIT publishes them and
        // ABORT drops the still-uncommitted artifacts.
        std::vector<components::catalog::oid_t> created_storage_oids_;
        std::vector<created_index_t> created_indexes_;
    };

} // namespace components::table
