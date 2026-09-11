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

    // Parked by an explicit BEGIN..COMMIT txn so COMMIT publishes all statements atomically; implicit txns skip these.
    struct dml_append_range_t {
        catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    // An index CREATE INDEX made (table oid + pg_index.indexrelid); parked until COMMIT/ABORT resolves it.
    struct created_index_t {
        components::catalog::oid_t table_oid;
        components::catalog::oid_t index_oid;
    };

    class transaction_t {
    public:
        // resource is required, not defaulted, so allocations can't leak across the txn boundary via a global default.
        transaction_t(uint64_t transaction_id,
                      uint64_t start_time,
                      session::session_id_t session,
                      std::pmr::memory_resource* resource);

        // Value-copy of the cached snapshot, so reads avoid re-locking; O(in-flight commits) to copy, typically <100.
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

        // Called by transaction_manager during begin_transaction, after capturing the snapshot under its lock.
        void set_snapshot(uint64_t horizon, std::pmr::vector<uint64_t> in_flight) {
            snapshot_horizon_ = horizon;
            in_flight_snapshot_ = std::move(in_flight);
        }

        // The executor's commit phase reads this to choose per-statement publish vs accumulate-until-COMMIT.
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

        // For explicit txns: parks appends/delete-tables so COMMIT drains them into one batched publish.
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

        // ALTER COLUMN backfill markers; operator_commit_transaction_t drains them post-commit_id.
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

        // Storage oids a DROP retired; parked so the GC-remap can stamp them with the real commit_id.
        void accumulate_dropped_storage(components::catalog::oid_t oid) { dropped_storage_oids_.push_back(oid); }
        std::vector<components::catalog::oid_t> drain_dropped_storages() {
            std::vector<components::catalog::oid_t> out(std::move(dropped_storage_oids_));
            dropped_storage_oids_.clear();
            return out;
        }

        // Storage oids/indexes a CREATE made; ABORT drains them to drop still-uncommitted artifacts.
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

        // Lets the commit-drain handler ABORT an empty COMMIT instead of allocating a commit_id for a no-op.
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

        // THREADING INVARIANT: transaction_manager_t::lock_ guards only the session map, not this raw
        // object; the executor worker and the dispatcher loop never touch it concurrently, because the
        // dispatcher co_awaits the executor result first and wait_future serializes per session. A new
        // cross-thread writer must route through a txn_*_msg mailbox handler instead.
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

        // ProcArray cached snapshot: set once during begin_transaction, never mutated after.
        uint64_t snapshot_horizon_{0};
        std::pmr::vector<uint64_t> in_flight_snapshot_;

        std::pmr::vector<dml_append_range_t> pending_base_appends_;
        std::pmr::vector<dml_delete_range_t> pending_base_deletes_;

        // Plain std::vector, not pmr: crosses no mailbox, drains into txn_commit_drain_t / txn_abort_drain_t.
        std::vector<components::catalog::oid_t> dropped_storage_oids_;

        std::vector<components::catalog::oid_t> created_storage_oids_;
        std::vector<created_index_t> created_indexes_;
    };

} // namespace components::table
