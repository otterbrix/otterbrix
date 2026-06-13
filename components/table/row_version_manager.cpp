#include "row_version_manager.hpp"

#include <cassert>
#include <cstdlib>

#include "collection.hpp"

namespace components::table {

    // ProcArray canonical visibility filter:
    //   1. If id == this txn's own transaction_id → self-write, always visible.
    //   2. If id >= TRANSACTION_ID_START → another txn's pending write, not visible.
    //   3. If id > snapshot_horizon → committed after our snapshot, not visible.
    //   4. If id is in in_flight_snapshot → committed at snapshot time but not yet
    //      publish()-published, not visible.
    //   5. Otherwise → committed-and-published before our snapshot, visible.
    //
    // use_deleted_version is the inverse: a delete-marker id "survives" (i.e. row
    // remains alive) when use_inserted_version says it's NOT visible. NOT_DELETED_ID
    // is huge (>> TRANSACTION_ID_START) so rule 2 implicitly handles it.
    struct transaction_version_operator {
        static bool use_inserted_version(const transaction_data& txn, uint64_t id) {
            if (txn.transaction_id != 0 && id == txn.transaction_id)
                return true;
            if (id >= TRANSACTION_ID_START)
                return false;
            if (id > txn.snapshot_horizon)
                return false;
            if (std::binary_search(txn.in_flight_snapshot.begin(), txn.in_flight_snapshot.end(), id))
                return false;
            return true;
        }

        static bool use_deleted_version(const transaction_data& txn, uint64_t id) {
            return !use_inserted_version(txn, id);
        }
    };

    static bool use_version(const transaction_data& transaction, uint64_t id) {
        return transaction_version_operator::use_inserted_version(transaction, id);
    }

    bool chunk_info::cleanup(uint64_t /* lowest_transaction */, std::unique_ptr<chunk_info>& /* result */) const {
        return false;
    }

    chunk_constant_info::chunk_constant_info(int64_t start)
        : chunk_info(start, chunk_info_type::CONSTANT_INFO)
        , insert_id(0)
        , delete_id(NOT_DELETED_ID) {}

    template<class OP>
    uint64_t chunk_constant_info::templated_indexing_vector(const transaction_data& txn,
                                                            vector::indexing_vector_t&,
                                                            uint64_t max_count) const {
        if (OP::use_inserted_version(txn, insert_id) && OP::use_deleted_version(txn, delete_id)) {
            return max_count;
        }
        return 0;
    }

    uint64_t chunk_constant_info::indexing_vector(transaction_data transaction,
                                                  vector::indexing_vector_t& indexing_vector,
                                                  uint64_t max_count) {
        return templated_indexing_vector<transaction_version_operator>(transaction, indexing_vector, max_count);
    }

    bool chunk_constant_info::fetch(const transaction_data& transaction, int64_t) {
        return use_version(transaction, insert_id) && !use_version(transaction, delete_id);
    }

    void chunk_constant_info::commit_append(uint64_t commit_id, uint64_t start, uint64_t end) {
        assert((start == 0 && end == vector::DEFAULT_VECTOR_CAPACITY) &&
               "chunk_constant_info::commit_append does not cover whole range");
        if (start != 0 || end != vector::DEFAULT_VECTOR_CAPACITY) {
            std::abort();
        }
        insert_id = commit_id;
    }

    bool chunk_constant_info::has_deletes() const {
        bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id < TRANSACTION_ID_START;
        return is_deleted;
    }

    uint64_t chunk_constant_info::committed_deleted_count(uint64_t max_count) {
        return delete_id < TRANSACTION_ID_START ? max_count : 0;
    }

    bool chunk_constant_info::has_version_above(uint64_t watermark, uint64_t /*max_count*/) const {
        // Pending txn ids (>= TRANSACTION_ID_START) are always above any watermark
        // (watermarks live in the commit-id/timestamp space far below 2^62).
        if (insert_id > watermark) {
            return true;
        }
        return delete_id != NOT_DELETED_ID && delete_id > watermark;
    }

    bool chunk_constant_info::cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>&) const {
        if (insert_id > lowest_transaction) {
            return false;
        }
        // Committed deletes are fine — they're visible to all active transactions
        if (delete_id != NOT_DELETED_ID && delete_id > lowest_transaction) {
            return false;
        }
        return true;
    }

    chunk_vector_info::chunk_vector_info(int64_t start)
        : chunk_info(start, chunk_info_type::VECTOR_INFO)
        , insert_id(0)
        , same_inserted_id(true)
        , any_deleted(false) {
        for (uint64_t i = 0; i < vector::DEFAULT_VECTOR_CAPACITY; i++) {
            inserted[i] = 0;
            deleted[i] = NOT_DELETED_ID;
        }
    }

    template<class OP>
    uint64_t chunk_vector_info::templated_indexing_vector(const transaction_data& txn,
                                                          vector::indexing_vector_t& indexing_vector,
                                                          uint64_t max_count) const {
        uint64_t count = 0;
        if (same_inserted_id && !any_deleted) {
            if (OP::use_inserted_version(txn, insert_id)) {
                return max_count;
            } else {
                return 0;
            }
        } else if (same_inserted_id) {
            if (!OP::use_inserted_version(txn, insert_id)) {
                return 0;
            }
            for (uint64_t i = 0; i < max_count; i++) {
                if (OP::use_deleted_version(txn, deleted[i])) {
                    indexing_vector.set_index(count++, i);
                }
            }
        } else if (!any_deleted) {
            for (uint64_t i = 0; i < max_count; i++) {
                if (OP::use_inserted_version(txn, inserted[i])) {
                    indexing_vector.set_index(count++, i);
                }
            }
        } else {
            for (uint64_t i = 0; i < max_count; i++) {
                if (OP::use_inserted_version(txn, inserted[i]) && OP::use_deleted_version(txn, deleted[i])) {
                    indexing_vector.set_index(count++, i);
                }
            }
        }
        return count;
    }

    uint64_t chunk_vector_info::indexing_vector(transaction_data transaction,
                                                vector::indexing_vector_t& indx_vector,
                                                uint64_t max_count) {
        return templated_indexing_vector<transaction_version_operator>(transaction, indx_vector, max_count);
    }

    bool chunk_vector_info::fetch(const transaction_data& transaction, int64_t row) {
        return use_version(transaction, inserted[row]) && !use_version(transaction, deleted[row]);
    }

    uint64_t chunk_vector_info::delete_rows(uint64_t transaction_id, int64_t rows[], uint64_t count) {
        any_deleted = true;

        uint64_t deleted_tuples = 0;
        for (uint64_t i = 0; i < count; i++) {
            if (deleted[rows[i]] != NOT_DELETED_ID) {
                // Already deleted (by this txn, or a prior committed txn in a
                // cascade-drop where the scan ignores MVCC visibility). Skip
                // rather than abort: cascade DDL must be idempotent.
                continue;
            }
            deleted[rows[i]] = transaction_id;
            rows[deleted_tuples] = rows[i];
            deleted_tuples++;
        }
        return deleted_tuples;
    }

    void chunk_vector_info::commit_delete(uint64_t commit_id, const delete_info& info) {
        if (info.is_consecutive) {
            for (uint64_t i = 0; i < info.count; i++) {
                deleted[i] = commit_id;
            }
        } else {
            auto rows = info.get_rows();
            for (uint64_t i = 0; i < info.count; i++) {
                deleted[rows[i]] = commit_id;
            }
        }
    }

    void chunk_vector_info::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        if (!any_deleted) {
            return;
        }
        for (uint64_t i = 0; i < vector::DEFAULT_VECTOR_CAPACITY; i++) {
            if (deleted[i] == txn_id) {
                deleted[i] = commit_id;
            }
        }
    }

    void chunk_vector_info::revert_all_deletes(uint64_t txn_id) {
        // Mirror of commit_all_deletes: instead of stamping this txn's pending
        // delete marks with a commit_id, un-stamp them back to NOT_DELETED_ID so
        // an aborted DELETE leaves the rows visible again. any_deleted stays as-is
        // (a conservative hint — indexing/cleanup re-check each slot).
        if (!any_deleted) {
            return;
        }
        for (uint64_t i = 0; i < vector::DEFAULT_VECTOR_CAPACITY; i++) {
            if (deleted[i] == txn_id) {
                deleted[i] = NOT_DELETED_ID;
            }
        }
    }

    void chunk_vector_info::append(uint64_t start, uint64_t end, uint64_t commit_id) {
        if (start == 0) {
            insert_id = commit_id;
        } else if (insert_id != commit_id) {
            same_inserted_id = false;
            insert_id = NOT_DELETED_ID;
        }
        for (uint64_t i = start; i < end; i++) {
            inserted[i] = commit_id;
        }
    }

    void chunk_vector_info::commit_append(uint64_t commit_id, uint64_t start, uint64_t end) {
        if (same_inserted_id) {
            insert_id = commit_id;
        }
        for (uint64_t i = start; i < end; i++) {
            inserted[i] = commit_id;
        }
    }

    bool chunk_vector_info::cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>& result) const {
        // Check inserts: all must be committed and old enough
        if (!same_inserted_id) {
            for (uint64_t idx = 0; idx < vector::DEFAULT_VECTOR_CAPACITY; idx++) {
                if (inserted[idx] > lowest_transaction) {
                    return false;
                }
            }
        } else if (insert_id > lowest_transaction) {
            return false;
        }

        if (any_deleted) {
            // Check if ALL deletes are committed (< TRANSACTION_ID_START) and old enough
            bool all_committed = true;
            bool all_deleted = true;
            uint64_t min_delete_id = NOT_DELETED_ID;
            for (uint64_t i = 0; i < vector::DEFAULT_VECTOR_CAPACITY; i++) {
                if (deleted[i] == NOT_DELETED_ID) {
                    all_deleted = false;
                    continue;
                }
                if (deleted[i] >= TRANSACTION_ID_START || deleted[i] > lowest_transaction) {
                    // Uncommitted or too recent delete — can't cleanup
                    all_committed = false;
                    break;
                }
                if (deleted[i] < min_delete_id) {
                    min_delete_id = deleted[i];
                }
            }
            if (!all_committed) {
                return false;
            }
            // All deletes are committed and old enough.
            // If every row is deleted, convert to chunk_constant_info
            if (all_deleted) {
                auto constant = std::make_unique<chunk_constant_info>(start);
                constant->insert_id = same_inserted_id ? insert_id : inserted[0];
                constant->delete_id = min_delete_id;
                result = std::move(constant);
            }
            // else: partial deletes — still allow cleanup (version info no longer needed
            //       because all versions are visible to all active transactions)
        }
        return true;
    }

    bool chunk_vector_info::has_deletes() const { return any_deleted; }

    bool chunk_vector_info::has_version_above(uint64_t watermark, uint64_t max_count) const {
        if (same_inserted_id) {
            if (insert_id > watermark) {
                return true;
            }
        } else {
            for (uint64_t i = 0; i < max_count; i++) {
                if (inserted[i] > watermark) {
                    return true;
                }
            }
        }
        if (any_deleted) {
            for (uint64_t i = 0; i < max_count; i++) {
                if (deleted[i] != NOT_DELETED_ID && deleted[i] > watermark) {
                    return true;
                }
            }
        }
        return false;
    }

    uint64_t chunk_vector_info::committed_deleted_count(uint64_t max_count) {
        if (!any_deleted) {
            return 0;
        }
        uint64_t delete_count = 0;
        for (uint64_t i = 0; i < max_count; i++) {
            if (deleted[i] < TRANSACTION_ID_START) {
                delete_count++;
            }
        }
        return delete_count;
    }

    row_version_manager_t::row_version_manager_t(int64_t start) noexcept
        : start_(start)
        , has_changes_(false) {}

    void row_version_manager_t::set_start(int64_t new_start) {
        std::lock_guard l(version_lock_);
        this->start_ = new_start;
        int64_t current_start = start_;
        for (auto& info : vector_info_) {
            if (info) {
                info->start = current_start;
            }
            current_start += static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY);
        }
    }

    uint64_t row_version_manager_t::committed_deleted_count(uint64_t count) {
        std::lock_guard l(version_lock_);
        uint64_t deleted_count = 0;
        for (uint64_t r = 0, i = 0; r < count; r += vector::DEFAULT_VECTOR_CAPACITY, i++) {
            if (i >= vector_info_.size() || !vector_info_[i]) {
                continue;
            }
            uint64_t max_count = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, count - r);
            if (max_count == 0) {
                break;
            }
            deleted_count += vector_info_[i]->committed_deleted_count(max_count);
        }
        return deleted_count;
    }

    bool row_version_manager_t::has_version_above(uint64_t watermark, uint64_t count) {
        std::lock_guard l(version_lock_);
        for (uint64_t r = 0, i = 0; r < count; r += vector::DEFAULT_VECTOR_CAPACITY, i++) {
            if (i >= vector_info_.size() || !vector_info_[i]) {
                continue;
            }
            // Bound by the live row count: revert_append truncates rows but may
            // leave stale pending stamps in the tail of a partial vector.
            uint64_t max_count = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, count - r);
            if (max_count == 0) {
                break;
            }
            if (vector_info_[i]->has_version_above(watermark, max_count)) {
                return true;
            }
        }
        return false;
    }

    chunk_info* row_version_manager_t::get_chunk_info(uint64_t vector_idx) {
        if (vector_idx >= vector_info_.size()) {
            return nullptr;
        }
        return vector_info_[vector_idx].get();
    }

    uint64_t row_version_manager_t::indexing_vector(transaction_data transaction,
                                                    uint64_t vector_idx,
                                                    vector::indexing_vector_t& indexing_vector,
                                                    uint64_t max_count) {
        std::lock_guard l(version_lock_);
        auto chunk_info = get_chunk_info(vector_idx);
        if (!chunk_info) {
            return max_count;
        }
        return chunk_info->indexing_vector(transaction, indexing_vector, max_count);
    }

    bool row_version_manager_t::fetch(const transaction_data& transaction, uint64_t row) {
        std::lock_guard lock(version_lock_);
        uint64_t vector_index = row / vector::DEFAULT_VECTOR_CAPACITY;
        auto info = get_chunk_info(vector_index);
        if (!info) {
            return true;
        }
        return info->fetch(transaction, static_cast<int64_t>(row - vector_index * vector::DEFAULT_VECTOR_CAPACITY));
    }

    void row_version_manager_t::fill_vector_info(uint64_t vector_idx) {
        if (vector_idx < vector_info_.size()) {
            return;
        }
        vector_info_.reserve(vector_idx + 1);
        for (uint64_t i = vector_info_.size(); i <= vector_idx; i++) {
            vector_info_.emplace_back();
        }
    }

    void row_version_manager_t::append_version_info(transaction_data transaction,
                                                    uint64_t,
                                                    uint64_t row_group_start,
                                                    uint64_t row_group_end) {
        std::lock_guard lock(version_lock_);
        has_changes_ = true;
        uint64_t start_vector_idx = row_group_start / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t end_vector_idx = (row_group_end - 1) / vector::DEFAULT_VECTOR_CAPACITY;
        fill_vector_info(end_vector_idx);

        for (uint64_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
            uint64_t vector_start = vector_idx == start_vector_idx
                                        ? row_group_start - start_vector_idx * vector::DEFAULT_VECTOR_CAPACITY
                                        : 0;
            uint64_t vector_end = vector_idx == end_vector_idx
                                      ? row_group_end - end_vector_idx * vector::DEFAULT_VECTOR_CAPACITY
                                      : vector::DEFAULT_VECTOR_CAPACITY;
            if (vector_start == 0 && vector_end == vector::DEFAULT_VECTOR_CAPACITY) {
                auto constant_info = std::make_unique<chunk_constant_info>(
                    start_ + static_cast<int64_t>(vector_idx * vector::DEFAULT_VECTOR_CAPACITY));
                constant_info->insert_id = transaction.transaction_id;
                constant_info->delete_id = NOT_DELETED_ID;
                vector_info_[vector_idx] = std::move(constant_info);
            } else {
                chunk_vector_info* new_info;
                if (!vector_info_[vector_idx]) {
                    auto insert_info = std::make_unique<chunk_vector_info>(
                        start_ + static_cast<int64_t>(vector_idx * vector::DEFAULT_VECTOR_CAPACITY));
                    new_info = insert_info.get();
                    vector_info_[vector_idx] = std::move(insert_info);
                } else if (vector_info_[vector_idx]->type == chunk_info_type::VECTOR_INFO) {
                    new_info = &vector_info_[vector_idx]->cast<chunk_vector_info>();
                } else {
                    assert(false && "Error in row_version_manager_t::append_version_info - "
                                    "expected either a chunk_vector_info or no version info");
                    std::abort();
                }
                new_info->append(vector_start, vector_end, transaction.transaction_id);
            }
        }
    }

    void row_version_manager_t::commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count) {
        if (count == 0) {
            return;
        }
        uint64_t row_group_end = row_group_start + count;

        std::lock_guard lock(version_lock_);
        uint64_t start_vector_idx = row_group_start / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t end_vector_idx = (row_group_end - 1) / vector::DEFAULT_VECTOR_CAPACITY;
        for (uint64_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
            uint64_t vstart = vector_idx == start_vector_idx
                                  ? row_group_start - start_vector_idx * vector::DEFAULT_VECTOR_CAPACITY
                                  : 0;
            uint64_t vend = vector_idx == end_vector_idx
                                ? row_group_end - end_vector_idx * vector::DEFAULT_VECTOR_CAPACITY
                                : vector::DEFAULT_VECTOR_CAPACITY;
            auto& info = *vector_info_[vector_idx];
            info.commit_append(commit_id, vstart, vend);
        }
    }

    void row_version_manager_t::cleanup_append(uint64_t lowest_active_transaction,
                                               uint64_t row_group_start,
                                               uint64_t count) {
        if (count == 0) {
            return;
        }
        uint64_t row_group_end = row_group_start + count;

        std::lock_guard lock(version_lock_);
        uint64_t start_vector_idx = row_group_start / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t end_vector_idx = (row_group_end - 1) / vector::DEFAULT_VECTOR_CAPACITY;
        for (uint64_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
            uint64_t vcount = vector_idx == end_vector_idx
                                  ? row_group_end - end_vector_idx * vector::DEFAULT_VECTOR_CAPACITY
                                  : vector::DEFAULT_VECTOR_CAPACITY;
            if (vcount != vector::DEFAULT_VECTOR_CAPACITY) {
                continue;
            }
            if (vector_idx >= vector_info_.size() || !vector_info_[vector_idx]) {
                continue;
            }
            auto& info = *vector_info_[vector_idx];
            std::unique_ptr<chunk_info> new_info;
            auto cleanup = info.cleanup(lowest_active_transaction, new_info);
            if (cleanup) {
                vector_info_[vector_idx] = std::move(new_info);
            }
        }
    }

    void row_version_manager_t::revert_append(uint64_t start_row) {
        std::lock_guard lock(version_lock_);
        uint64_t start_vector_idx =
            (start_row + (vector::DEFAULT_VECTOR_CAPACITY - 1)) / vector::DEFAULT_VECTOR_CAPACITY;
        for (uint64_t vector_idx = start_vector_idx; vector_idx < vector_info_.size(); vector_idx++) {
            vector_info_[vector_idx].reset();
        }
    }

    chunk_vector_info& row_version_manager_t::vector_info(uint64_t vector_idx) {
        fill_vector_info(vector_idx);

        if (!vector_info_[vector_idx]) {
            vector_info_[vector_idx] = std::make_unique<chunk_vector_info>(
                start_ + static_cast<int64_t>(vector_idx * vector::DEFAULT_VECTOR_CAPACITY));
        } else if (vector_info_[vector_idx]->type == chunk_info_type::CONSTANT_INFO) {
            auto& constant = vector_info_[vector_idx]->cast<chunk_constant_info>();
            auto new_info = std::make_unique<chunk_vector_info>(
                start_ + static_cast<int64_t>(vector_idx * vector::DEFAULT_VECTOR_CAPACITY));
            new_info->insert_id = constant.insert_id;
            for (uint64_t i = 0; i < vector::DEFAULT_VECTOR_CAPACITY; i++) {
                new_info->inserted[i] = constant.insert_id;
            }
            vector_info_[vector_idx] = std::move(new_info);
        }
        assert(vector_info_[vector_idx]->type == chunk_info_type::VECTOR_INFO);
        return vector_info_[vector_idx]->cast<chunk_vector_info>();
    }

    uint64_t
    row_version_manager_t::delete_rows(uint64_t vector_idx, uint64_t transaction_id, int64_t rows[], uint64_t count) {
        std::lock_guard lock(version_lock_);
        has_changes_ = true;
        return vector_info(vector_idx).delete_rows(transaction_id, rows, count);
    }

    void row_version_manager_t::commit_delete(uint64_t vector_idx, uint64_t commit_id, const delete_info& info) {
        std::lock_guard lock(version_lock_);
        has_changes_ = true;
        vector_info(vector_idx).commit_delete(commit_id, info);
    }

    void row_version_manager_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        std::lock_guard lock(version_lock_);
        for (auto& info : vector_info_) {
            if (info && info->type == chunk_info_type::VECTOR_INFO) {
                info->cast<chunk_vector_info>().commit_all_deletes(txn_id, commit_id);
            }
        }
    }

    void row_version_manager_t::revert_all_deletes(uint64_t txn_id) {
        std::lock_guard lock(version_lock_);
        for (auto& info : vector_info_) {
            if (info && info->type == chunk_info_type::VECTOR_INFO) {
                info->cast<chunk_vector_info>().revert_all_deletes(txn_id);
            }
        }
    }

} // namespace components::table
