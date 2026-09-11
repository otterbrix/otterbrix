#pragma once
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include "column_data.hpp"
#include "row_version_manager.hpp"
#include "table_state.hpp"

#include "column_definition.hpp"

namespace components::table::storage {
    struct row_group_pointer_t;
    class partial_block_manager_t;
} // namespace components::table::storage

namespace components::table {

    class data_table_t;

    // DEFAULT-or-NULL for an unmaterialized column; shared by table_storage_adapter.hpp and
    // row_group_t so SELECT and WHERE don't diverge again (one saw the default, the other matched nothing).
    void fill_published_default(vector::vector_t& target, const column_definition_t* published, uint64_t rows);

    class row_group_segment_tree_t : public segment_tree_t<row_group_t, true> {
    public:
        explicit row_group_segment_tree_t(collection_t& collection);
        ~row_group_segment_tree_t() override = default;

    protected:
        collection_t& collection_;
        uint64_t current_row_group_;
        uint64_t max_row_group_;
    };

    // A copy of this pointer taken before data_table_t::compact swaps in a rebuilt collection must
    // outlive the swap — block_manager_t::unregister_block's identity check depends on it
    // (test_root_reclaim, test_block_manager).
    class collection_t final : public boost::intrusive_ref_counter<collection_t> {
    public:
        collection_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::pmr::vector<types::complex_logical_type> types,
                     int64_t row_start,
                     uint64_t total_rows = 0,
                     uint64_t row_group_size = vector::DEFAULT_VECTOR_CAPACITY);
        ~collection_t();

        uint64_t total_rows() const;
        uint64_t committed_row_count() const;
        bool has_version_above(uint64_t watermark) const;

        bool is_empty() const;

        void append_row_group(std::unique_lock<std::mutex>& l, int64_t start_row);
        row_group_t* append_row_group(int64_t start_row);
        row_group_t* row_group(int64_t index);

        void initialize_scan(collection_scan_state& state, const std::vector<storage_index_t>& column_ids);
        void initialize_create_index_scan(create_index_scan_state& state);
        void initialize_scan_with_offset(collection_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);
        static bool initialize_scan_in_row_group(collection_scan_state& state,
                                                 collection_t& collection,
                                                 row_group_t& row_group,
                                                 uint64_t vector_index,
                                                 int64_t max_row);

        // SNAPSHOT gathers only rows visible to `txn`; RAW skips that check (CREATE INDEX backfill
        // reads deleted rows on purpose). An invisible or unmatched row id shortens result.row_ids
        // rather than being masked, so the reply is not positional with the request.
        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_identifiers,
                   uint64_t fetch_count,
                   column_fetch_state& state,
                   const std::vector<size_t>& projected_cols,
                   const transaction_data& txn,
                   fetch_visibility_t visibility);

        // NOT_DELETED_ID when no delete was recorded (or the id names no row group), else a commit
        // id (committed) or transaction id (pending) — the read-only companion of fetch's RAW visibility.
        uint64_t delete_stamp(int64_t row_id);

        // append's bool, on success, reports whether a new row group was started (not plain success).
        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(table_append_state& state);
        [[nodiscard]] core::result_wrapper_t<bool> append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        // Best-effort: every row group gets a chance to truncate before the first refusal is reported.
        core::result_wrapper_t<bool> revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);
        void cleanup_append(int64_t start, uint64_t count);

        void merge_storage(collection_t& data);

        [[nodiscard]] core::result_wrapper_t<uint64_t>
        delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id);
        // write_conflict or out_of_memory on failure.
        [[nodiscard]] core::result_wrapper_t<bool>
        update(int64_t* ids, const std::vector<uint64_t>& column_ids, vector::data_chunk_t& updates);
        [[nodiscard]] core::result_wrapper_t<bool> update_column(vector::vector_t& row_ids,
                                                                 const std::vector<uint64_t>& column_path,
                                                                 vector::data_chunk_t& updates);

        std::vector<column_segment_info> get_column_segment_info();

        // Exclusively owned blocks only; data_table_t::compact frees them after swapping this collection out.
        void collect_disk_block_ids(std::pmr::vector<uint64_t>& out);

        // Candidates only, not proven-exclusive (see row_group_t::collect_column_disk_block_ids).
        void collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out);

        const std::pmr::vector<types::complex_logical_type>& types() const;
        void adopt_types(std::pmr::vector<types::complex_logical_type> types);

        // An ALTER successor's row groups share this collection's column objects and row-version
        // managers (row_group_t::add_column/remove_column), so the parent stays readable while it installs.
        [[nodiscard]] core::result_wrapper_t<boost::intrusive_ptr<collection_t>>
        add_column(column_definition_t& new_column);
        boost::intrusive_ptr<collection_t> remove_column(uint64_t col_idx);
        // TODO: type casting
        // std::shared_ptr<collection_t> alter_type(uint64_t changed_idx, const types::complex_logical_type &target_type,
        // std::vector<storage_index_t> bound_columns);

        // out_of_memory when a column flush pin fails.
        [[nodiscard]] core::result_wrapper_t<std::vector<storage::row_group_pointer_t>>
        checkpoint(storage::partial_block_manager_t& partial_block_manager);

        storage::block_manager_t& block_manager() { return block_manager_; }

        uint64_t allocation_size() const { return allocation_size_; }

        uint64_t row_group_size() const { return row_group_size_; }

        row_group_segment_tree_t* row_group_tree() { return row_groups_.get(); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        uint64_t calculate_size();
        void cleanup_versions(uint64_t lowest_active_start_time);

        void set_total_rows(uint64_t total) { total_rows_ = total; }

        // Rebound on every read (table_storage_adapter_t::begin_read), not fixed at construction —
        // compact/add_column/remove_column replace the collection and would drop a construction-time binding.
        void publish_unmaterialized_columns(const std::vector<column_definition_t>* columns) noexcept {
            unmaterialized_ = columns;
        }
        // nullptr past the materialized schema; fill_published_default reads that as all-NULL.
        const column_definition_t* published_column(size_t offset) const noexcept {
            return unmaterialized_ != nullptr && offset < unmaterialized_->size() ? &(*unmaterialized_)[offset]
                                                                                  : nullptr;
        }

    private:
        bool is_empty(std::unique_lock<std::mutex>&) const;

        std::pmr::memory_resource* resource_;
        storage::block_manager_t& block_manager_;
        uint64_t row_group_size_;
        std::atomic<uint64_t> total_rows_;
        std::pmr::vector<types::complex_logical_type> types_;
        int64_t row_start_;
        // Exclusive; a shared_ptr stood here though nothing shared it -- every consumer uses .get()/operator->.
        std::unique_ptr<row_group_segment_tree_t> row_groups_;
        uint64_t allocation_size_;
        const std::vector<column_definition_t>* unmaterialized_ = nullptr;
    };

} // namespace components::table