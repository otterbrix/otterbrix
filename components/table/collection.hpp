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

    // Cells of an UNMATERIALIZED column (published by pg_attribute, no row group backs it yet):
    // the catalog DEFAULT, or all-NULL. Both readers — table_storage_adapter.hpp
    // fill_unmaterialized and row_group_t::evaluate_predicate — must share this one function:
    // they used to answer it apart and disagreed (SELECT saw the default, WHERE matched nothing).
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

    // Ownership: SHARED and outlives the sharer. data_table_t::compact replaces the collection
    // with a compacted rebuild and frees the outgoing one's blocks; a copy taken before the swap
    // keeps the replaced collection (and its block_handle_t's) alive until released — load-bearing
    // for block_manager_t::unregister_block's identity check (see test_root_reclaim,
    // test_block_manager). Count lives inside the object (intrusive_ref_counter; shared_ptr
    // is forbidden). `final` is load-bearing too: no virtual destructor needed, allocated
    // with plain `new`. No weak reference to a collection may exist — intrusive_ref_counter has
    // no weak analogue.
    class collection_t final : public boost::intrusive_ref_counter<collection_t> {
    public:
        collection_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::pmr::vector<types::complex_logical_type> types,
                     int64_t row_start,
                     uint64_t total_rows = 0,
                     uint64_t row_group_size = vector::DEFAULT_VECTOR_CAPACITY);
        // Out-of-line: destroying the unique_ptr member needs row_group_t complete, so this
        // can't be defaulted in every TU that merely includes this header.
        ~collection_t();

        uint64_t total_rows() const;
        uint64_t committed_row_count() const;
        // True when any row group holds a version stamp above `watermark` —
        // i.e. some version is NOT visible-to-all snapshots at/below it.
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

        // Point fetch by row id. SNAPSHOT gathers only rows visible to `txn`; RAW skips the
        // check (the CREATE INDEX backfill reads deleted rows on purpose) — no default mode.
        // result.row_ids is stamped one slot per row actually gathered: an invisible or
        // unmatched id shortens the answer rather than being masked, so the reply is not
        // positional with the request.
        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_identifiers,
                   uint64_t fetch_count,
                   column_fetch_state& state,
                   const std::vector<size_t>& projected_cols,
                   const transaction_data& txn,
                   fetch_visibility_t visibility);

        // The append chain returns out_of_memory when a row group / column segment allocation
        // fails. initialize_append: true on success. append: on success the bool reports whether
        // a new row group was started.
        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(table_append_state& state);
        [[nodiscard]] core::result_wrapper_t<bool> append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        // Best-effort across row groups (see row_group_t::revert_append); the first
        // refusal is reported after every group had its chance to truncate.
        core::result_wrapper_t<bool> revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);
        void cleanup_append(int64_t start, uint64_t count);

        void merge_storage(collection_t& data);

        uint64_t delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id);
        // Update path returns write_conflict / out_of_memory; true on success.
        [[nodiscard]] core::result_wrapper_t<bool>
        update(int64_t* ids, const std::vector<uint64_t>& column_ids, vector::data_chunk_t& updates);
        [[nodiscard]] core::result_wrapper_t<bool> update_column(vector::vector_t& row_ids,
                                                                 const std::vector<uint64_t>& column_path,
                                                                 vector::data_chunk_t& updates);

        std::vector<column_segment_info> get_column_segment_info();

        // Append the ids of disk blocks exclusively owned by this collection's columns to `out`,
        // so data_table_t::compact can free them after swapping the collection out for a compacted one.
        void collect_disk_block_ids(std::pmr::vector<uint64_t>& out);

        // The same walk restricted to ONE top-level column, across every row group. Reported ids
        // are candidates, not proven-exclusive blocks — see row_group_t::collect_column_disk_block_ids.
        void collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out);

        const std::pmr::vector<types::complex_logical_type>& types() const;
        void adopt_types(std::pmr::vector<types::complex_logical_type> types);

        // ALTER successors: build a whole new collection whose row groups SHARE this
        // collection's column objects and row-version managers (row_group_t::add_column /
        // remove_column), so the parent stays readable while the successor is installed.
        // add_column returns out_of_memory on a backfill failure; no successor is built then.
        [[nodiscard]] core::result_wrapper_t<boost::intrusive_ptr<collection_t>>
        add_column(column_definition_t& new_column);
        boost::intrusive_ptr<collection_t> remove_column(uint64_t col_idx);
        // TODO: type casting
        // std::shared_ptr<collection_t> alter_type(uint64_t changed_idx, const types::complex_logical_type &target_type,
        // std::vector<storage_index_t> bound_columns);

        // The checkpoint chain returns out_of_memory when a column flush pin fails;
        // the row group pointers on success.
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

        // Columns pg_attribute publishes that no row group here holds. BORROWED and REBOUND on
        // every read (table_storage_adapter_t::begin_read), not once at construction: compact /
        // add_column / remove_column all replace the collection, which would silently drop a
        // construction-time binding. Read by row_group_t::evaluate_predicate, whose pushed-down
        // filter can bind an ordinal past the last materialized column.
        void publish_unmaterialized_columns(const std::vector<column_definition_t>* columns) noexcept {
            unmaterialized_ = columns;
        }
        // The column `offset` slots past the materialized schema, or nullptr when nothing is
        // published for it — which fill_published_default reads as all-NULL.
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
        // EXCLUSIVE ownership (a shared_ptr stood here on a member nothing ever shared —
        // every consumer takes .get() or operator->).
        std::unique_ptr<row_group_segment_tree_t> row_groups_;
        uint64_t allocation_size_;
        // BORROWED, may be null. See publish_unmaterialized_columns.
        const std::vector<column_definition_t>* unmaterialized_ = nullptr;
    };

} // namespace components::table