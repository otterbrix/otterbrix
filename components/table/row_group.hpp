#pragma once
#include "column_data.hpp"
#include "row_version_manager.hpp"
#include "storage/data_pointer.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <optional>

namespace components::vector {
    class data_chunk_t;
}

namespace components::table {

#ifdef DEV_MODE
    // Test-observable count of STRING cells the late-materialisation gather would leave BORROWED
    // from a pin that dies with the gather while the result chunk outlives it (see the guard on
    // result_outlives_pins in row_group.cpp). Must stay at zero.
    uint64_t gathered_borrowed_strings() noexcept;
    // Per-row fetches issued by the in-memory predicate evaluation.
    uint64_t predicate_row_fetches() noexcept;
    uint64_t string_materializations() noexcept;
    uint64_t gather_rows_fetched() noexcept;
    uint64_t escaping_borrowed_cells() noexcept;
    void note_escaping_borrowed_cells(uint64_t cells) noexcept;
    void note_gather_row_fetched() noexcept;
    void note_string_materialization() noexcept;
    void reset_gathered_borrowed_strings() noexcept;
#endif
    class row_version_manager_t;

    constexpr static uint64_t MAX_ROW_GROUP_SIZE = uint64_t(1) << 30;

    class data_table_t;
    enum class table_scan_type : uint8_t;
    class scan_filter_info;
    class collection_scan_state;
    class column_definition_t;
    class collection_t;

    class row_group_t : public segment_base_t<row_group_t> {
    public:
        friend class column_data_t;

        row_group_t(collection_t* collection, int64_t start, uint64_t count);
        ~row_group_t() = default;

    private:
        collection_t* collection_;
        // ONE row-version manager, kept in TWO representations, with an invariant between them.
        //
        //   version_info_ is a NON-OWNING cache and the lock-free read path — version_info(),
        //   committed_row_count, has_version_above and move_to_collection all go through it. It
        //   keeps nothing alive.
        //
        //   owned_version_info_ OWNS. The manager is SHARED: add_column / remove_column give the
        //   ALTER successor's row group a second owning reference to the SAME object, so a table
        //   and its successor agree about which rows are deleted, and the last of the two row
        //   groups to die frees it. Shared ownership is carried by the count inside
        //   row_version_manager_t (see the note on that class), not by a control block.
        //
        // INVARIANT: version_info_ is either null or names exactly the object owned_version_info_
        // owns — never a different one, never a freed one. set_version_info is the SOLE writer of
        // both; see the note there for the publication order that keeps this true and for the
        // one transition it is valid for.
        std::atomic<row_version_manager_t*> version_info_ = nullptr;
        boost::intrusive_ptr<row_version_manager_t> owned_version_info_;
        uint64_t current_version_ = 0;
        // SHARED with the row groups of this group's ALTER successors: add_column / remove_column
        // copy this vector into the successor, so both point at the SAME column objects and the
        // last row group to die frees them. Shared ownership is carried by the count inside
        // column_data_t (see the note on the class), not by a control block.
        std::vector<boost::intrusive_ptr<column_data_t>> columns_;

    public:
        void move_to_collection(collection_t* collection, int64_t new_start);
        collection_t& collection() { return *collection_; }

        storage::block_manager_t& block_manager();

        // TODO: type casting
        // std::unique_ptr<row_group_t> alter_type(collection_t* collection, const types::complex_logical_type &target_type, uint64_t changed_idx,
        // collection_scan_state &scan_state, vector::data_chunk_t &scan_chunk);
        std::unique_ptr<row_group_t> add_column(collection_t* collection,
                                                column_definition_t& new_column,
                                                const std::optional<types::logical_value_t>& default_value,
                                                vector::vector_t& intermediate);
        std::unique_ptr<row_group_t> remove_column(collection_t* collection, uint64_t removed_column);

        void initialize_empty(const std::pmr::vector<types::complex_logical_type>& types);

        bool initialize_scan(collection_scan_state& state);
        bool initialize_scan_with_offset(collection_scan_state& state, uint64_t vector_offset);
        bool check_zonemap_segments(collection_scan_state& state);
        void scan(collection_scan_state& state, vector::data_chunk_t& result);
        void scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type);

        // Run the filter's graph over one vector's rows and return the per-row decision, indexed by
        // vector offset. `error` carries an out_of_memory error_t when a pin fails mid-materialisation.
        core::result_wrapper_t<vector::vector_t>
        evaluate_predicate(const table_filter_t& filter, int64_t base_row, uint64_t count);

        void fetch_row(column_fetch_state& state,
                       const std::vector<storage_index_t>& column_ids,
                       int64_t row_id,
                       vector::data_chunk_t& result,
                       uint64_t result_idx,
                       const std::vector<size_t>& projected_cols);

        // Point-fetch visibility gate — the predicate fetch_row must be asked BEFORE it
        // gathers, and the only reader of row_version_manager_t::fetch. `row_id` is
        // collection-ABSOLUTE: that manager keeps the absolute contract for this one
        // method and rebases to its group-local slots internally (A6), so nothing here
        // rebases and nothing here may start.
        //
        // A group with no version manager has recorded no insert and no delete, so every
        // one of its rows is visible — the same answer indexing_vector gives a scan over
        // a null chunk_info, and the reason this cannot silently hide rows.
        bool is_visible(const transaction_data& txn, int64_t row_id);

        void append_version_info(transaction_data txn, uint64_t count);

        void commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count);
        void revert_append(uint64_t row_group_start);

        uint64_t delete_rows(uint64_t vector_idx, int64_t rows[], uint64_t count);
        uint64_t delete_rows(data_table_t& table, int64_t* row_ids, uint64_t count, uint64_t transaction_id);
        void commit_delete(uint64_t commit_id, uint64_t vector_idx, const delete_info& info);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);

        uint64_t committed_row_count();
        // True when any version stamp in this row group is above `watermark`
        // (pending txn id or commit id newer than the visible-to-all horizon).
        bool has_version_above(uint64_t watermark);

        // The append chain returns out_of_memory when a column segment allocation fails;
        // true on success.
        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(row_group_append_state& append_state);
        [[nodiscard]] core::result_wrapper_t<bool>
        append(row_group_append_state& append_state, vector::data_chunk_t& chunk, uint64_t append_count);

        // Update path returns write_conflict / out_of_memory; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> update(vector::data_chunk_t& updates,
                                                          int64_t* ids,
                                                          uint64_t offset,
                                                          uint64_t count,
                                                          const std::vector<uint64_t>& column_ids);
        [[nodiscard]] core::result_wrapper_t<bool> update_column(vector::data_chunk_t& updates,
                                                                 vector::vector_t& row_ids,
                                                                 const std::vector<uint64_t>& column_path);

        void get_column_segment_info(uint64_t row_group_index, std::vector<column_segment_info>& result);

        // Append the ids of disk blocks exclusively owned by this row group's columns (and their
        // sub-columns) to `out`, so a compacting caller can free them after swapping the collection.
        void collect_disk_block_ids(std::pmr::vector<uint64_t>& out);

        // Same walk, restricted to ONE top-level column (and its sub-columns). The caller is
        // table_storage_t::drop_column, which has to name the outgoing column's blocks BEFORE the
        // rebuild destroys the column object — after it, nothing can enumerate them again. The ids
        // are NOT proven exclusive here: B2 packs segments of several columns into one block, so a
        // reported id may still belong to a surviving column. Proving that is the release site's
        // job (see table_storage_t::checkpoint).
        void collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out);

        // The checkpoint chain returns out_of_memory when a column flush pin fails;
        // the row group pointer on success.
        [[nodiscard]] core::result_wrapper_t<storage::row_group_pointer_t>
        write_to_disk(storage::partial_block_manager_t& partial_block_manager);
        // Disk load: rebuilds every column (with its persisted validity and nested children)
        // from the row-group pointer. A malformed pointer — wrong column count, a column tree
        // missing its validity child, a short validity record — is data_corruption; the load
        // fails loudly instead of returning a half-valid table.
        [[nodiscard]] core::result_wrapper_t<bool> create_from_pointer(const storage::row_group_pointer_t& pointer);

        // Write-through: re-point every COMPLETE managed column segment of this row group to a
        // disk-backed segment (call once the row group is closed -> its segments are final). Returns
        // io_error/out_of_memory on failure; true on success.
        //
        // Owns a short-lived partial_block_manager_t so this closed row group's column segments are PACKED
        // into shared blocks (segment packing) and flushed to disk BEFORE returning -- the flush is the
        // flush-before-evict guarantee: once transition_to_disk returns, every re-pointed segment's block is
        // durable, so a later scan/eviction can safely load() it.
        [[nodiscard]] core::result_wrapper_t<bool> transition_to_disk();

        uint64_t allocation_size() const { return allocation_size_; }

        void next_vector(collection_scan_state& state);

        uint64_t row_group_size() const;
        row_version_manager_t& get_or_create_version_info();
        boost::intrusive_ptr<row_version_manager_t> get_or_create_version_info_ptr();

        uint64_t calculate_size();

#ifdef DEV_MODE
        // Test-observable IDENTITY of top-level column `c`: its object address and the number of
        // row groups that currently own it. add_column / remove_column must hand the successor's
        // row group the SAME column objects, not copies — and a deep copy is invisible to every
        // scan, count and checksum a test could take, because it reads back exactly the same data.
        // The address and the reference count are the only things that tell the two apart, so the
        // structural-sharing gate (test_alter_column_sharing.cpp) asserts on these.
        const column_data_t* column_identity(uint64_t c) const;
        uint64_t column_owner_count(uint64_t c) const;

        // Test-observable IDENTITY of this row group's row-version manager, in BOTH of the two
        // representations the group keeps of it: the object the group OWNS, and the raw pointer
        // the lock-free read path publishes. add_column / remove_column must hand the successor's
        // row group the SAME manager, not a fresh one — and a fresh one is invisible to every
        // scan and count a test could take on a freshly-ALTERed table, because a manager with no
        // deletes recorded answers "visible" exactly like the shared one does. Only the address
        // and the owner count tell them apart. The third observer exists because the owner and
        // the published raw pointer are the invariant this pair has to keep (see
        // set_version_info): the gate asserts they agree, so a conversion that publishes the
        // wrong pointer, or forgets to publish, cannot pass.
        // Gate: test_alter_version_sharing.cpp.
        const row_version_manager_t* version_manager_identity() const;
        const row_version_manager_t* version_manager_published() const;
        uint64_t version_manager_owner_count() const;
#endif

    private:
        uint64_t indexing_vector(transaction_data txn,
                                 uint64_t vector_idx,
                                 vector::indexing_vector_t& indexing_vector,
                                 uint64_t max_count);
        boost::intrusive_ptr<row_version_manager_t> get_or_create_version_info_internal();
        row_version_manager_t* version_info();
        void set_version_info(boost::intrusive_ptr<row_version_manager_t> version);
        column_data_t& get_column(uint64_t c);
        column_data_t& get_column(const storage_index_t& c);
        uint64_t get_column_count() const;
        std::vector<boost::intrusive_ptr<column_data_t>>& columns();

        void filter_indexing(std::pmr::memory_resource* resource,
                             uint64_t vector_index,
                             vector::indexing_vector_t& indexing,
                             const table_filter_t* filter,
                             uint64_t vector_count,
                             uint64_t& approved_tuple_count,
                             core::error_t& error);

        template<table_scan_type TYPE>
        void templated_scan(collection_scan_state& state, vector::data_chunk_t& result);

        bool has_unloaded_deletes() const;

        // Single-owner: see the proof on data_table_t (components/table/data_table.hpp).
        std::vector<storage::meta_block_pointer_t> column_pointers_;
        std::unique_ptr<std::atomic<bool>[]> is_loaded_;
        std::vector<storage::meta_block_pointer_t> deletes_pointers_;
        std::atomic<bool> deletes_is_loaded_;
        uint64_t allocation_size_;
    };
} // namespace components::table