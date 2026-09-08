#pragma once

#include <utility>

#include "collection.hpp"
#include "storage/metadata_reader.hpp"
#include "storage/metadata_writer.hpp"

namespace components::table {

#ifdef DEV_MODE
    // Rows delivered by scan; a write-path handler must never pay a read that grows with the table.
    uint64_t table_scan_rows_streamed() noexcept;
    void reset_table_scan_rows_streamed() noexcept;
#endif

    class data_table_t {
    public:
        data_table_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::vector<column_definition_t> column_definitions,
                     std::string name = "temp");
        // A failed backfill can't be returned from a constructor, so it LATCHES: has_construction_error()
        // answers true, and the parent stays root with writes to the new column refused.
        data_table_t(data_table_t& parent, column_definition_t& new_column);
        data_table_t(data_table_t& parent, uint64_t removed_column);

        bool has_construction_error() const noexcept { return construction_error_.contains_error(); }
        const core::error_t& construction_error() const noexcept { return construction_error_; }

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> copy_types() const;
        const std::vector<column_definition_t>& columns() const;
        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& types);

        void initialize_scan(table_scan_state& state,
                             const std::vector<storage_index_t>& column_ids,
                             const table_filter_t* filter = nullptr);

        uint64_t max_threads() const;

        void scan(vector::data_chunk_t& result, table_scan_state& state);
        void scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                          const std::vector<size_t>* projected_cols,
                          std::pmr::vector<vector::data_chunk_t>& batches,
                          table_scan_state& state,
                          std::pmr::memory_resource* resource);

        // DORMANT: kept for the future buffer-pool bounded scan, not yet wired — do not delete.
        [[nodiscard]] core::result_wrapper_t<bool> fetch_next_batch(vector::data_chunk_t& result,
                                                                    const std::vector<storage_index_t>& column_ids,
                                                                    const table_filter_t* filter,
                                                                    transaction_data txn,
                                                                    int64_t& next_row,
                                                                    int64_t max_row,
                                                                    bool& drained);

        // `txn`/`visibility` have no default: under SNAPSHOT the collection drops rows the
        // transaction may not see; under RAW it drops nothing (CREATE INDEX backfill needs deleted rows).
        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_ids,
                   uint64_t fetch_count,
                   column_fetch_state& state,
                   const std::vector<size_t>& projected_cols,
                   const transaction_data& txn,
                   fetch_visibility_t visibility);

        std::unique_ptr<table_delete_state>
        initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        delete_rows(table_delete_state& state, vector::vector_t& row_ids, uint64_t count, uint64_t transaction_id);

        std::unique_ptr<table_update_state>
        initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        // NOT A TRANSACTIONAL UPDATE: the overlay it writes publishes immediately with no version
        // chain or undo; the txn-carrying UPDATE a statement runs is delete-stamp + append instead.
        [[nodiscard]] core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(table_update_state& state,
               vector::vector_t& row_ids,
               // const std::vector<uint64_t>& column_ids,
               vector::data_chunk_t& data);
        [[nodiscard]] core::result_wrapper_t<bool> update_column(vector::vector_t& row_ids,
                                                                 const std::vector<uint64_t>& column_path,
                                                                 vector::data_chunk_t& updates);

        // write_conflict when concurrent DDL altered the table (no longer root); true on success.
        [[nodiscard]] core::result_wrapper_t<bool> append_lock(table_append_state& state);
        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(table_append_state& state);
        [[nodiscard]] core::result_wrapper_t<bool> append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        core::result_wrapper_t<bool> revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);

        void merge_storage(collection_t& data);

        uint64_t column_count() const;

        std::vector<column_segment_info> get_column_segment_info();
        bool create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type);

        std::unique_ptr<constraint_state>
        initialize_constraint_state(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        std::string table_name() const;
        void set_table_name(std::string new_name);

        // Renames IN PLACE, no successor table (a name isn't part of any segment/block, so
        // existing holders stay valid). true=renamed; false=no such column here (legitimate for
        // ALTER ADD COLUMN's catalog-only case); error=`new_name` already exists.
        // NOT COSMETIC: bootstrap reconciliation matches by attoid, but the append and drop paths
        // address storage BY NAME; renaming in place keeps that cache from going stale.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_name,
                                                                 const std::string& new_name);

        uint64_t row_group_size() const;

        boost::intrusive_ptr<collection_t> row_group() const;

        // Sole caller (drop_column) must run this BEFORE the rebuild forgets the dropped column, or never.
        void collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) const;

        uint64_t calculate_size();
        void cleanup_versions(uint64_t lowest_active_start_time);
        // Drops version history, keeping only rows visible to a "see all committed" scan — only
        // when every stamp is at/below compact_watermark (older snapshots still need it).
        bool compact(uint64_t compact_watermark);

        // Bumped inside compact() whenever row_groups_ swaps (row ids MAY be renumbered); an index
        // answer stamped with an older epoch is refused by storage_fetch.
        [[nodiscard]] uint64_t compact_epoch() const noexcept { return compact_epoch_; }

        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(storage::metadata_writer_t& writer);

        // True unless byte-for-byte on disk already. Lives here (not table_storage_t) so every
        // mutating method marks it directly; cleared only by load_from_disk and checkpoint's write_header commit.
        [[nodiscard]] bool modified_since_checkpoint() const noexcept { return modified_since_checkpoint_; }
        void clear_modified_since_checkpoint() noexcept { modified_since_checkpoint_ = false; }
        // data_corruption (not a throw) when the on-disk metadata chain is truncated/corrupt.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<data_table_t>>
        load_from_disk(std::pmr::memory_resource* resource,
                       storage::block_manager_t& block_manager,
                       storage::metadata_reader_t& reader);

#ifdef DEV_MODE
        const collection_t* collection_identity() const;
        uint64_t collection_owner_count() const;
#endif

    private:
        // Plain bool, not atomic: same single-actor ownership as row_groups_ below.
        void mark_modified() noexcept { modified_since_checkpoint_ = true; }

        void initialize_scan_with_offset(table_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);

        std::pmr::memory_resource* resource_;
        core::error_t construction_error_{core::error_t::no_error()};
        std::vector<column_definition_t> column_definitions_;
        // NO LOCK: reachable from exactly one disk agent, and actor-zeta resumes an actor on at
        // most one thread (the parallel replay loop was removed for racing here, TSan-confirmed).
        boost::intrusive_ptr<collection_t> row_groups_;
        // false = superseded by an ALTER successor; destroyed in the same statement that installs
        // the successor, so this should never be observed false live.
        std::atomic<bool> is_root_;
        bool modified_since_checkpoint_{true};
        uint64_t compact_epoch_{0};
        std::string name_;
    };

} // namespace components::table