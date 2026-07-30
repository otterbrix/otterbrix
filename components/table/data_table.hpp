#pragma once

#include <utility>

#include "collection.hpp"
#include "storage/metadata_reader.hpp"
#include "storage/metadata_writer.hpp"

namespace components::table {

    class data_table_t {
    public:
        data_table_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::vector<column_definition_t> column_definitions,
                     std::string name = "temp");
        data_table_t(data_table_t& parent, column_definition_t& new_column);
        data_table_t(data_table_t& parent, uint64_t removed_column);
        data_table_t(data_table_t& parent,
                     uint64_t changed_idx,
                     const types::complex_logical_type& target_type,
                     const std::vector<storage_index_t>& bound_columns);

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> copy_types() const;
        const std::vector<column_definition_t>& columns() const;

        // M3-B4. Give each column of a chunk that came out of THIS table the catalog identity
        // of the table column it was read from, so the identity leaves the storage layer with
        // the data instead of having to be re-derived from a name downstream.
        //
        // Position-aligned, which is the same alignment every other table-vs-chunk loop in the
        // engine already assumes (NOT NULL enforcement and type promotion in agent_disk both
        // index table_columns[i] against data->data[i]). A projected scan keeps the width by
        // leaving placeholder columns in place, precisely so this alignment holds.
        //
        // Columns whose definition carries no attoid — a storage restored from .otbx, one
        // synthesized during WAL replay, one grown by dynamic schema growth — are stamped
        // INVALID_OID, which is the true answer for them and is what routes them by name.
        void stamp_column_identity(vector::data_chunk_t& chunk) const;

        // The recovery direction of stamp_column_identity: give a column that has NO
        // catalog identity the one pg_attribute records for it. A storage restored from
        // a pre-versioning .otbx, or synthesised during WAL replay from a chunk whose
        // codec carries no identity, comes back knowing only column names — so the name
        // is the only key the catalog row can be matched on, and it is exactly the key
        // such a column is already routed by.
        //
        // No-op when `attoid` is INVALID_OID, when no column carries that name, or when
        // the named column already has an identity. The immutability contract on
        // set_attoid is therefore never even reached, which is what makes this safe to
        // re-run and impossible to use to overwrite a live identity.
        //
        // Returns true when it stamped a column.
        bool stamp_missing_attoid(std::string_view column_name, std::uint32_t attoid);
        void adopt_schema(const std::pmr::vector<vector::column_schema_t>& schema);
        void overlay_not_null(const std::string& col_name);

        void initialize_scan(table_scan_state& state,
                             const std::vector<storage_index_t>& column_ids,
                             const table_filter_t* filter = nullptr);

        uint64_t max_threads() const;

        void scan(vector::data_chunk_t& result, table_scan_state& state);
        // Emits ≤DEFAULT_VECTOR_CAPACITY chunks straight from the scan, no concat-then-split.
        void scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                          const std::vector<size_t>* projected_cols,
                          std::pmr::vector<vector::data_chunk_t>& batches,
                          table_scan_state& state,
                          std::pmr::memory_resource* resource);

        // DORMANT: dormant foundation for the future buffer-pool bounded scan, not yet wired pending
        // the actor-zeta await-core fix (scan sources reverted to whole-scan buffering). Kept, not
        // deleted — the buffer-pool effort revives it.
        // Streaming fetch-next (STEP 3 / index-resume). Reads ONE ≤DEFAULT_VECTOR_CAPACITY batch
        // into `result`, RESUMING from absolute source row `next_row` (capped at `max_row`). Builds
        // a TRANSIENT table_scan_state, seeks to next_row, reads one batch, then advances `next_row`
        // past the SOURCE rows consumed (independent of how many the filter matched, so no row is
        // re-read) and sets `drained` once the scan reaches `max_row`. The scan state (and its
        // buffer pins) is local to this call and destroyed before return — ZERO pins survive.
        // Returns a buffer-pool OOM / data_corruption surfaced by the table-layer scan, else true.
        [[nodiscard]] core::result_wrapper_t<bool> fetch_next_batch(vector::data_chunk_t& result,
                                                                    const std::vector<storage_index_t>& column_ids,
                                                                    const table_filter_t* filter,
                                                                    transaction_data txn,
                                                                    int64_t& next_row,
                                                                    int64_t max_row,
                                                                    bool& drained);

        void fetch(vector::data_chunk_t& result,
                   const std::vector<storage_index_t>& column_ids,
                   const vector::vector_t& row_ids,
                   uint64_t fetch_count,
                   column_fetch_state& state);

        std::unique_ptr<table_delete_state>
        initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        uint64_t
        delete_rows(table_delete_state& state, vector::vector_t& row_ids, uint64_t count, uint64_t transaction_id);

        std::unique_ptr<table_update_state>
        initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        // Returns write_conflict / out_of_memory. On success the pair is
        // {0, affected-row count}; the caller's update reply carries it.
        [[nodiscard]] core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(table_update_state& state,
               vector::vector_t& row_ids,
               // const std::vector<uint64_t>& column_ids,
               vector::data_chunk_t& data);
        [[nodiscard]] core::result_wrapper_t<bool> update_column(vector::vector_t& row_ids,
                                                                 const std::vector<uint64_t>& column_path,
                                                                 vector::data_chunk_t& updates);

        // append_lock returns write_conflict when concurrent DDL altered the table
        // (the table is no longer root); true on success.
        [[nodiscard]] core::result_wrapper_t<bool> append_lock(table_append_state& state);
        // The append chain returns out_of_memory when a row group / column segment
        // allocation fails; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> initialize_append(table_append_state& state);
        [[nodiscard]] core::result_wrapper_t<bool> append(vector::data_chunk_t& chunk, table_append_state& state);
        void finalize_append(table_append_state& state, transaction_data txn);
        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count);
        void revert_append(int64_t row_start, uint64_t count);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);
        void scan_table_segment(int64_t start_row,
                                uint64_t count,
                                const std::function<void(vector::data_chunk_t& chunk)>& function);

        void merge_storage(collection_t& data);

        void set_as_root() { is_root_ = true; }

        bool is_root() { return is_root_; }

        uint64_t column_count() const;

        std::vector<column_segment_info> get_column_segment_info();
        bool create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type);

        std::unique_ptr<constraint_state>
        initialize_constraint_state(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        std::string table_name() const;
        void set_table_name(std::string new_name);

        uint64_t row_group_size() const;

        std::shared_ptr<collection_t> row_group() const;

        uint64_t calculate_size();
        void cleanup_versions(uint64_t lowest_active_start_time);
        // Rebuild row_groups_ keeping only rows visible to the txn-less
        // "see all committed" scan, dropping all version history. Runs ONLY when
        // every version stamp is at/below `compact_watermark` — the
        // visible-to-all horizon from transaction_manager_t::compact_watermark()
        // — otherwise it is a no-op returning false (MVCC: older snapshots and
        // in-flight commits still need the history). True = table is fully
        // compacted (or empty) and safe to checkpoint without version metadata.
        bool compact(uint64_t compact_watermark);

        // What compact_dropped_columns did. Two facts, because "removed nothing because
        // nothing was dead" and "removed nothing because the MVCC gate said no" are
        // different answers and the caller reports them differently.
        struct column_compaction_t {
            uint64_t removed{0};
            bool mvcc_refused{false};
        };

        // Physically remove every storage column whose catalog identity is in
        // `dead_attoids`, narrowing this table in place.
        //
        // This is the storage half of ALTER TABLE ... DROP COLUMN. The catalog half runs
        // at DDL time: pg_attribute loses the column and every ordinal above the scan is
        // resolved without it. The storage keeps the slot, because dropping it rewrites
        // every row group — and rewriting every row group is what this does, on VACUUM's
        // schedule. Until it runs the relation is DISPLACED (it holds a storage slot the
        // logical schema no longer names) and plan generation refuses column pruning,
        // filter pushdown, aggregate pushdown and index probes on it, all four of which
        // address a storage column by the logical ordinal the validator resolved.
        //
        // THE KEY IS THE ATTOID, never the position and never the name. A column carrying
        // NO identity is never removed: nothing on the catalog side can testify about it,
        // and matching it by name instead is precisely what ALTER TABLE ... RENAME COLUMN
        // makes wrong (a rename does not touch storage, so the storage still says the old
        // name). Refusing to compact such a relation costs it an optimisation; guessing
        // costs it a column.
        //
        // In place, NOT a rebuilt data_table_t: the storage adapter holds a data_table_t&
        // and the append/scan states point into this object, so replacing it out from under
        // them is what an earlier synchronous attempt did and is why it crashed
        // row_group::append on a column-count mismatch. Only row_groups_ and
        // column_definitions_ change, together, under the same swap compact() uses.
        //
        // MVCC-gated exactly as compact() is, and for the same reason: the rebuild scans
        // the committed rows and re-stamps them with transaction_data{0,0}, collapsing the
        // version history, which is only correct once every stamp is at/below
        // `compact_watermark`. Above it the compaction is REFUSED (mvcc_refused) and the
        // table is left exactly as it was, for a later round. Whether a dropped column is
        // dead ENOUGH to appear in `dead_attoids` is the CALLER's judgement: that gate
        // lives in start-time space (a snapshot older than the drop still resolves the
        // column and would then find no chunk column to match it), not in this commit-id
        // one.
        [[nodiscard]] column_compaction_t compact_dropped_columns(const std::pmr::vector<uint32_t>& dead_attoids,
                                                                  uint64_t compact_watermark);

        // Table-metadata stream versioning (the per-table record inside .otbx: name,
        // column list, row-group pointers).
        //
        // The stream used to open with the uint32 byte-length of the table name and had
        // no version of its own — the file-level main_header_t version covers the block
        // layer, not this record. A versioned stream opens with TABLE_META_MAGIC
        // instead, a value far larger than any length a real table name can have, so a
        // reader distinguishes the two layouts from the first uint32 alone and pre-
        // versioning files keep loading unchanged (their columns simply carry no
        // identity, which is the truth about them).
        //
        // The compatibility is one-way by construction: a build that predates the marker
        // reads the magic as a name length and fails on a file this build wrote.
        //
        // v1: adds the per-column attoid (pg_attribute identity) to the column record.
        static constexpr uint32_t TABLE_META_MAGIC = 0xFFFF0001u;
        static constexpr uint32_t TABLE_META_VERSION = 1;

        // The checkpoint chain returns out_of_memory when a column flush pin fails;
        // true on success.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(storage::metadata_writer_t& writer);
        // Returns data_corruption when the on-disk metadata chain is truncated/corrupt (the reader records
        // a sticky error during deserialize, checked here at the boundary) instead of throwing on the
        // load path. The caller (bootstrap/load) maps the error onto its .prev recovery flow.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<data_table_t>>
        load_from_disk(std::pmr::memory_resource* resource,
                       storage::block_manager_t& block_manager,
                       storage::metadata_reader_t& reader);

    private:
        void initialize_scan_with_offset(table_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);

        std::pmr::memory_resource* resource_;
        std::vector<column_definition_t> column_definitions_;
        std::mutex append_lock_;
        std::shared_ptr<collection_t> row_groups_;
        std::atomic<bool> is_root_;
        std::string name_;
    };

} // namespace components::table