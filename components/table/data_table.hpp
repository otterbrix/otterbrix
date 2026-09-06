#pragma once

#include <utility>

#include "collection.hpp"
#include "storage/metadata_reader.hpp"
#include "storage/metadata_writer.hpp"

namespace components::table {

#ifdef DEV_MODE
    // Test-observable count of rows delivered by data_table_t::scan — the materializing
    // chunk scan behind storage_t::scan. A write-path handler must never pay a read that
    // grows with the table; the scaling test resets this, runs one INSERT batch, and asserts
    // the rows streamed do not grow with the rows already stored.
    uint64_t table_scan_rows_streamed() noexcept;
    void reset_table_scan_rows_streamed() noexcept;
#endif

    class data_table_t {
    public:
        data_table_t(std::pmr::memory_resource* resource,
                     storage::block_manager_t& block_manager,
                     std::vector<column_definition_t> column_definitions,
                     std::string name = "temp");
        // ALTER ADD COLUMN successor. A failed backfill (out_of_memory) cannot be returned
        // from a constructor, so it LATCHES: has_construction_error() answers true, the
        // parent stays root (the DDL did not happen), and this object degrades to a
        // read-only view of the parent WITHOUT the new column whose writes all refuse.
        // The DDL site must read the latch before installing the successor.
        data_table_t(data_table_t& parent, column_definition_t& new_column);
        data_table_t(data_table_t& parent, uint64_t removed_column);
        // THERE IS NO ALTER TYPE SUCCESSOR CONSTRUCTOR: the one that used to stand here left
        // row_groups_ null while demoting its parent out of root -- a null-dereference trap with
        // zero callers, unbuildable until collection_t::alter_type and row_group_t::alter_type
        // (both commented out) exist. test_update_overlay_predicate.cpp asserts it stays undeclared.

        // ALTER-constructor failure latch — see the ADD COLUMN constructor above.
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

        // Point fetch by row id. `txn` + `visibility` are NOT optional and have no default:
        // under SNAPSHOT the collection drops rows the transaction may not see, under RAW it
        // drops nothing (the CREATE INDEX backfill reads deleted rows on purpose). The filled
        // chunk's row_ids name exactly the rows it carries — see collection_t::fetch.
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
        uint64_t
        delete_rows(table_delete_state& state, vector::vector_t& row_ids, uint64_t count, uint64_t transaction_id);

        std::unique_ptr<table_update_state>
        initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints);
        // Returns write_conflict when an ALTER has superseded this table, and
        // out_of_memory / data_corruption / io_error from the storage below. On success the
        // pair is {0, affected-row count}; the caller's update reply carries it.
        //
        // NOT A TRANSACTIONAL UPDATE: no transaction id parameter, because the overlay it writes
        // publishes immediately with no version chain and no undo
        // (components/table/update_segment.hpp; test_storage_update_rollback.cpp measures it).
        // The txn-carrying UPDATE a statement runs is delete-stamp + append instead
        // (table_storage_adapter.hpp). Entitled callers: WAL replay (pre-scheduler) and the
        // pg_attribute commit-id stamp, both via agent_disk_t::direct_update_sync;
        // test_nontransactional_update_contract.cpp pins the semantics.
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
        // Rollback of an appended range. Returns the first refusal from the column
        // truncation walk (out_of_memory / data_corruption). result_wrapper_t is
        // [[nodiscard]] at the CLASS level, so every caller must consume the answer — the
        // storage adapter's void override reports it (it has no channel of its own).
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

        // Rename ONE column IN PLACE — the storage half of ALTER TABLE RENAME COLUMN.
        //
        // No successor table, unlike the ALTER add/drop rebuilds: a name is not part of any segment,
        // row group or block, so nothing below column_definitions_ moves and every holder of this
        // data_table_t stays valid. It IS a change to the file — checkpoint() writes each column's
        // name into the metadata stream — so it marks modified.
        //
        // NOT COSMETIC: bootstrap reconciliation (rearm_dropped_column_blocks_sync) matches by
        // attoid, but the storage name is still a cache the append path's column expansion and
        // drop_column address BY NAME; renaming in place keeps that cache from going stale before
        // the next checkpoint.
        //
        // Three outcomes, kept apart so a real error can't hide inside `false`:
        //   true  = renamed;
        //   false = no column called `old_name` here (a column the catalog knows but storage never
        //           materialized — the ALTER ADD COLUMN case — is legitimately nothing to rename);
        //   error = `new_name` is ALREADY a column of this table.
        [[nodiscard]] core::result_wrapper_t<bool> rename_column(const std::string& old_name,
                                                                 const std::string& new_name);

        uint64_t row_group_size() const;

        // Hands back a COUNTED COPY, by value, and that is load-bearing rather than incidental:
        // the caller's copy may outlive compact(), which replaces row_groups_ with a compacted
        // rebuild and frees the outgoing collection's disk blocks. The stale holder keeps the
        // REPLACED collection — and its block handles — alive until it lets go; see the ownership
        // note on collection_t and the block-registry reasoning in compact().
        boost::intrusive_ptr<collection_t> row_group() const;

        // Append the disk block ids reported by ONE top-level column across every row group. The
        // one caller is table_storage_t::drop_column: the removed-column rebuild simply forgets
        // the dropped column, so this must run BEFORE it or never (the object is gone with the
        // superseded parent). These are CANDIDATES, not proven-exclusive: the checkpoint packs
        // several columns into one block, so proving sole ownership is the release site's job.
        void collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) const;

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

        // Counts every compact() that actually swapped row_groups_ (row ids MAY have been
        // renumbered). An index answer is stamped with the value this read at build time;
        // storage_fetch refuses ids whose stamp no longer matches. Bumped INSIDE compact()
        // so no call site can forget; carried across the ALTER successor constructors, which
        // share the parent's rows and renumber nothing.
        [[nodiscard]] uint64_t compact_epoch() const noexcept { return compact_epoch_; }

        // The checkpoint chain returns out_of_memory when a column flush pin fails;
        // true on success.
        [[nodiscard]] core::result_wrapper_t<bool> checkpoint(storage::metadata_writer_t& writer);

        // Has anything happened to this table since the root on the device was written? False
        // means it's byte-for-byte on the device already (table_storage_t::needs_checkpoint
        // combines this with state living above the table). Lives HERE, not on table_storage_t,
        // so every mutating method below marks it directly instead of a dozen caller-marked
        // sites in agent_disk.cpp that could forget one. Set by every content-changing method
        // (ALTER rebuilds included), NOT by cleanup_versions (version GC doesn't change what a
        // checkpoint serializes). Cleared only by load_from_disk and by
        // table_storage_t::checkpoint once write_header commits.
        [[nodiscard]] bool modified_since_checkpoint() const noexcept { return modified_since_checkpoint_; }
        // Called once the committing header is on the device -- not by data_table_t::checkpoint
        // itself, which has only written the pointer stream and must stay dirty if the round
        // dies before write_header.
        void clear_modified_since_checkpoint() noexcept { modified_since_checkpoint_ = false; }
        // Returns data_corruption when the on-disk metadata chain is truncated/corrupt, instead
        // of throwing on the load path. The caller reports the refusal loudly and leaves the
        // file untouched (there is no external backup to recover from).
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<data_table_t>>
        load_from_disk(std::pmr::memory_resource* resource,
                       storage::block_manager_t& block_manager,
                       storage::metadata_reader_t& reader);

#ifdef DEV_MODE
        // Test-observable IDENTITY of the collection this table OWNS, read off the member rather
        // than through row_group(), so a gate can tell a real handoff from a deep copy (invisible
        // to any scan/count/checksum). Owner count catches a copied POINTER without a counted
        // reference, which keeps address equality while compact() frees the object underneath.
        // Gate: test_collection_ownership.cpp.
        const collection_t* collection_identity() const;
        uint64_t collection_owner_count() const;
#endif

    private:
        // See modified_since_checkpoint(). A plain bool, not an atomic: this member obeys
        // the same single-actor ownership rule as everything else in the class (the proof is on
        // row_groups_ below), so there is no second thread to publish it to.
        void mark_modified() noexcept { modified_since_checkpoint_ = true; }

        void initialize_scan_with_offset(table_scan_state& state,
                                         const std::vector<storage_index_t>& column_ids,
                                         int64_t start_row,
                                         int64_t end_row);

        std::pmr::memory_resource* resource_;
        // See the ADD COLUMN constructor: the only writer; no_error() everywhere else.
        core::error_t construction_error_{core::error_t::no_error()};
        std::vector<column_definition_t> column_definitions_;
        // NO LOCK HERE — deliberate: every table is reachable from exactly ONE disk agent, nothing
        // beneath data_table_t crosses a mailbox, and actor-zeta resumes one actor on at most one
        // thread (the manager-side *_sync paths all run before the schedulers start; the parallel
        // replay loop was removed for racing on storages_, TSan-confirmed). A mutex would hide the
        // ownership rule, not fix a race.
        //
        // Counted, not exclusive: row_group() hands out copies and compact() swaps this pointer
        // while copies are outstanding, so whichever holder dies last frees the collection.
        boost::intrusive_ptr<collection_t> row_groups_;
        // false = superseded by an ALTER successor (the ALTER constructors clear the parent's
        // flag). A superseded parent is destroyed in the very statement that installs its
        // successor, so `false` here should never be observed live -- this is the regression
        // channel, not a live code path.
        std::atomic<bool> is_root_;
        // True when this table holds something the durable root does not. See
        // modified_since_checkpoint(). Starts true: a table that was BUILT has never been
        // written, and only load_from_disk (built out of the file itself) may say otherwise.
        bool modified_since_checkpoint_{true};
        // See compact_epoch(). Starts at 0 every process start, together with every index's
        // built stamp (compaction happens only at runtime, inside checkpoint rounds). Same
        // single-actor ownership as row_groups_, so a plain integer.
        uint64_t compact_epoch_{0};
        std::string name_;
    };

} // namespace components::table