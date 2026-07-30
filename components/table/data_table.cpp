#include "data_table.hpp"

#include <algorithm>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <cstdlib>
#include <unordered_set>

#include "row_group.hpp"

namespace components::table {

    data_table_t::data_table_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::vector<column_definition_t> column_definitions,
                               std::string name)
        : resource_(resource)
        , column_definitions_(std::move(column_definitions))
        , is_root_(true)
        , name_(std::move(name)) {
        this->row_groups_ = std::make_shared<collection_t>(resource_, block_manager, copy_types(), 0);
    }

    data_table_t::data_table_t(data_table_t& parent, column_definition_t& new_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }
        column_definitions_.emplace_back(new_column);

        std::lock_guard parent_lock(parent.append_lock_);

        this->row_groups_ = parent.row_groups_->add_column(new_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent, uint64_t removed_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard parent_lock(parent.append_lock_);

        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }

        assert(removed_column < column_definitions_.size());
        column_definitions_.erase(column_definitions_.begin() + static_cast<int64_t>(removed_column));

        uint64_t storage_idx = 0;
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            auto& col = column_definitions_[i];
            col.set_oid(i);
            col.set_storage_oid(storage_idx++);
        }

        this->row_groups_ = parent.row_groups_->remove_column(removed_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent,
                               uint64_t changed_idx,
                               const types::complex_logical_type& target_type,
                               const std::vector<storage_index_t>&)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard lock(append_lock_);
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }

        column_definitions_[changed_idx].type() = target_type;

        parent.is_root_ = false;
    }

    [[nodiscard]] std::pmr::vector<types::complex_logical_type> data_table_t::copy_types() const {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(column_definitions_.size());
        for (auto& it : column_definitions_) {
            types.push_back(it.type());
        }
        return types;
    }

    const std::vector<column_definition_t>& data_table_t::columns() const { return column_definitions_; }

    void data_table_t::stamp_column_identity(vector::data_chunk_t& chunk) const {
        const auto width = std::min<uint64_t>(chunk.column_count(), column_definitions_.size());
        for (uint64_t i = 0; i < width; i++) {
            chunk.set_column_attoid(i, column_definitions_[i].attoid());
            // M3-B5: the NAME is stamped from the same source as the attoid, and it has to be,
            // because a chunk built from copy_types() cannot carry one for every column. A
            // scalar column's name used to ride along inside its type; a STRUCT column's never
            // did — column_definition_t refuses to overwrite a self-naming type, so the type
            // answers with the TYPE's name ("test_struct") and the column's name
            // ("struct_column") had nowhere to be. The definition is the one place that knows
            // both, so it is the one place that says both. (test_table.cpp pins the struct case.)
            chunk.set_column_name(i, column_definitions_[i].name());
        }
    }

    bool data_table_t::stamp_missing_attoid(std::string_view column_name, std::uint32_t attoid) {
        if (attoid == 0) {
            return false;
        }
        for (auto& col : column_definitions_) {
            // Already identified: leave it alone. This is the guard that keeps the
            // recovery path clear of set_attoid's immutability precondition entirely,
            // rather than relying on the two values happening to agree.
            if (col.attoid() != 0) {
                continue;
            }
            if (std::string_view{col.name()} != column_name) {
                continue;
            }
            col.set_attoid(attoid);
            return true;
        }
        return false;
    }

    void data_table_t::adopt_schema(const std::pmr::vector<vector::column_schema_t>& schema) {
        assert(column_definitions_.empty() && "adopt_schema can only be called on schema-less table");
        column_definitions_.reserve(schema.size());
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(schema.size());
        for (const auto& record : schema) {
            // M3-B2/B5: a storage column's name is column identity, so it comes from the
            // incoming chunk's schema record, where it is TOTAL — an unnamed column is named
            // "". Read off the type instead, this asserted on a missing extension and
            // dereferenced null in release for exactly the shape the WAL decoder produces for
            // an unnamed column (data_chunk_binary.cpp), i.e. on the recovery path.
            column_definitions_.emplace_back(std::string{record.name}, record.type);
            types.push_back(record.type);
        }
        row_groups_->adopt_types(std::move(types));
    }

    void data_table_t::overlay_not_null(const std::string& col_name) {
        for (auto& col : column_definitions_) {
            if (col.name() == col_name) {
                col.set_not_null(true);
                return;
            }
        }
    }

    void data_table_t::initialize_scan(table_scan_state& state,
                                       const std::vector<storage_index_t>& column_ids,
                                       const table_filter_t* filter) {
        state.initialize(column_ids, filter);
        row_groups_->initialize_scan(state.table_state, column_ids);
    }

    void data_table_t::initialize_scan_with_offset(table_scan_state& state,
                                                   const std::vector<storage_index_t>& column_ids,
                                                   int64_t start_row,
                                                   int64_t end_row) {
        state.initialize(column_ids);
        row_groups_->initialize_scan_with_offset(state.table_state, column_ids, start_row, end_row);
    }

    uint64_t data_table_t::row_group_size() const { return row_groups_->row_group_size(); }

    std::shared_ptr<collection_t> data_table_t::row_group() const { return row_groups_; }

    uint64_t data_table_t::calculate_size() { return row_groups_->calculate_size(); }

    void data_table_t::cleanup_versions(uint64_t lowest_active_start_time) {
        row_groups_->cleanup_versions(lowest_active_start_time);
    }

    bool data_table_t::compact(uint64_t compact_watermark) {
        auto total = row_groups_->total_rows();
        if (total == 0) {
            return true;
        }

        // MVCC safety gate (all-or-nothing). The rebuild below scans with the
        // txn-less "see all committed" view and re-stamps every surviving row
        // with transaction_data{0,0} — it collapses the version history. That is
        // only correct when EVERY stamp in the table is already visible to all
        // current and future snapshots, i.e. no stamp is above the caller's
        // watermark (transaction_manager_t::compact_watermark()):
        //   * a pending txn id (>= TRANSACTION_ID_START) means an uncommitted or
        //     committed-but-not-yet-storage-stamped write: the scan would drop
        //     the row AND a later positional commit_append would target moved
        //     rows — the mid-update "row vanishes" window;
        //   * a committed id above the watermark means some active snapshot (or
        //     one taken while that commit_id is still in in_flight_commits_)
        //     must NOT see that insert / must STILL see that deleted row.
        // The watermark only goes stale in the safe direction: ids above it stay
        // above any earlier-computed value, so a watermark computed before this
        // call (it rides actor messages) never green-lights an unsafe compact.
        if (row_groups_->has_version_above(compact_watermark)) {
            return false;
        }

        auto types = row_groups_->types();
        auto new_collection = std::make_shared<collection_t>(
            resource_,
            row_groups_->block_manager(),
            std::pmr::vector<types::complex_logical_type>(types.begin(), types.end(), resource_),
            0);

        {
            table_append_state append_state(resource_);
            // compact is best-effort maintenance: an out_of_memory during the rebuild append
            // leaves the original collection untouched and returns false.
            if (new_collection->initialize_append(append_state).has_error()) {
                return false;
            }

            // Scan committed non-deleted rows from old collection
            std::vector<storage_index_t> column_ids;
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                column_ids.emplace_back(i);
            }

            table_scan_state state(resource_);
            initialize_scan_with_offset(state, column_ids, 0, static_cast<int64_t>(total));

            auto scan_types = copy_types();
            vector::data_chunk_t chunk(resource_, scan_types, vector::DEFAULT_VECTOR_CAPACITY);
            while (true) {
                state.table_state.scan(chunk);
                if (chunk.size() == 0) {
                    break;
                }
                if (new_collection->append(chunk, append_state).has_error()) {
                    return false;
                }
                chunk.reset();
            }

            new_collection->finalize_append(append_state, transaction_data{0, 0});
        }
        // scan state and buffer handles destroyed before swapping collection

        // Keep a reference to the outgoing collection so its disk blocks can be reclaimed AFTER the
        // atomic swap. The swap itself (row_groups_ = move(new_collection)) is the MVCC all-or-nothing
        // guard and stays intact; the old collection becomes unreferenced once `old_collection` drops.
        auto old_collection = row_groups_;

        // Swap old collection with compacted one
        row_groups_ = std::move(new_collection);

        // Return the OLD (now-replaced) collection's disk blocks to the block manager's free list so the
        // NEXT compact reuses them instead of bumping total_blocks() unbounded. No-op for in-memory tables
        // (no backing store). The new collection's write-through already allocated FRESH ids (free list was
        // empty / disjoint), so the old ids it frees are not referenced by row_groups_. The persisted free
        // list survives checkpoint, so reclaimed space is durable. mark_as_free under the block manager's
        // allocation lock; no live segment references the freed blocks (the old collection is being torn down).
        //
        // Each mark_as_free MUST be paired with unregister_block(id): returning the id to the free list while a
        // live block_handle for that id lingers in the block manager's blocks_ registry is an ABA hazard -- a
        // later free_block_id()/register_block() that reuses the id would resurrect the STALE handle (pointing at
        // OLD data) instead of creating a fresh one. unregister_block drops only the registry's weak_ptr entry;
        // the old collection's segments still own the block_handle objects (dropped when old_collection releases),
        // and their dtors call unregister_block again -- a harmless no-op erase on an already-removed id.
        if (old_collection) {
            auto& block_manager = old_collection->block_manager();
            if (!block_manager.in_memory()) {
                std::pmr::vector<uint64_t> reclaimable{resource_};
                old_collection->collect_disk_block_ids(reclaimable);
                // collect_disk_block_ids reports one id PER reloadable segment; B2 packs many segments into a
                // single shared block, so the SAME block id appears multiple times. mark_as_free /
                // unregister_block must run ONCE per id (free_list_ is a set so a double mark_as_free is
                // idempotent, but unregister_block twice could race a reused id's fresh handle), so dedupe.
                std::sort(reclaimable.begin(), reclaimable.end());
                reclaimable.erase(std::unique(reclaimable.begin(), reclaimable.end()), reclaimable.end());
                for (uint64_t block_id : reclaimable) {
                    block_manager.mark_as_free(block_id);
                    block_manager.unregister_block(block_id);
                }
            }
        }
        return true;
    }

    data_table_t::column_compaction_t
    data_table_t::compact_dropped_columns(const std::pmr::vector<uint32_t>& dead_attoids, uint64_t compact_watermark) {
        column_compaction_t outcome;
        if (dead_attoids.empty() || column_definitions_.empty()) {
            return outcome;
        }

        // Which storage slots survive, in the order they already sit in. Removing entries
        // from a list preserves the relative order of what is left, which is the whole
        // reason this makes the relation undisplaced: the survivors are the live logical
        // columns in attnum order, so slot i ends up being logical ordinal i again without
        // anything renumbering pg_attribute.
        std::vector<uint64_t> survivors;
        survivors.reserve(column_definitions_.size());
        uint64_t doomed = 0;
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            const auto attoid = column_definitions_[i].attoid();
            // attoid 0 is INVALID_OID: the column has no catalog identity, so no entry in
            // `dead_attoids` can be about it and it is kept. See the header.
            const bool dead = attoid != 0 && std::find(dead_attoids.begin(), dead_attoids.end(), attoid) !=
                                                 dead_attoids.end();
            if (dead) {
                ++doomed;
            } else {
                survivors.push_back(i);
            }
        }
        if (doomed == 0) {
            return outcome;
        }
        if (survivors.empty()) {
            // A zero-column table is not a narrower table, it is a different object: every
            // append and scan path indexes columns, and a chunk with no width addresses
            // nothing. DROP COLUMN already refuses to remove a relation's last column, so
            // this is unreachable from SQL and stays a refusal rather than an assert.
            return outcome;
        }

        // MVCC gate — see compact(), which this rebuild is modelled on.
        if (row_groups_->has_version_above(compact_watermark)) {
            outcome.mvcc_refused = true;
            return outcome;
        }

        std::pmr::vector<types::complex_logical_type> kept_types(resource_);
        kept_types.reserve(survivors.size());
        for (auto idx : survivors) {
            kept_types.push_back(column_definitions_[idx].type());
        }

        auto new_collection = std::make_shared<collection_t>(
            resource_,
            row_groups_->block_manager(),
            std::pmr::vector<types::complex_logical_type>(kept_types.begin(), kept_types.end(), resource_),
            0);

        const auto total = row_groups_->total_rows();
        if (total > 0) {
            table_append_state append_state(resource_);
            // Like compact(), this is best-effort maintenance: an out_of_memory anywhere in
            // the rebuild leaves the ORIGINAL collection and column list untouched (nothing
            // is swapped until the rebuild has completed) and reports nothing removed.
            if (new_collection->initialize_append(append_state).has_error()) {
                return outcome;
            }

            std::vector<storage_index_t> column_ids;
            column_ids.reserve(column_definitions_.size());
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                column_ids.emplace_back(i);
            }

            table_scan_state state(resource_);
            initialize_scan_with_offset(state, column_ids, 0, static_cast<int64_t>(total));

            // The scan writes each column into the output slot its STORAGE index names
            // (row_group_t::templated_scan resolves the destination as
            // storage_index_t::primary_index, not as the loop counter), so it has to be
            // given a full-width chunk. `narrow` then references only the surviving slots —
            // no copy, the same buffers — and is what the append sees.
            auto scan_types = copy_types();
            vector::data_chunk_t chunk(resource_, scan_types, vector::DEFAULT_VECTOR_CAPACITY);
            vector::data_chunk_t narrow(resource_, kept_types, vector::DEFAULT_VECTOR_CAPACITY);
            while (true) {
                state.table_state.scan(chunk);
                if (chunk.size() == 0) {
                    break;
                }
                // Referenced, not copied: `narrow`'s columns ARE the scanned chunk's
                // surviving columns, so its capacity is the scanned chunk's capacity.
                //
                // Written out rather than through data_chunk_t::reference_columns because
                // that helper resets the destination first, and reset() puts the capacity
                // back to DEFAULT_VECTOR_CAPACITY — which the scan has already grown past
                // whenever one call drains more than one vector (validate_chunk_capacity).
                // The identity and name of each column are deliberately not carried over:
                // the append reads neither, and the narrowed table names its columns from
                // column_definitions_ below.
                narrow.set_capacity(chunk.capacity());
                for (std::size_t j = 0; j < survivors.size(); j++) {
                    narrow.data[j].reference(chunk.data[survivors[j]]);
                }
                narrow.set_cardinality(chunk.size());
                if (new_collection->append(narrow, append_state).has_error()) {
                    return outcome;
                }
                chunk.reset();
            }

            new_collection->finalize_append(append_state, transaction_data{0, 0});
        }
        // scan state and buffer handles destroyed before swapping collection

        auto old_collection = row_groups_;

        // The two halves of "what this table is" move together: the row data and the column
        // list that names it. Anything that read one without the other between these two
        // statements would see a table that never existed — which is why they are adjacent
        // and why nothing is co_awaited between them (this runs to completion inside one
        // agent mailbox handler).
        row_groups_ = std::move(new_collection);
        std::vector<column_definition_t> kept;
        kept.reserve(survivors.size());
        for (auto idx : survivors) {
            kept.emplace_back(column_definitions_[idx]);
        }
        column_definitions_ = std::move(kept);
        // storage_oid/oid are POSITIONAL handles into the row-group layer, so they are
        // renumbered onto the new width. attoid is not: it is the catalog identity, it
        // survives the move, and it is what every reader now joins on.
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            column_definitions_[i].set_oid(i);
            column_definitions_[i].set_storage_oid(i);
        }

        // Return the outgoing collection's disk blocks to the free list. Identical in
        // purpose and hazard to the reclaim at the end of compact() — see the ABA note
        // there for why every mark_as_free is paired with unregister_block, and why the
        // ids are deduplicated first.
        if (old_collection) {
            auto& block_manager = old_collection->block_manager();
            if (!block_manager.in_memory()) {
                std::pmr::vector<uint64_t> reclaimable{resource_};
                old_collection->collect_disk_block_ids(reclaimable);
                std::sort(reclaimable.begin(), reclaimable.end());
                reclaimable.erase(std::unique(reclaimable.begin(), reclaimable.end()), reclaimable.end());
                for (uint64_t block_id : reclaimable) {
                    block_manager.mark_as_free(block_id);
                    block_manager.unregister_block(block_id);
                }
            }
        }

        outcome.removed = doomed;
        return outcome;
    }

    void data_table_t::scan(vector::data_chunk_t& result, table_scan_state& state) { state.table_state.scan(result); }

    void data_table_t::scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                                    const std::vector<size_t>* projected_cols,
                                    std::pmr::vector<vector::data_chunk_t>& batches,
                                    table_scan_state& state,
                                    std::pmr::memory_resource* resource) {
        state.table_state.scan_batched(types, projected_cols, batches, resource);
    }

    core::result_wrapper_t<bool> data_table_t::fetch_next_batch(vector::data_chunk_t& result,
                                                                const std::vector<storage_index_t>& column_ids,
                                                                const table_filter_t* filter,
                                                                transaction_data txn,
                                                                int64_t& next_row,
                                                                int64_t max_row,
                                                                bool& drained) {
        if (drained || next_row >= max_row) {
            drained = true;
            return true;
        }
        // Resume by ABSOLUTE row position against the CURRENT (post-checkpoint) segment tree, reading
        // forward one vector at a time until a vector yields rows OR the scan drains. A filter / all-
        // deleted vector produces 0 rows but still advances the position; the agent and source
        // operator treat an empty batch as end-of-scan, so this loop must keep walking past those
        // empty vectors here rather than handing back a premature empty batch (which would stop the
        // scan before reaching a later group that does match). The walk is geometry-agnostic: each
        // seek re-resolves next_row against the live tree and advances only within the resolved
        // group's [start, start+count) bounds, never assuming a fixed row_group_size.
        while (next_row < max_row) {
            // Transient scan state seeked to next_row, capped at max_row. initialize_scan_with_offset
            // binds the filter and pins only ONE batch's segment(s); both release when `state`
            // destructs at the end of each iteration — nothing pinned crosses the mailbox round-trip.
            table_scan_state state(resource_);
            initialize_scan_with_offset(state, column_ids, next_row, max_row);
            state.filter = filter;
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            auto& css = state.table_state;

            // Capture the seeked group's absolute end BEFORE the read so the advance stays within
            // [start, start+count) of the group this seek resolved against the LIVE tree — never
            // assuming a fixed row_group_size.
            const row_group_t* seeked_group = css.row_group;
            const int64_t group_end =
                seeked_group != nullptr
                    ? std::min(seeked_group->start + static_cast<int64_t>(seeked_group->count.load()), max_row)
                    : max_row;

            const bool produced = css.next_batch(result);
            if (css.has_error()) {
                return css.scan_error;
            }

            // Resume position = the absolute row just past the vector(s) next_batch consumed. Since
            // initialize_scan_with_offset stamps vector_index in collection-absolute space (the same
            // convention as the continuous scan), css.vector_index*CAP is the absolute next row — this
            // correctly accounts for empty vectors next_batch skipped WITHIN the group, instead of a
            // blind one-vector step that could re-read or skip rows. Clamp to the group end (next seek
            // moves into the following group) and to max_row (drain).
            const int64_t prev_row = next_row;
            const int64_t scanned_to = static_cast<int64_t>(css.vector_index * vector::DEFAULT_VECTOR_CAPACITY);
            next_row = std::min({scanned_to, group_end, max_row});

            // No segment resolved for this position, or the position did not move forward: the scan
            // has run out of source rows. Drain.
            if (seeked_group == nullptr || next_row <= prev_row) {
                drained = true;
                return true;
            }
            // Produced a batch: hand it back, leaving next_row positioned for the caller's next fetch.
            if (produced) {
                if (next_row >= max_row) {
                    drained = true;
                }
                return true;
            }
            // Empty vector (filtered / all-deleted) but the position advanced: keep walking forward
            // within this call instead of returning an empty (would-be-drained) batch.
        }
        // Reached max_row without producing further rows.
        drained = true;
        return true;
    }

    bool data_table_t::create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        return state.table_state.scan_committed(result, type);
    }

    std::string data_table_t::table_name() const { return name_; }

    void data_table_t::set_table_name(std::string new_name) { name_ = std::move(new_name); }

    void data_table_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state) {
        row_groups_->fetch(result, column_ids, row_identifiers, fetch_count, state);
    }

    std::unique_ptr<constraint_state> data_table_t::initialize_constraint_state(
        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        return std::make_unique<constraint_state>(bound_constraints);
    }

    core::result_wrapper_t<bool> data_table_t::append_lock(table_append_state& state) {
        state.append_lock = std::unique_lock(append_lock_);
        // Concurrent DDL altered the table (no longer root). Report a write_conflict up to the
        // append boundary for a graceful txn abort, instead of aborting: under -fno-exceptions a
        // throw inside an actor-zeta coroutine is silently swallowed.
        if (!is_root_) {
            return core::error_t(core::error_code_t::write_conflict,
                                 std::pmr::string("Transaction conflict: adding entries to a table that has "
                                                  "been altered!",
                                                  resource_));
        }
        state.row_start = static_cast<int64_t>(row_groups_->total_rows());
        state.current_row = state.row_start;
        return true;
    }

    core::result_wrapper_t<bool> data_table_t::initialize_append(table_append_state& state) {
        assert(state.append_lock &&
               "data_table_t::append_lock should be called before data_table_t::initialize_append");
        if (!state.append_lock) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("data_table_t::append_lock must precede initialize_append", resource_));
        }
        return row_groups_->initialize_append(state); // out_of_memory
    }

    core::result_wrapper_t<bool> data_table_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        assert(is_root_);
        return row_groups_->append(chunk, state); // out_of_memory
    }

    void data_table_t::finalize_append(table_append_state& state, transaction_data txn) {
        row_groups_->finalize_append(state, txn);
    }

    void data_table_t::commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) {
        row_groups_->commit_append(commit_id, row_start, count);
    }

    void data_table_t::revert_append(int64_t row_start, uint64_t count) {
        row_groups_->revert_append(row_start, count);
    }

    void data_table_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        row_groups_->commit_all_deletes(txn_id, commit_id);
    }

    void data_table_t::revert_all_deletes(uint64_t txn_id) { row_groups_->revert_all_deletes(txn_id); }

    void data_table_t::scan_table_segment(int64_t row_start,
                                          uint64_t count,
                                          const std::function<void(vector::data_chunk_t& chunk)>& function) {
        if (count == 0) {
            return;
        }
        int64_t end = row_start + static_cast<int64_t>(count);

        std::vector<storage_index_t> column_ids;
        std::pmr::vector<types::complex_logical_type> types(resource_);
        for (uint64_t i = 0; i < this->column_definitions_.size(); i++) {
            auto& col = this->column_definitions_[i];
            column_ids.emplace_back(i);
            types.push_back(col.type());
        }
        vector::data_chunk_t chunk(resource_, types);
        // The chunk is reused across the whole segment walk, so one stamp covers every
        // callback invocation (M3-B4).
        stamp_column_identity(chunk);

        create_index_scan_state state(resource_);

        initialize_scan_with_offset(state, column_ids, row_start, row_start + static_cast<int64_t>(count));
        // vector_index is stamped in collection-absolute space by initialize_scan_with_offset, so
        // vector_index*CAP is already the vector-aligned absolute start row (do NOT re-add row_group
        // start — that would double-count the group origin).
        auto row_start_aligned = static_cast<int64_t>(state.table_state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);

        int64_t current_row = row_start_aligned;
        while (current_row < end) {
            state.table_state.scan_committed(chunk, table_scan_type::COMMITTED_ROWS);
            if (chunk.size() == 0) {
                break;
            }
            int64_t end_row = current_row + static_cast<int64_t>(chunk.size());
            int64_t chunk_start = std::max(current_row, row_start);
            int64_t chunk_end = std::min(end_row, end);
            assert(chunk_start < chunk_end);
            uint64_t chunk_count = static_cast<uint64_t>(chunk_end - chunk_start);
            if (chunk_count != chunk.size()) {
                assert(chunk_count <= chunk.size());
                uint64_t start_in_chunk;
                if (current_row >= row_start) {
                    start_in_chunk = 0;
                } else {
                    start_in_chunk = static_cast<uint64_t>(row_start - current_row);
                }
                vector::indexing_vector_t indexing(resource_, start_in_chunk, chunk_count);
                chunk.slice(indexing, chunk_count);
            }
            function(chunk);
            chunk.reset();
            current_row = end_row;
        }
    }

    void data_table_t::merge_storage(collection_t& data) { row_groups_->merge_storage(data); }

    std::unique_ptr<table_delete_state>
    data_table_t::initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        auto result = std::make_unique<table_delete_state>(resource_);
        if (result->has_delete_constraints) {
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                result->col_ids.emplace_back(column_definitions_[i].storage_oid());
                types.emplace_back(column_definitions_[i].type());
            }
            result->constraint = std::make_unique<constraint_state>(bound_constraints);
        }
        return result;
    }

    uint64_t data_table_t::delete_rows(table_delete_state&,
                                       vector::vector_t& row_identifiers,
                                       uint64_t count,
                                       uint64_t transaction_id) {
        assert(row_identifiers.type().type() == types::logical_type::BIGINT);
        if (count == 0) {
            return 0;
        }

        row_identifiers.flatten(count);
        auto ids = row_identifiers.data<int64_t>();

        uint64_t pos = 0;
        uint64_t delete_count = 0;
        while (pos < count) {
            uint64_t start = pos;
            bool is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
            for (pos++; pos < count; pos++) {
                bool row_is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
                if (row_is_transaction_delete != is_transaction_delete) {
                    break;
                }
            }
            uint64_t current_offset = start;
            uint64_t current_count = pos - start;

            vector::vector_t offset_ids(row_identifiers, current_offset, pos);
            delete_count += row_groups_->delete_rows(*this, ids + current_offset, current_count, transaction_id);
        }
        return delete_count;
    }

    std::unique_ptr<table_update_state>
    data_table_t::initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        auto result = std::make_unique<table_update_state>();
        result->constraint = initialize_constraint_state(bound_constraints);
        return result;
    }

    core::result_wrapper_t<std::pair<int64_t, uint64_t>>
    data_table_t::update(table_update_state&,
                         vector::vector_t& row_ids,
                         // const std::vector<uint64_t>& column_ids,
                         vector::data_chunk_t& data) {
        assert(row_ids.type().to_physical_type() == types::physical_type::INT64);

        uint64_t count = data.size();
        if (count == 0) {
            return std::pair<int64_t, uint64_t>{0, 0};
        }
        vector::vector_t max_row_id_vec(resource_,
                                        types::logical_value_t(resource_, static_cast<int64_t>(MAX_ROW_ID)),
                                        count);
        vector::vector_t row_ids_slice(resource_, types::logical_type::BIGINT, count);
        vector::data_chunk_t updates_slice(resource_, data.types(), count);
        vector::indexing_vector_t sel_local_update(resource_, count);
        vector::indexing_vector_t sel_global_update(resource_, count);

        auto local_count = vector::vector_ops::compare<std::greater_equal<>>(row_ids,
                                                                             max_row_id_vec,
                                                                             count,
                                                                             &sel_local_update,
                                                                             &sel_global_update);
        if (local_count.has_error()) {
            return local_count.convert_error<std::pair<int64_t, uint64_t>>();
        }
        auto update_count = count - local_count.value();
        if (update_count > 0) {
            updates_slice.slice(data, sel_global_update, update_count);
            updates_slice.flatten();
            row_ids_slice.slice(row_ids, sel_global_update, update_count);
            row_ids_slice.flatten(update_count);

            // Which storage column each update column addresses. row_group_t::update reads
            // column_ids[i] as the destination of updates_slice.data[i].
            //
            // The identities come off `data` and not off `updates_slice`: the slice is a fresh
            // chunk built from data.types(), and a type cannot carry an identity. Column i of one
            // IS column i of the other, so the join answers for both.
            //
            // Same hierarchy the append matcher states out loud (agent_disk: identity outranks
            // name outranks position). An UPDATE's chunk is built from the SCAN's, so its columns
            // carry what the scan stamped — and after ALTER TABLE ... DROP COLUMN that chunk is
            // NARROWER than the table, because the tombstoned column left the logical schema while
            // its physical slot stayed. Position would write every column past the hole into its
            // left neighbour.
            const auto incoming_width = static_cast<size_t>(data.column_count());
            std::vector<uint64_t> column_ids(incoming_width, 0);
            std::vector<bool> resolved(incoming_width, false);
            std::vector<bool> claimed(column_definitions_.size(), false);
            // Pass 1 — catalog identity. An attoid is unique per relation, so it needs no
            // disambiguation, and resolving all of one kind before any of the next is what makes
            // "identity outranks name" hold regardless of column order.
            for (size_t i = 0; i < incoming_width; i++) {
                const auto attoid = data.data[i].attoid();
                if (attoid == 0) { // catalog::INVALID_OID — nothing to look up
                    continue;
                }
                for (size_t t = 0; t < column_definitions_.size(); t++) {
                    if (claimed[t] || column_definitions_[t].attoid() != attoid) {
                        continue;
                    }
                    column_ids[i] = t;
                    resolved[i] = true;
                    claimed[t] = true;
                    break;
                }
            }
            // Pass 2 — name. Not a fallback: it is the answer for an input that genuinely has no
            // identity. This overload IS the replay path, and data_chunk_binary carries a column's
            // name but not its attoid (that codec has no version field to widen).
            for (size_t i = 0; i < incoming_width; i++) {
                if (resolved[i]) {
                    continue;
                }
                const auto name = data.data[i].name();
                if (name.empty()) {
                    continue;
                }
                for (size_t t = 0; t < column_definitions_.size(); t++) {
                    if (claimed[t] || std::string_view{column_definitions_[t].name()} != name) {
                        continue;
                    }
                    column_ids[i] = t;
                    resolved[i] = true;
                    claimed[t] = true;
                    break;
                }
            }
            // Pass 3 — position, last and only at equal width, and only for a column neither of
            // the passes above spoke for. A NARROWER chunk cannot be read positionally at all:
            // that is exactly the shape a post-DROP-COLUMN write has, and reading it by position
            // is what would write every column past the hole into its left neighbour.
            const bool positional = incoming_width == column_count();
            for (size_t i = 0; i < incoming_width; i++) {
                if (resolved[i] || !positional || claimed[i]) {
                    continue;
                }
                column_ids[i] = i;
                resolved[i] = true;
                claimed[i] = true;
            }
            for (size_t i = 0; i < incoming_width; i++) {
                if (resolved[i]) {
                    continue;
                }
                // R6: an update column that addresses no storage column is an error. Dropping it
                // silently would report a row updated that was not.
                std::pmr::string message{"data_table_t::update: update column '", resource_};
                const auto unresolved_name = data.data[i].name();
                message.append(unresolved_name.data(), unresolved_name.size());
                message += "' matches no column of this table";
                return core::error_t(core::error_code_t::schema_error, std::move(message));
            }
            auto updated = row_groups_->update(row_ids_slice.data<int64_t>(), column_ids, updates_slice);
            if (updated.has_error()) {
                return updated.convert_error<std::pair<int64_t, uint64_t>>(); // write_conflict / out_of_memory
            }
        }
        // pair = {0, affected-row count}; the caller's update reply reads it.
        return std::pair<int64_t, uint64_t>{0, update_count};
    }

    core::result_wrapper_t<bool> data_table_t::update_column(vector::vector_t& row_ids,
                                                             const std::vector<uint64_t>& column_path,
                                                             vector::data_chunk_t& updates) {
        assert(row_ids.type().type() == types::logical_type::BIGINT);
        assert(updates.column_count() == 1);
        if (updates.size() == 0) {
            return true;
        }

        // Concurrent DDL altered the table (no longer root). Report write_conflict for a graceful
        // txn abort instead of aborting: under -fno-exceptions a throw inside an actor-zeta
        // coroutine is silently swallowed (UB).
        if (!is_root_) {
            return core::error_t(
                core::error_code_t::write_conflict,
                std::pmr::string("Transaction conflict: cannot update a table that has been altered!", resource_));
        }

        updates.flatten();
        row_ids.flatten(updates.size());
        return row_groups_->update_column(row_ids, column_path, updates);
    }

    uint64_t data_table_t::column_count() const { return column_definitions_.size(); }

    std::vector<column_segment_info> data_table_t::get_column_segment_info() {
        return row_groups_->get_column_segment_info();
    }

    core::result_wrapper_t<bool> data_table_t::checkpoint(storage::metadata_writer_t& writer) {
        storage::partial_block_manager_t partial_block_manager(row_groups_->block_manager());

        auto row_group_pointers_res = row_groups_->checkpoint(partial_block_manager);
        if (row_group_pointers_res.has_error()) {
            return row_group_pointers_res.convert_error<bool>(); // out_of_memory
        }
        const auto& row_group_pointers = row_group_pointers_res.value();

        // write table metadata, versioned (see TABLE_META_MAGIC)
        writer.write<uint32_t>(TABLE_META_MAGIC);
        writer.write<uint32_t>(TABLE_META_VERSION);
        writer.write_string(name_);

        // write column definitions
        writer.write<uint32_t>(static_cast<uint32_t>(column_definitions_.size()));
        for (const auto& col : column_definitions_) {
            writer.write_string(col.name());
            writer.write<uint8_t>(static_cast<uint8_t>(col.type().type()));
            writer.write<uint8_t>(col.is_not_null() ? 1 : 0);
            // v1: the column's catalog identity. Without it a reopened storage routes
            // appends by name, which is the whole reason DROP COLUMN cannot move a
            // column's physical position today.
            writer.write<uint32_t>(col.attoid());
        }

        // write row group count and pointers
        writer.write<uint32_t>(static_cast<uint32_t>(row_group_pointers.size()));
        for (const auto& rgp : row_group_pointers) {
            rgp.serialize(writer);
        }

        writer.flush();
        return true;
    }

    core::result_wrapper_t<std::unique_ptr<data_table_t>>
    data_table_t::load_from_disk(std::pmr::memory_resource* resource,
                                 storage::block_manager_t& block_manager,
                                 storage::metadata_reader_t& reader) {
        // First uint32 discriminates the layout: TABLE_META_MAGIC opens a versioned
        // stream, anything else is the byte-length of the table name in a pre-versioning
        // one (a name that long cannot exist, so the two can never be confused).
        const auto head = reader.read<uint32_t>();
        uint32_t stream_version = 0;
        std::string name;
        if (head == TABLE_META_MAGIC) {
            stream_version = reader.read<uint32_t>();
            if (stream_version > TABLE_META_VERSION) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string("data_table_t::load_from_disk: table metadata version is newer than "
                                     "this build understands",
                                     resource));
            }
            name = reader.read_string();
        } else if (head != 0 && !reader.has_error()) {
            name.resize(head);
            reader.read_data(reinterpret_cast<std::byte*>(name.data()), head);
        }

        auto col_count = reader.read<uint32_t>();
        std::vector<column_definition_t> columns;
        columns.reserve(col_count);
        for (uint32_t i = 0; i < col_count; i++) {
            auto col_name = reader.read_string();
            auto logical_type = static_cast<types::logical_type>(reader.read<uint8_t>());
            auto not_null = reader.read<uint8_t>() != 0;
            columns.emplace_back(col_name, types::complex_logical_type{logical_type}, not_null);
            if (stream_version >= 1) {
                const auto attoid = reader.read<uint32_t>();
                // 0 is INVALID_OID: a column that never had an identity stays without
                // one rather than being handed a fabricated zero.
                if (attoid != 0) {
                    columns.back().set_attoid(attoid);
                }
            }
        }

        auto table = std::make_unique<data_table_t>(resource, block_manager, std::move(columns), std::move(name));

        uint64_t total_loaded_rows = 0;
        auto rg_count = reader.read<uint32_t>();
        for (uint32_t i = 0; i < rg_count; i++) {
            auto pointer = storage::row_group_pointer_t::deserialize(reader);

            // create a new row group and populate from disk pointer
            auto* rg = table->row_groups_->append_row_group(static_cast<int64_t>(pointer.row_start));
            if (rg) {
                rg->create_from_pointer(pointer);
                total_loaded_rows += pointer.tuple_count;
            }
        }
        table->row_groups_->set_total_rows(total_loaded_rows);

        // Corrupt-stream check at the load boundary: if any read above ran past the end of the metadata
        // chain, the reader recorded a sticky data_corruption error (reads became no-ops, so `table` may
        // be partially built). Surface it instead of returning a half-loaded table.
        if (reader.has_error()) {
            return core::error_t(reader.error());
        }

        return table;
    }

} // namespace components::table