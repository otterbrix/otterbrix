#include "data_table.hpp"

#include <algorithm>
#include <atomic>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/types/type_spec_codec.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <cstdlib>
#include <unordered_set>

#include "row_group.hpp"

namespace components::table {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_table_scan_rows_streamed{0};
    } // namespace
    uint64_t table_scan_rows_streamed() noexcept { return g_table_scan_rows_streamed.load(std::memory_order_relaxed); }
    void reset_table_scan_rows_streamed() noexcept {
        g_table_scan_rows_streamed.store(0, std::memory_order_relaxed);
    }
#endif

    namespace {
        // A7.3. The DISK block ids one persisted column-pointer tree names: its segments'
        // blocks plus, for a STRING segment, the separate blocks its big-string payloads were
        // moved into. Recursive, because a nested column keeps its payload in child nodes
        // (validity is always children[0]) and a flat walk would silently miss every one of
        // them.
        //
        // This is deliberately the ONLY place the engine turns a row_group_pointer_t into block
        // ids, and both callers of it are the two ends of the same stream: the checkpoint that
        // WRITES the pointers, and load_from_disk which READS them back. The set therefore
        // cannot drift from what a reload would actually address -- which is exactly what
        // column_data_t::initialize_column registers, one block_handle_t per data_pointer_t
        // plus the overflow blocks handed to the segment.
        void collect_pointer_blocks(const storage::column_data_pointers_t& node,
                                    std::pmr::vector<uint64_t>& out) {
            for (const auto& segment : node.segments) {
                out.push_back(segment.block_pointer.block_id);
                for (uint64_t overflow : segment.overflow_blocks) {
                    out.push_back(overflow);
                }
            }
            for (const auto& child : node.children) {
                collect_pointer_blocks(child, out);
            }
        }

        void collect_root_blocks(const std::vector<storage::row_group_pointer_t>& row_groups,
                                 std::pmr::vector<uint64_t>& out) {
            for (const auto& rgp : row_groups) {
                for (const auto& column : rgp.data_pointers) {
                    collect_pointer_blocks(column, out);
                }
                // The delete bitmaps are part of the root too: a block that holds one is as
                // reachable as a data block, and reclaiming it would drop committed deletes.
                for (const auto& deletes : rgp.deletes_pointers) {
                    out.push_back(deletes.block_pointer.block_id);
                    for (uint64_t overflow : deletes.overflow_blocks) {
                        out.push_back(overflow);
                    }
                }
            }
            // One physical block backs many segments (B2 packs a whole row group into a
            // handful of 256 KiB blocks), so the raw walk repeats ids heavily. Dedup: the
            // consumers are set-shaped and a repeated id would only cost work.
            std::sort(out.begin(), out.end());
            out.erase(std::unique(out.begin(), out.end()), out.end());
        }
    } // namespace

    data_table_t::data_table_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::vector<column_definition_t> column_definitions,
                               std::string name)
        : resource_(resource)
        , column_definitions_(std::move(column_definitions))
        , is_root_(true)
        , name_(std::move(name)) {
        // Plain `new`, never the pmr resource: the reference count lives inside the collection, so
        // the counter's `delete` is the matching deallocation. Nothing was lost by giving up
        // make_shared's single object+control-block allocation — no weak_ptr, aliasing pointer,
        // custom deleter or shared_from_this is ever taken on a collection.
        this->row_groups_ =
            boost::intrusive_ptr<collection_t>(new collection_t(resource_, block_manager, copy_types(), 0));
    }

    data_table_t::data_table_t(data_table_t& parent, column_definition_t& new_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def);
        }
        column_definitions_.emplace_back(new_column);

        this->row_groups_ = parent.row_groups_->add_column(new_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent, uint64_t removed_column)
        : resource_(parent.resource_)
        , is_root_(true) {
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

    void data_table_t::adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) {
        assert(column_definitions_.empty() && "adopt_schema can only be called on schema-less table");
        column_definitions_.reserve(types.size());
        for (const auto& type : types) {
            column_definitions_.emplace_back(type.alias(), type);
        }
        row_groups_->adopt_types(std::pmr::vector<types::complex_logical_type>(types, resource_));
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

    // A COUNTED copy: the caller's reference keeps this collection alive on its own, so it stays
    // valid — and keeps its block handles registered — even after compact() swaps a different one
    // in. See the note on the declaration.
    boost::intrusive_ptr<collection_t> data_table_t::row_group() const { return row_groups_; }

    uint64_t data_table_t::calculate_size() { return row_groups_->calculate_size(); }

    void data_table_t::cleanup_versions(uint64_t lowest_active_start_time) {
        row_groups_->cleanup_versions(lowest_active_start_time);
    }

    bool data_table_t::compact(uint64_t compact_watermark) {
        // Compacting a SUPERSEDED ALTER PARENT would free blocks its successor still
        // references (the ALTER constructors share the parent's column_data_t objects),
        // returning as silent wrong data after restart. Proven unreachable, twice over:
        //   * ownership — the parent's sole owner is table_storage_t::table_, and
        //     table_storage_t::add_column / drop_column destroy the parent in the very
        //     move-assign that installs the successor, inside one synchronous call on the
        //     owning agent's mailbox (the bootstrap *_sync twins run before the schedulers
        //     start). Every production compact site resolves its target through that same
        //     registry at call time (agent_disk checkpoint_inner / vacuum_inner /
        //     maybe_cleanup_inner, none of which suspends mid-body), so a superseded
        //     parent no longer exists by the time any compact can run;
        //   * instrumentation — a probe on this exact predicate (!is_root_ here) stayed
        //     silent across the full unit, service and integration suites, ALTER +
        //     checkpoint/vacuum/commit-fan-out paths included.
        // The assert is the regression tripwire for that ownership rule, same as append's.
        assert(is_root_);
        auto total = row_groups_->total_rows();
        if (total == 0) {
            return true;
        }

        // A DEGRADED block manager must not be rebuilt on top of.
        //
        // Both of the manager's latches (a write/fsync that did not reach the device, a free
        // list proven corrupt) are sticky BY DESIGN, and both make write_header return before
        // it promotes pending_free_. So after ONE transient EIO/ENOSPC the pool free_block_id
        // draws from never refills: this rebuild would allocate a whole fresh copy of the
        // table by extending the file, the checkpoint after it would refuse to commit, and the
        // next round would do it again — the table grows by its own full size every round, for
        // the life of the process. Measured: +19 blocks per round on a 12k-row table after a
        // single failed fsync.
        //
        // Refusing HERE, rather than only at the checkpoint, is the point: the growth is the
        // rebuild's, not the header's. `false` is the channel this function already has for
        // "not this round" (the MVCC gate uses it), and every caller already handles it by
        // deferring the entry and keeping its WAL records — which is exactly right, because a
        // degraded file must not have anything sealed away from it. Loud, not fatal (rule 6):
        // the table keeps serving reads and writes, and every checkpoint keeps reporting the
        // latched error until the file is rebuilt.
        if (row_groups_->block_manager().degraded()) {
            return false;
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
        auto new_collection = boost::intrusive_ptr<collection_t>(
            new collection_t(resource_,
                             row_groups_->block_manager(),
                             std::pmr::vector<types::complex_logical_type>(types.begin(), types.end(), resource_),
                             0));

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
                // A scan failure must NOT look like the end of the table. collection_scan_state
                // ::scan gives up on error AFTER templated_scan has already folded earlier
                // vectors into `chunk`, so the next round hands back an empty one — which this
                // loop used to read as "drained". It then swapped the TRUNCATED collection in
                // and mark_as_free'd every block of the old one: the rows were gone and their
                // blocks recycled, with compact still reporting success. Reachable on an
                // UNCORRUPTED database, because a buffer-pool OOM in initialize_scan lands in
                // this same channel. Refuse the round instead; the caller treats false as
                // "not compacted this time" and leaves the collection untouched.
                if (state.table_state.has_error()) {
                    return false;
                }
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
        // the old collection's segments still own the block_handle objects.
        //
        // ITEM C corrects what this comment used to claim about those objects' destructors. It said their later
        // unregister_block was "a harmless no-op erase on an already-removed id" -- true only while nothing
        // re-registered the id in between, and A7.2/A7.3 made re-registration the NORMAL case: the ids released
        // here land in pending_free_, a committed header promotes them to reusable_, and the next round hands one
        // back out and register_block()s a FRESH handle for it. A holder that outlives this swap (row_group()
        // returns COUNTED collection copies BY VALUE, so the outgoing collection is destroyed when the LAST
        // holder lets go, not at the swap) then destroyed the stale handle AFTER that, and the id-only erase
        // took the LIVE handle's slot with it -- turning registry_alive(id) false while a live segment was still
        // reading the block, which is exactly the subtraction reclaim_superseded_root relies on. The handle
        // destructor's erase is now identity-checked (block_manager_t::unregister_block(block_handle_t&)), so it
        // really is a no-op for a slot that belongs to someone else. The by-ID erase below is the deliberate one
        // and stays: it is what makes the reuse safe in the first place.
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
                    // Same domain guard reclaim_superseded_root applies, and for the same
                    // reason: these ids are DISK-FED. collect_disk_block_ids emits
                    // state->additional_blocks() unfiltered, and those come from
                    // data_pointer_t::overflow_blocks, read off the file as a raw uint64 with
                    // no check. mark_as_free screens its OWN input and returns — it does not
                    // stop the next statement — so without this `continue` a corrupt id in the
                    // transient domain reaches unregister_block's assert: an abort on the agent
                    // thread inside the checkpoint coroutine (rule 9) in a debug build, and
                    // silence under NDEBUG. mark_as_free has already latched the corruption,
                    // which is what stops the next write_header from committing.
                    if (block_id >= storage::MAXIMUM_BLOCK) {
                        block_manager.mark_as_free(block_id);
                        continue;
                    }
                    block_manager.mark_as_free(block_id);
                    block_manager.unregister_block(block_id);
                }
            }
        }
        return true;
    }

    void data_table_t::scan(vector::data_chunk_t& result, table_scan_state& state) {
        state.table_state.scan(result);
#ifdef DEV_MODE
        g_table_scan_rows_streamed.fetch_add(result.size(), std::memory_order_relaxed);
#endif
    }

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
                             column_fetch_state& state,
                             const std::vector<size_t>& projected_cols) {
        row_groups_->fetch(result, column_ids, row_identifiers, fetch_count, state, projected_cols);
    }

    std::unique_ptr<constraint_state> data_table_t::initialize_constraint_state(
        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        return std::make_unique<constraint_state>(bound_constraints);
    }

    core::result_wrapper_t<bool> data_table_t::append_lock(table_append_state& state) {
        state.append_locked = true;
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
        assert(state.append_locked &&
               "data_table_t::append_lock should be called before data_table_t::initialize_append");
        if (!state.append_locked) {
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

        auto update_count = count - vector::vector_ops::compare<std::greater_equal<>>(row_ids,
                                                                                      max_row_id_vec,
                                                                                      count,
                                                                                      &sel_local_update,
                                                                                      &sel_global_update);
        if (update_count > 0) {
            updates_slice.slice(data, sel_global_update, update_count);
            updates_slice.flatten();
            row_ids_slice.slice(row_ids, sel_global_update, update_count);
            row_ids_slice.flatten(update_count);

            // For now ids are fixed
            std::vector<uint64_t> column_ids;
            column_ids.reserve(column_count());
            for (size_t i = 0; i < column_count(); i++) {
                column_ids.emplace_back(i);
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

        // write table metadata
        writer.write_string(name_);

        // write column definitions. The type is the FULL spec (types::encode_type_spec),
        // not a bare logical_type byte: a one-byte tag loses DECIMAL width/scale and every
        // nested child type, which made the reload rebuild a different (UB-adjacent) type.
        writer.write<uint32_t>(static_cast<uint32_t>(column_definitions_.size()));
        std::pmr::vector<std::byte> type_spec(resource_);
        for (const auto& col : column_definitions_) {
            writer.write_string(col.name());
            type_spec.clear();
            auto encoded = types::encode_type_spec(col.type(), type_spec);
            if (encoded.has_error()) {
                return encoded; // schema_error: this column type cannot be persisted
            }
            writer.write<uint32_t>(static_cast<uint32_t>(type_spec.size()));
            writer.write_data(type_spec.data(), type_spec.size());
            writer.write<uint8_t>(col.is_not_null() ? 1 : 0);
        }

        // write row group count and pointers
        writer.write<uint32_t>(static_cast<uint32_t>(row_group_pointers.size()));
        for (const auto& rgp : row_group_pointers) {
            rgp.serialize(writer);
        }

        // The flush is what actually puts this table's metadata on the device; returning true
        // without looking at it would report a checkpointed table whose description never
        // landed.
        if (auto flush_r = writer.flush(); flush_r.has_error()) {
            return flush_r;
        }

        // A7.3. Every block of the root under construction is now allocated and written, so
        // this is the earliest point at which the SUPERSEDED root can be taken down -- and it
        // has to be the LATEST one too: table_storage_t::checkpoint serializes the free list
        // immediately after this call, and that list is the new root's own statement about what
        // it does not reference. Reclaiming after it would publish a root that still claims
        // blocks nothing reads; reclaiming before the pointers exist would have nothing to
        // subtract against.
        //
        // The ids go to pending_free_, never to reusable_ (that is reclaim_superseded_root's
        // job, composing with A7.2): until write_header commits, the root a crash recovers is
        // still root N and still reads every one of them.
        //
        // No-op for an in-memory table (base-class default) and for the first checkpoint of a
        // fresh file (no durable root yet).
        std::pmr::vector<uint64_t> new_root_blocks(resource_);
        collect_root_blocks(row_group_pointers, new_root_blocks);
        auto reclaimed = row_groups_->block_manager().reclaim_superseded_root(new_root_blocks);
        if (reclaimed.has_error()) {
            // Root N's chains could not be walked. That is corrupt input, not a hiccup: the
            // checkpoint must not commit a root on top of accounting it cannot close, and the
            // caller already knows how to defer a failed round.
            return reclaimed.convert_error<bool>();
        }
        return true;
    }

    core::result_wrapper_t<std::unique_ptr<data_table_t>>
    data_table_t::load_from_disk(std::pmr::memory_resource* resource,
                                 storage::block_manager_t& block_manager,
                                 storage::metadata_reader_t& reader) {
        auto name = reader.read_string();

        auto col_count = reader.read<uint32_t>();
        std::vector<column_definition_t> columns;
        columns.reserve(col_count);
        std::pmr::vector<std::byte> type_spec(resource);
        for (uint32_t i = 0; i < col_count; i++) {
            auto col_name = reader.read_string();
            auto spec_size = reader.read<uint32_t>();
            if (reader.has_error()) {
                return core::error_t(reader.error()); // bail before sizing a buffer off garbage
            }
            type_spec.resize(spec_size);
            reader.read_data(type_spec.data(), spec_size);
            auto not_null = reader.read<uint8_t>() != 0;
            if (reader.has_error()) {
                return core::error_t(reader.error());
            }
            // Full type spec decode — restores DECIMAL width/scale, nested child types and
            // aliases exactly as checkpointed. No set_alias here: the alias (when any) is
            // part of the spec, and set_alias on a bare DECIMAL would fabricate a GENERIC
            // extension that to_physical_type() later misreads as a decimal extension (UB).
            auto col_type = types::decode_type_spec(resource, type_spec.data(), type_spec.size());
            if (col_type.has_error()) {
                return col_type.convert_error<std::unique_ptr<data_table_t>>(); // data_corruption
            }
            columns.emplace_back(std::move(col_name), std::move(col_type.value()), not_null);
        }

        auto table = std::make_unique<data_table_t>(resource, block_manager, std::move(columns), std::move(name));

        uint64_t total_loaded_rows = 0;
        auto rg_count = reader.read<uint32_t>();
        // A7.3: the LOADER defines what the durable root references. Collected here, out of the
        // very pointer stream the table is being built from, so the block manager's idea of
        // "root N's data blocks" is the loader's own answer and cannot drift from it.
        std::pmr::vector<uint64_t> durable_blocks(resource);
        std::vector<storage::row_group_pointer_t> loaded_pointers;
        loaded_pointers.reserve(rg_count);
        for (uint32_t i = 0; i < rg_count; i++) {
            auto pointer = storage::row_group_pointer_t::deserialize(reader);

            // create a new row group and populate from disk pointer
            auto* rg = table->row_groups_->append_row_group(static_cast<int64_t>(pointer.row_start));
            if (rg) {
                auto created = rg->create_from_pointer(pointer);
                if (created.has_error()) {
                    return created.convert_error<std::unique_ptr<data_table_t>>(); // data_corruption
                }
                total_loaded_rows += pointer.tuple_count;
            }
            loaded_pointers.push_back(std::move(pointer));
        }
        table->row_groups_->set_total_rows(total_loaded_rows);

        // Corrupt-stream check at the load boundary: if any read above ran past the end of the metadata
        // chain, the reader recorded a sticky data_corruption error (reads became no-ops, so `table` may
        // be partially built). Surface it instead of returning a half-loaded table.
        if (reader.has_error()) {
            return core::error_t(reader.error());
        }

        // Only now, with the stream proven whole: a half-read pointer list would hand the block
        // manager a bogus "this is what root N owns" and A7.3 would reclaim live blocks off it.
        collect_root_blocks(loaded_pointers, durable_blocks);
        block_manager.adopt_durable_root_data_blocks(durable_blocks);

        return table;
    }

#ifdef DEV_MODE
    const collection_t* data_table_t::collection_identity() const {
        // The OWNING side, read off the member: row_group() must hand back exactly THIS object,
        // and after compact() a holder taken before the swap must still name the OLD one while
        // this answers with the new.
        return row_groups_.get();
    }

    uint64_t data_table_t::collection_owner_count() const {
        // A live collection is owned by at least the table, so 0 can only mean "no object".
        return row_groups_ ? static_cast<uint64_t>(row_groups_->use_count()) : 0;
    }
#endif

} // namespace components::table