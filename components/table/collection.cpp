#include "collection.hpp"

#include <components/table/storage/partial_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <queue>

#include "column_data.hpp"
#include "row_group.hpp"
#include "row_version_manager.hpp"

namespace components::table {

    row_group_segment_tree_t::row_group_segment_tree_t(collection_t& collection)
        : collection_(collection)
        , current_row_group_(0)
        , max_row_group_(0) {}

    collection_t::collection_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::pmr::vector<types::complex_logical_type> types,
                               int64_t row_start,
                               uint64_t total_rows,
                               uint64_t row_group_size)
        : resource_(resource)
        , block_manager_(block_manager)
        , row_group_size_(row_group_size)
        , total_rows_(total_rows)
        , types_(std::move(types))
        , row_start_(row_start)
        , allocation_size_(0) {
        row_groups_ = std::make_unique<row_group_segment_tree_t>(*this);
    }

    uint64_t collection_t::total_rows() const { return total_rows_.load(); }

    uint64_t collection_t::committed_row_count() const {
        uint64_t total = 0;
        for (auto* rg = row_groups_->root_segment(); rg; rg = row_groups_->next_segment(rg)) {
            total += rg->committed_row_count();
        }
        return total;
    }

    bool collection_t::has_version_above(uint64_t watermark) const {
        for (auto* rg = row_groups_->root_segment(); rg; rg = row_groups_->next_segment(rg)) {
            if (rg->has_version_above(watermark)) {
                return true;
            }
        }
        return false;
    }

    const std::pmr::vector<types::complex_logical_type>& collection_t::types() const { return types_; }

    void collection_t::adopt_types(std::pmr::vector<types::complex_logical_type> types) {
        assert(types_.empty() && "adopt_types can only be called on schema-less collection");
        if (!types_.empty()) {
            return;
        }
        types_ = std::move(types);
    }

    void collection_t::append_row_group(std::unique_lock<std::mutex>& l, int64_t start_row) {
        assert(start_row >= row_start_);
        auto new_row_group = std::make_unique<row_group_t>(this, start_row, 0U);
        new_row_group->initialize_empty(types_);
        row_groups_->append_segment(l, std::move(new_row_group));
    }

    row_group_t* collection_t::append_row_group(int64_t start_row) {
        auto l = row_groups_->lock();
        append_row_group(l, start_row);
        return row_groups_->last_segment(l);
    }

    collection_t::~collection_t() = default;

    row_group_t* collection_t::row_group(int64_t index) { return row_groups_->segment_at(index); }

    void collection_t::initialize_scan(collection_scan_state& state, const std::vector<storage_index_t>&) {
        auto row_group = row_groups_->root_segment();
        if (!row_group) {
            return;
        }
        state.row_groups = row_groups_.get();
        state.max_row = row_start_ + static_cast<int64_t>(total_rows_.load());
        state.initialize(types_);
        while (row_group && !row_group->initialize_scan(state)) {
            row_group = row_groups_->next_segment(row_group);
        }
    }

    void collection_t::initialize_create_index_scan(create_index_scan_state& state) {
        state.segment_lock = row_groups_->lock();
    }

    void collection_t::initialize_scan_with_offset(collection_scan_state& state,
                                                   const std::vector<storage_index_t>&,
                                                   int64_t start_row,
                                                   int64_t end_row) {
        state.row_groups = row_groups_.get();
        state.max_row = end_row;
        state.initialize(types_);
        auto row_group = row_groups_->get_segment(start_row);
        if (!row_group) {
            // The seek names a row the segment tree does not bracket. Rule 2: no throw
            // crosses this layer (the caller is a disk agent behind a mailbox); rule 6: it
            // is not a quiet empty scan either. Both live callers — data_table_t::compact
            // and data_table_t::fetch_next_batch — already read has_error() before they
            // trust the batch, and compact refuses the round on it rather than swapping a
            // truncated collection in.
            state.scan_error =
                core::error_t{core::error_code_t::data_corruption,
                              std::pmr::string{"collection_t::initialize_scan_with_offset: no row group brackets "
                                               "the requested start row",
                                               resource_}};
            return;
        }
        uint64_t start_vector = static_cast<uint64_t>(start_row - row_group->start) / vector::DEFAULT_VECTOR_CAPACITY;
        if (!row_group->initialize_scan_with_offset(state, start_vector)) {
            // The resolved group is empty or entirely past the scan ceiling. That is a
            // legitimate end-of-scan, not a failure: the group's max_row_group_row is left
            // at the seek position, so the caller's next_batch produces nothing and the
            // fetch-next loop walks on to the following group / drains.
            return;
        }
    }

    bool collection_t::initialize_scan_in_row_group(collection_scan_state& state,
                                                    collection_t& collection,
                                                    row_group_t& row_group,
                                                    uint64_t vector_index,
                                                    int64_t max_row) {
        state.max_row = max_row;
        state.row_groups = collection.row_groups_.get();
        if (state.column_scans.empty()) {
            state.initialize(collection.types());
        }
        return row_group.initialize_scan_with_offset(state, vector_index);
    }

    void collection_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state,
                             const std::vector<size_t>& projected_cols,
                             const transaction_data& txn,
                             fetch_visibility_t visibility) {
        auto row_ids = row_identifiers.data<int64_t>();
        auto* produced_ids = result.row_ids.data<int64_t>();
        uint64_t count = 0;
        // Read only by the DEV_MODE pairing guard below; the increment stays unconditional so
        // the counter cannot drift from the loop it is meant to describe.
        [[maybe_unused]] uint64_t stamped = 0;
#ifdef DEV_MODE
        // The stamps are written into result.row_ids, which the chunk allocated at its own
        // capacity. A caller asking for more rows than the chunk can hold would overrun it —
        // and would have overrun the columns too, so this guards the whole call, not just
        // the new field.
        assert(fetch_count <= result.capacity() &&
               "collection_t::fetch: the request is larger than the chunk it must fill");
#endif
        for (uint64_t i = 0; i < fetch_count; i++) {
            auto row_id = row_ids[i];
            row_group_t* row_group;
            {
                uint64_t segment_index;
                auto l = row_groups_->lock();
                if (!row_groups_->try_segment_index(l, row_id, segment_index)) {
                    // The id names no row group. It is dropped from the answer rather than
                    // gathered — and because the stamps below name only gathered rows, the
                    // drop is REPORTED, not masked by a request-shaped row_ids vector.
                    continue;
                }
                row_group = row_groups_->segment_at(l, static_cast<int64_t>(segment_index));
            }
            // The visibility question, asked BEFORE the gather so an invisible row costs no
            // column read. `row_id` stays collection-absolute: row_version_manager_t::fetch
            // keeps the absolute contract for this one method and rebases internally.
            if (visibility == fetch_visibility_t::SNAPSHOT && !row_group->is_visible(txn, row_id)) {
                continue;
            }
#ifdef DEV_MODE
            note_gather_row_fetched();
#endif
            row_group->fetch_row(state, column_ids, row_id, result, count, projected_cols);
            produced_ids[count] = row_id;
            stamped++;
            count++;
        }
        result.set_cardinality(count);
#ifdef DEV_MODE
        // One stamp per row carried, no more and no fewer. The guard is on the PAIRING: it
        // is the invariant every consumer of this reply now relies on in place of "the reply
        // is positionally the request", which it no longer is. An edit that sets the
        // cardinality from the request — the shape this code had — trips it here.
        assert(stamped == result.size() && "collection_t::fetch: stamped row_ids disagree with the cardinality");
#endif
    }

    bool collection_t::is_empty() const {
        auto l = row_groups_->lock();
        return is_empty(l);
    }

    uint64_t collection_t::calculate_size() {
        uint64_t res = 0;
        auto row_group = row_groups_->root_segment();
        while (row_group) {
            res += row_group->calculate_size();
            row_group = row_groups_->next_segment(row_group);
        }
        return res;
    }

    void collection_t::cleanup_versions(uint64_t lowest_active_start_time) {
        for (auto& rg : row_groups_->segments()) {
            auto count = rg.count.load();
            if (count > 0) {
                // cleanup_append is safe on get_or_create — only creates lightweight info
                rg.get_or_create_version_info().cleanup_append(lowest_active_start_time, 0, count);
            }
        }
    }

    bool collection_t::is_empty(std::unique_lock<std::mutex>& l) const { return row_groups_->is_empty(l); }

    core::result_wrapper_t<bool> collection_t::initialize_append(table_append_state& state) {
        // Every write reaches a row group's columns through here, and a row group builds them
        // with column_data_t::create_column — whose constructors cannot refuse a type they
        // cannot represent. So the type is judged FIRST, on the channel this function already
        // returns. Before this, an unnamed struct threw inside struct_column_data_t's
        // constructor, across the disk agent's mailbox and into a coroutine with an empty
        // unhandled_exception(): the statement hung instead of failing (rules 2/9).
        for (const auto& type : types_) {
            if (auto err = column_data_t::validate_column_type(type, resource_); err.contains_error()) {
                return err;
            }
        }
        state.row_start = static_cast<int64_t>(total_rows_.load());
        state.current_row = state.row_start;
        state.total_append_count = 0;

        auto l = row_groups_->lock();
        if (is_empty(l)) {
            append_row_group(l, row_start_);
        }
        state.start_row_group = row_groups_->last_segment(l);
        assert(row_start_ + static_cast<int64_t>(total_rows_.load()) ==
               state.start_row_group->start + static_cast<int64_t>(state.start_row_group->count));
        return state.start_row_group->initialize_append(state.append_state); // out_of_memory
    }

    core::result_wrapper_t<bool> collection_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        const uint64_t prev_row_group_size = row_group_size_;
        assert(chunk.column_count() == types_.size());

        bool new_row_group = false;
        uint64_t total_append_count = chunk.size();
        uint64_t remaining = chunk.size();
        state.total_append_count += total_append_count;
        while (true) {
            auto current_row_group = state.append_state.row_group;
            uint64_t append_count =
                std::min<uint64_t>(remaining, prev_row_group_size - state.append_state.offset_in_row_group);
            if (append_count > 0) {
                auto previous_allocation_size = current_row_group->allocation_size();
                auto appended = current_row_group->append(state.append_state, chunk, append_count);
                allocation_size_ += current_row_group->allocation_size() - previous_allocation_size;
                if (appended.has_error()) {
                    return appended; // out_of_memory
                }
            }
            remaining -= append_count;
            if (remaining == 0) {
                break;
            }
            assert(chunk.size() == remaining + append_count);
            if (remaining < chunk.size()) {
                chunk.slice(resource_, append_count, remaining);
            }
            new_row_group = true;
            auto next_start = current_row_group->start + static_cast<int64_t>(state.append_state.offset_in_row_group);

            auto l = row_groups_->lock();
            append_row_group(l, next_start);
            auto last_row_group = row_groups_->last_segment(l);
            auto init = last_row_group->initialize_append(state.append_state);
            if (init.has_error()) {
                return init; // out_of_memory
            }
            // Write-through: the row group we just closed is now COMPLETE (its column segments are final
            // and the append state has moved to the new row group). Re-point its managed segments to disk so the
            // pool can evict+reload them -> bounded memory at any table size. A write/alloc failure
            // surfaces as io_error/out_of_memory, never a throw.
            auto transitioned = current_row_group->transition_to_disk();
            if (transitioned.has_error()) {
                return transitioned;
            }
        }
        state.current_row += int64_t(total_append_count);
        return new_row_group;
    }

    void collection_t::finalize_append(table_append_state& state, transaction_data txn) {
        auto remaining = state.total_append_count;
        auto row_group = state.start_row_group;
        while (remaining > 0) {
            auto append_count = std::min<uint64_t>(remaining, row_group_size_ - row_group->count);
            row_group->append_version_info(txn, append_count);
            remaining -= append_count;
            row_group = row_groups_->next_segment(row_group);
        }
        total_rows_ += state.total_append_count;

        state.total_append_count = 0;
        state.start_row_group = nullptr;
    }

    void collection_t::commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) {
        for (auto& rg : row_groups_->segments()) {
            auto rg_end = rg.start + static_cast<int64_t>(rg.count.load());
            if (rg.start >= row_start + static_cast<int64_t>(count))
                break;
            if (rg_end <= row_start)
                continue;
            auto local_start = static_cast<uint64_t>(std::max(int64_t{0}, row_start - rg.start));
            auto local_end =
                std::min(rg.count.load(), static_cast<uint64_t>(row_start + static_cast<int64_t>(count) - rg.start));
            auto local_count = local_end - local_start;
            rg.commit_append(commit_id, local_start, local_count);
        }
    }

    void collection_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        for (auto& rg : row_groups_->segments()) {
            rg.commit_all_deletes(txn_id, commit_id);
        }
    }

    void collection_t::revert_all_deletes(uint64_t txn_id) {
        for (auto& rg : row_groups_->segments()) {
            rg.revert_all_deletes(txn_id);
        }
    }

    core::result_wrapper_t<bool> collection_t::revert_append(int64_t row_start, uint64_t count) {
        core::error_t first_error = core::error_t::no_error();
        for (auto& rg : row_groups_->segments()) {
            auto rg_end = rg.start + static_cast<int64_t>(rg.count.load());
            if (rg_end <= row_start)
                continue;
            if (rg.start >= row_start + static_cast<int64_t>(count))
                break;
            auto local_start = static_cast<uint64_t>(std::max(int64_t{0}, row_start - rg.start));
            auto reverted = rg.revert_append(local_start);
            if (reverted.has_error() && !first_error.contains_error()) {
                first_error = reverted.error();
            }
        }
        if (total_rows_.load() >= count) {
            total_rows_ -= count;
        } else {
            total_rows_ = 0;
        }
        if (first_error.contains_error()) {
            return first_error;
        }
        return true;
    }

    void collection_t::merge_storage(collection_t& data) {
        assert(data.types() == types_);
        auto start_index = row_start_ + static_cast<int64_t>(total_rows_.load());
        auto index = start_index;
        auto segments = data.row_groups_->move_segments();

        for (auto& entry : segments) {
            auto& row_group = entry.node;
            row_group->move_to_collection(this, index);

            index += static_cast<int64_t>(row_group->count);
            row_groups_->append_segment(std::move(row_group));
        }
        total_rows_ += data.total_rows_.load();
    }

    uint64_t collection_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id) {
        uint64_t delete_count = 0;
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(ids[start]);
            if (!row_group) {
                // get_segment answers a miss with null. This walk has NO error channel (the
                // return is the deleted-row count read by the caller's reply), so the refusal
                // is reported and the walk stops: deleting "some nearby rows" instead is worse
                // than deleting fewer, and the short count is visible to the caller (rule 6).
                std::fprintf(stderr,
                             "components::table::collection_t::delete_rows: row id %lld names no row group; "
                             "stopping after %llu of %llu deletions\n",
                             static_cast<long long>(ids[start]),
                             static_cast<unsigned long long>(delete_count),
                             static_cast<unsigned long long>(count));
                return delete_count;
            }
            for (pos++; pos < count; pos++) {
                assert(ids[pos] >= 0);
                if (ids[pos] < row_group->start) {
                    break;
                }
                if (ids[pos] >= row_group->start + static_cast<int64_t>(row_group->count)) {
                    break;
                }
            }
            delete_count += row_group->delete_rows(table, ids + start, pos - start, transaction_id);
        } while (pos < count);
        return delete_count;
    }

    core::result_wrapper_t<bool>
    collection_t::update(int64_t* ids, const std::vector<uint64_t>& column_ids, vector::data_chunk_t& updates) {
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(ids[pos]);
            if (!row_group) {
                // get_segment answers a miss with null; the refusal rides the channel this
                // function already returns (rules 2/9).
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string("table update: a row id names no row group of this table", resource_));
            }
            int64_t base_id = row_group->start +
                              (ids[pos] - row_group->start) / static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY *
                                                                                   vector::DEFAULT_VECTOR_CAPACITY);
            auto max_id = std::min(base_id + static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY),
                                   row_group->start + static_cast<int64_t>(row_group->count));
            for (pos++; pos < updates.size(); pos++) {
                assert(ids[pos] >= 0);
                if (ids[pos] < base_id) {
                    break;
                }
                if (ids[pos] >= max_id) {
                    break;
                }
            }
            auto updated = row_group->update(updates, ids, start, pos - start, column_ids);
            if (updated.has_error()) {
                return updated; // write_conflict / out_of_memory
            }
        } while (pos < updates.size());
        return true;
    }

    core::result_wrapper_t<bool> collection_t::update_column(vector::vector_t& row_ids,
                                                             const std::vector<uint64_t>& column_path,
                                                             vector::data_chunk_t& updates) {
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(row_ids.data<int64_t>()[pos]);
            if (!row_group) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string("table update: a row id names no row group of this table", resource_));
            }
            int64_t base_id = row_group->start + (row_ids.data<int64_t>()[pos] - row_group->start) /
                                                     static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY *
                                                                          vector::DEFAULT_VECTOR_CAPACITY);
            auto max_id = std::min(base_id + static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY),
                                   row_group->start + static_cast<int64_t>(row_group->count));
            for (pos++; pos < updates.size(); pos++) {
                assert(row_ids.data<int64_t>()[pos] >= 0);
                if (row_ids.data<int64_t>()[pos] < base_id) {
                    break;
                }
                if (row_ids.data<int64_t>()[pos] >= max_id) {
                    break;
                }
            }
            // THE PATH GOES TO update_column, NOT update. row_group_t::update reads its last
            // argument as a list of TOP-LEVEL column ordinals — one per updates column — so a
            // column_path of depth 2 makes it treat the child ordinal as a second table column
            // and index updates.data[1] of a one-column chunk. row_group_t::update_column is the
            // entry that walks the path INTO the column (depth 1 below the root ordinal).
            auto updated = row_group->update_column(updates, row_ids, column_path, start, pos - start);
            if (updated.has_error()) {
                return updated;
            }
        } while (pos < updates.size());
        return true;
    }

    std::vector<column_segment_info> collection_t::get_column_segment_info() {
        std::vector<column_segment_info> result;
        for (auto& row_group : row_groups_->segments()) {
            row_group.get_column_segment_info(row_group.index, result);
        }
        return result;
    }

    void collection_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) {
        for (auto& row_group : row_groups_->segments()) {
            row_group.collect_disk_block_ids(out);
        }
    }

    void collection_t::collect_column_disk_block_ids(uint64_t column_index, std::pmr::vector<uint64_t>& out) {
        for (auto& row_group : row_groups_->segments()) {
            row_group.collect_column_disk_block_ids(column_index, out);
        }
    }

    core::result_wrapper_t<boost::intrusive_ptr<collection_t>>
    collection_t::add_column(column_definition_t& new_column) {
        auto new_types = types_;
        new_types.push_back(new_column.type());
        // Plain `new`, never the pmr resource: the reference count lives inside the collection, so
        // the counter's `delete` is the matching deallocation. Nothing was lost by giving up
        // make_shared's single object+control-block allocation — no weak_ptr, aliasing pointer,
        // custom deleter or shared_from_this is ever taken on a collection.
        auto result = boost::intrusive_ptr<collection_t>(new collection_t(resource_,
                                                                          block_manager_,
                                                                          std::move(new_types),
                                                                          row_start_,
                                                                          total_rows_.load(),
                                                                          row_group_size_));

        vector::vector_t default_vector(resource_, new_column.type());
        for (auto& current_row_group : row_groups_->segments()) {
            auto new_row_group =
                current_row_group.add_column(result.get(), new_column, new_column.default_value_opt(), default_vector);
            if (new_row_group.has_error()) {
                // The partially-built successor dies with `result`; the parent was never
                // touched, so the refusal leaves the table exactly as it was.
                return new_row_group.convert_error<boost::intrusive_ptr<collection_t>>();
            }

            result->row_groups_->append_segment(std::move(new_row_group.value()));
        }
        return result;
    }

    boost::intrusive_ptr<collection_t> collection_t::remove_column(uint64_t col_idx) {
        assert(col_idx < types_.size());
        auto new_types = types_;
        new_types.erase(new_types.begin() + static_cast<int64_t>(col_idx));

        // Same allocation note as add_column above.
        auto result = boost::intrusive_ptr<collection_t>(new collection_t(resource_,
                                                                          block_manager_,
                                                                          std::move(new_types),
                                                                          row_start_,
                                                                          total_rows_.load(),
                                                                          row_group_size_));

        for (auto& current_row_group : row_groups_->segments()) {
            auto new_row_group = current_row_group.remove_column(result.get(), col_idx);
            result->row_groups_->append_segment(std::move(new_row_group));
        }
        return result;
    }

    core::result_wrapper_t<std::vector<storage::row_group_pointer_t>>
    collection_t::checkpoint(storage::partial_block_manager_t& partial_block_manager) {
        std::vector<storage::row_group_pointer_t> pointers;

        auto l = row_groups_->lock();
        auto& segments = row_groups_->reference_segments(l);
        for (const auto& segment : segments) {
            auto pointer = segment.node->write_to_disk(partial_block_manager);
            if (pointer.has_error()) {
                return pointer.convert_error<std::vector<storage::row_group_pointer_t>>(); // out_of_memory
            }
            pointers.push_back(std::move(pointer.value()));
        }

        // Rule 19, and the durability chain: THIS is where every column segment of the checkpoint
        // reaches the file, so its answer must travel. Dropping it still leaves the block
        // manager's durability latch refusing to commit a header over the hole, but the caller
        // is told the row-group pointers are good — so the failure surfaces two layers later,
        // with nothing left to attribute it to.
        if (auto flushed = partial_block_manager.flush_partial_blocks(); flushed.has_error()) {
            return flushed.convert_error<std::vector<storage::row_group_pointer_t>>(); // io_error
        }
        return pointers;
    }

} // namespace components::table