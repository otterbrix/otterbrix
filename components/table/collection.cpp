#include "collection.hpp"

#include <algorithm>
#include <components/table/storage/block_manager.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <queue>

#include "column_data.hpp"
#include "row_group.hpp"
#include "row_version_manager.hpp"

namespace components::table {

    collection_t::collection_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::pmr::vector<types::complex_logical_type> types,
                               int64_t row_start,
                               uint64_t total_rows,
                               uint64_t row_group_size)
        : resource_(resource)
        , block_manager_(block_manager)
        , append_pbm_(storage::partial_block_manager_t::for_appends(block_manager))
        , row_group_size_(row_group_size)
        , total_rows_(total_rows)
        , types_(std::move(types))
        , row_start_(row_start)
        , allocation_size_(0) {
        row_groups_ = std::make_unique<segment_tree_t<row_group_t>>();
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

    row_group_t* collection_t::append_row_group(int64_t start_row) {
        assert(start_row >= row_start_);
        auto new_row_group = std::make_unique<row_group_t>(this, start_row, 0U);
        new_row_group->initialize_empty(types_);
        row_groups_->append_segment(std::move(new_row_group));
        return row_groups_->last_segment();
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

    void collection_t::initialize_scan_with_offset(collection_scan_state& state,
                                                   const std::vector<storage_index_t>&,
                                                   int64_t start_row,
                                                   int64_t end_row) {
        state.row_groups = row_groups_.get();
        state.max_row = end_row;
        state.initialize(types_);
        auto row_group = row_groups_->get_segment(start_row);
        if (!row_group) {
            // No row group brackets this start row. Reported via scan_error, not
            // thrown: data_table_t::compact and fetch_next_batch both check has_error() first.
            state.scan_error =
                core::error_t{core::error_code_t::data_corruption,
                              std::pmr::string{"collection_t::initialize_scan_with_offset: no row group brackets "
                                               "the requested start row",
                                               resource_}};
            return;
        }
        uint64_t start_vector = static_cast<uint64_t>(start_row - row_group->start) / vector::DEFAULT_VECTOR_CAPACITY;
        if (!row_group->initialize_scan_with_offset(state, start_vector)) {
            // Empty or past the scan ceiling is a legitimate end-of-scan, not a failure:
            // next_batch produces nothing and the fetch loop moves to the next group.
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
        // Only read by the DEV_MODE guard below; incremented unconditionally so it can't
        // drift from the loop it describes.
        [[maybe_unused]] uint64_t stamped = 0;
#ifdef DEV_MODE
        // Guards the whole call, not just row_ids: a request bigger than the chunk's capacity
        // would overrun the columns too.
        assert(fetch_count <= result.capacity() &&
               "collection_t::fetch: the request is larger than the chunk it must fill");
#endif
        for (uint64_t i = 0; i < fetch_count; i++) {
            auto row_id = row_ids[i];
            auto* row_group = row_groups_->get_segment(row_id);
            if (!row_group) {
                // Names no row group: dropped from the answer. Stamps below name only
                // gathered rows, so the drop is visible, not masked.
                continue;
            }
            // Asked before the gather so an invisible row costs no column read. row_id stays
            // collection-absolute; row_version_manager_t::fetch rebases internally.
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
        // Guards the pairing: exactly one stamp per gathered row. Consumers rely on this
        // instead of positional row_ids == request.
        assert(stamped == result.size() && "collection_t::fetch: stamped row_ids disagree with the cardinality");
#endif
    }

    bool collection_t::is_empty() const { return row_groups_->is_empty(); }

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

    core::result_wrapper_t<bool> collection_t::initialize_append(table_append_state& state) {
        // Type validated first: create_column's constructors cannot refuse a type they cannot
        // represent, or an unnamed struct throws inside struct_column_data_t's ctor and hangs the
        // statement across the disk agent's coroutine instead of failing cleanly.
        for (const auto& type : types_) {
            if (auto err = column_data_t::validate_column_type(type, resource_); err.contains_error()) {
                return err;
            }
        }
        state.row_start = static_cast<int64_t>(total_rows_.load());
        state.current_row = state.row_start;
        state.total_append_count = 0;

        if (row_groups_->is_empty()) {
            append_row_group(row_start_);
        }
        state.start_row_group = row_groups_->last_segment();
        assert(row_start_ + static_cast<int64_t>(total_rows_.load()) ==
               state.start_row_group->start + static_cast<int64_t>(state.start_row_group->count));
        state.append_state.pbm = &append_pbm_;
        return state.start_row_group->initialize_append(state.append_state); // out_of_memory
    }

    core::result_wrapper_t<bool> collection_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        const uint64_t prev_row_group_size = row_group_size_;
        assert(chunk.column_count() == types_.size());

        bool new_row_group = false;
        uint64_t total_append_count = chunk.size();
        uint64_t remaining = chunk.size();
        state.total_append_count += total_append_count;
        auto* entry_row_group = state.append_state.row_group;
        const uint64_t entry_offset = state.append_state.offset_in_row_group;
        while (true) {
            auto current_row_group = state.append_state.row_group;
            uint64_t append_count =
                std::min<uint64_t>(remaining, prev_row_group_size - state.append_state.offset_in_row_group);
            if (append_count > 0) {
                auto previous_allocation_size = current_row_group->allocation_size();
                auto appended = current_row_group->append(state.append_state, chunk, append_count);
                allocation_size_ += current_row_group->allocation_size() - previous_allocation_size;
                if (appended.has_error()) {
                    if (current_row_group == entry_row_group) {
                        return appended; // out_of_memory
                    }
                    return unwind_append(state, entry_row_group, entry_offset, total_append_count, appended.error());
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

            auto last_row_group = append_row_group(next_start);
            auto init = last_row_group->initialize_append(state.append_state);
            if (init.has_error()) {
                return unwind_append(state, entry_row_group, entry_offset, total_append_count, init.error());
            }
            // Write-through: the row group we just closed is now complete (segments final, append state
            // moved on), so re-pointing it to disk lets the pool evict+reload it -> bounded memory at any
            // table size. A write/alloc failure surfaces as io_error/out_of_memory, never a throw.
            auto transitioned = current_row_group->transition_to_disk(append_pbm_);
            if (transitioned.has_error()) {
                return unwind_append(state, entry_row_group, entry_offset, total_append_count, transitioned.error());
            }
        }
        // Once per append, for every segment re-pointed above or filled inside a column.
        if (auto flushed = append_pbm_.flush_partial_blocks(); flushed.has_error()) {
            // io_error: the re-pointed segments' blocks are not on disk
            return unwind_append(state, entry_row_group, entry_offset, total_append_count, flushed.error());
        }
        state.current_row += int64_t(total_append_count);
        return new_row_group;
    }

    core::error_t collection_t::unwind_append(table_append_state& state,
                                              row_group_t* entry_row_group,
                                              uint64_t entry_offset,
                                              uint64_t append_count,
                                              const core::error_t& cause) {
        for (uint64_t c = 0; c < types_.size(); c++) {
            state.append_state.states[c].release_pins();
        }
        std::pmr::vector<uint64_t> erased(resource_);
        assert(row_groups_->has_segment(entry_row_group) && "the append started in a row group of this collection");
        const auto& segments = row_groups_->reference_segments();
        for (uint64_t later = entry_row_group->index + 1; later < segments.size(); later++) {
            segments[later]->collect_disk_block_ids(erased);
        }
        row_groups_->erase_segments(entry_row_group->index + 1);
        state.append_state.row_group = entry_row_group;
        state.append_state.offset_in_row_group = entry_offset;
        state.total_append_count -= append_count;
        auto unwound =
            entry_row_group->unwind_append(entry_row_group->start + static_cast<int64_t>(entry_offset), types_.size());
        if (unwound.contains_error()) {
            return unwind_refused(cause, unwound, resource_);
        }
        if (auto settled = settle_unwind(std::move(erased)); settled.contains_error()) {
            return unwind_refused(cause, settled, resource_);
        }
        return cause;
    }

    core::error_t collection_t::settle_unwind(std::pmr::vector<uint64_t> erased_blocks) {
        if (erased_blocks.empty()) {
            if (auto flushed = append_pbm_.flush_partial_blocks(); flushed.has_error()) {
                return flushed.error();
            }
            return core::error_t::no_error();
        }
        // An open tail would take the next append's segments into a freed block.
        if (auto sealed = append_pbm_.seal(); sealed.contains_error()) {
            return sealed;
        }
        std::pmr::vector<uint64_t> live(resource_);
        collect_disk_block_ids(live);
        std::sort(live.begin(), live.end());
        std::sort(erased_blocks.begin(), erased_blocks.end());
        erased_blocks.erase(std::unique(erased_blocks.begin(), erased_blocks.end()), erased_blocks.end());
        for (uint64_t block_id : erased_blocks) {
            if (std::binary_search(live.begin(), live.end(), block_id) || block_manager_.registry_alive(block_id)) {
                continue;
            }
            block_manager_.mark_as_free(block_id);
            block_manager_.unregister_block(block_id);
        }
        return core::error_t::no_error();
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

    uint64_t collection_t::delete_stamp(int64_t row_id) {
        auto* row_group = row_groups_->get_segment(row_id);
        if (!row_group) {
            return NOT_DELETED_ID;
        }
        return row_group->delete_stamp(row_id);
    }

    void release_disk_blocks(storage::block_manager_t& block_manager, std::pmr::vector<uint64_t> block_ids) {
        std::sort(block_ids.begin(), block_ids.end());
        block_ids.erase(std::unique(block_ids.begin(), block_ids.end()), block_ids.end());
        for (uint64_t block_id : block_ids) {
            if (block_id >= block_manager.total_blocks()) {
                block_manager.mark_as_free(block_id);
                continue;
            }
            block_manager.mark_as_free(block_id);
            block_manager.unregister_block(block_id);
        }
    }

    core::result_wrapper_t<bool> collection_t::revert_append(int64_t row_start, uint64_t count) {
        if (row_start + static_cast<int64_t>(count) != row_start_ + static_cast<int64_t>(total_rows_.load())) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string("table revert: the range is not the table's tail", resource_));
        }
        if (count == 0) {
            return true;
        }
        uint64_t segment_index;
        if (!row_groups_->try_segment_index(row_start, segment_index)) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("table revert: no row group brackets the revert row", resource_));
        }
        const auto& segments = row_groups_->reference_segments();
        std::pmr::vector<uint64_t> released{resource_};
        for (uint64_t later = segment_index + 1; later < segments.size(); ++later) {
            segments[later]->collect_disk_block_ids(released);
        }
        release_disk_blocks(block_manager_, std::move(released));
        row_groups_->erase_segments(segment_index + 1);

        auto* row_group = row_groups_->segment_at(static_cast<int64_t>(segment_index));
        total_rows_ = static_cast<uint64_t>(row_start - row_start_);
        return row_group->revert_append(static_cast<uint64_t>(row_start - row_group->start));
    }

    void collection_t::merge_storage(collection_t& data) {
        assert(data.types() == types_);
        auto start_index = row_start_ + static_cast<int64_t>(total_rows_.load());
        auto index = start_index;
        auto segments = data.row_groups_->move_segments();

        for (auto& entry : segments) {
            auto& row_group = entry;
            row_group->move_to_collection(this, index);

            index += static_cast<int64_t>(row_group->count);
            row_groups_->append_segment(std::move(row_group));
        }
        total_rows_ += data.total_rows_.load();
    }

    core::result_wrapper_t<uint64_t>
    collection_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count, uint64_t transaction_id) {
        uint64_t delete_count = 0;
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(ids[start]);
            if (!row_group) {
                // get_segment miss rides the channel this function now returns: a partial delete
                // must not read back as a completed one -- the count alone cannot tell them apart
                // (see the repeat-delete case in services/disk/tests/test_error_handling.cpp).
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string("table delete: a row id names no row group of this table", resource_));
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
        if (auto err = column_data_t::validate_column_type(new_column.type(), resource_); err.contains_error()) {
            return err;
        }
        // Named-resource copy: std::pmr::vector's plain copy ctor asks
        // select_on_container_copy_construction, which for polymorphic_allocator is
        // default-constructed — without this the successor's schema would land on the
        // process-wide default resource instead of this one.
        std::pmr::vector<types::complex_logical_type> new_types(types_, resource_);
        new_types.push_back(new_column.type());
        // The successor shares this collection's row groups and columns; its first checkpoint may
        // name any tail block still open here, so none may be grown after this point.
        if (auto sealed = append_pbm_.seal(); sealed.contains_error()) {
            return sealed; // io_error
        }
        // Plain `new`, never the pmr resource: the intrusive ref count lives inside the
        // object, so `delete` is the matching deallocation (no shared_ptr ever taken here).
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
                // The partially-built successor dies with `result`; the parent is untouched.
                return new_row_group.convert_error<boost::intrusive_ptr<collection_t>>();
            }

            result->row_groups_->append_segment(std::move(new_row_group.value()));
        }
        // The added column's filled segments went into the successor's packer.
        if (auto flushed = result->append_pbm_.flush_partial_blocks(); flushed.has_error()) {
            return flushed.convert_error<boost::intrusive_ptr<collection_t>>(); // io_error
        }
        return result;
    }

    core::result_wrapper_t<boost::intrusive_ptr<collection_t>> collection_t::remove_column(uint64_t col_idx) {
        assert(col_idx < types_.size());
        // Same allocator-extended copy as add_column above.
        std::pmr::vector<types::complex_logical_type> new_types(types_, resource_);
        new_types.erase(new_types.begin() + static_cast<int64_t>(col_idx));

        // Same sharing as add_column: no tail of this collection may be grown once a successor exists.
        if (auto sealed = append_pbm_.seal(); sealed.contains_error()) {
            return sealed; // io_error
        }
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

        // The root written below may name an open tail block; once named it must never be rewritten.
        if (auto sealed = append_pbm_.seal(); sealed.contains_error()) {
            return sealed; // io_error
        }

        auto& segments = row_groups_->reference_segments();
        for (const auto& segment : segments) {
            auto pointer = segment->write_to_disk(partial_block_manager);
            if (pointer.has_error()) {
                return pointer.convert_error<std::vector<storage::row_group_pointer_t>>(); // out_of_memory
            }
            pointers.push_back(std::move(pointer.value()));
        }

        // Every column segment of the checkpoint reaches the file through here, so a
        // dropped answer would let the failure surface two layers later with nothing to
        // attribute it to.
        if (auto flushed = partial_block_manager.flush_partial_blocks(); flushed.has_error()) {
            return flushed.convert_error<std::vector<storage::row_group_pointer_t>>(); // io_error
        }
        return pointers;
    }

} // namespace components::table