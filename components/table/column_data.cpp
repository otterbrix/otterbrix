#include "column_data.hpp"

#include <cstring>

#include <components/types/types.hpp>
#include <components/vector/validation.hpp>

#include "array_column_data.hpp"
#include "column_checkpoint_state.hpp"
#include "column_data_checkpointer.hpp"
#include "column_state.hpp"
#include "list_column_data.hpp"
#include "persistent_column_data.hpp"
#include "row_group.hpp"
#include "standard_column_data.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_handle.hpp"
#include "storage/buffer_manager.hpp"
#include "storage/partial_block_manager.hpp"
#include "struct_column_data.hpp"
#include "validity_column_data.hpp"

namespace components::table {
    column_data_t::column_data_t(std::pmr::memory_resource* resource,
                                 storage::block_manager_t& block_manager,
                                 uint64_t column_index,
                                 int64_t start_row,
                                 types::complex_logical_type type,
                                 column_data_t* parent)
        : start_(start_row)
        , count_(0)
        , block_manager_(block_manager)
        , column_index_(column_index)
        , type_(std::move(type))
        , parent_(std::move(parent))
        , allocation_size_(0)
        , statistics_(resource, type_.type())
        , resource_(resource) {}

    filter_propagate_result_t column_data_t::check_zonemap(column_scan_state&, table_filter_t& filter) {
        if (!statistics_.has_stats()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        if (statistics_.min_value().is_null() || statistics_.max_value().is_null()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        // Stats may be stale after updates — skip pruning if column has updates
        if (has_updates()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        // set_membership_filter_t has multiple constants; zonemap pruning would need
        // to compute min(values), max(values) and intersect with the segment range. Until
        // Future wiring will add zonemap pruning; fall through to per-row dispatch (NO_PRUNING_POSSIBLE).
        if (dynamic_cast<const set_membership_filter_t*>(&filter)) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        if (filter.filter_type == expressions::compare_type::eq ||
            filter.filter_type == expressions::compare_type::gt ||
            filter.filter_type == expressions::compare_type::gte ||
            filter.filter_type == expressions::compare_type::lt ||
            filter.filter_type == expressions::compare_type::lte) {
            auto& constant_filter = filter.cast<constant_filter_t>();
            const auto& constant = constant_filter.constant;
            const auto& min = statistics_.min_value();
            const auto& max = statistics_.max_value();
            switch (filter.filter_type) {
                case expressions::compare_type::eq:
                    // eq is impossible if constant < min or constant > max
                    if (constant < min || constant > max) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    break;
                case expressions::compare_type::gt:
                    // value > constant: impossible if max <= constant
                    if (max <= constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    // always true if min > constant
                    if (min > constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::gte:
                    // value >= constant: impossible if max < constant
                    if (max < constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (min >= constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::lt:
                    // value < constant: impossible if min >= constant
                    if (min >= constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (max < constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::lte:
                    // value <= constant: impossible if min > constant
                    if (min > constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (max <= constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                default:
                    break;
            }
        }
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    filter_propagate_result_t column_data_t::check_segment_zonemap(column_scan_state& state, table_filter_t& filter) {
        if (!state.current || !state.current->segment_statistics().has_stats()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        auto& seg_stats = state.current->segment_statistics();
        if (!seg_stats.has_stats() || seg_stats.min_value().is_null() || seg_stats.max_value().is_null()) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }
        // See check_zonemap above — set_membership_filter_t needs min(values)/max(values)
        // intersection logic, deferred to M4. Until then, no pruning for IN-list filters.
        if (dynamic_cast<const set_membership_filter_t*>(&filter)) {
            return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
        }

        if (filter.filter_type == expressions::compare_type::eq ||
            filter.filter_type == expressions::compare_type::gt ||
            filter.filter_type == expressions::compare_type::gte ||
            filter.filter_type == expressions::compare_type::lt ||
            filter.filter_type == expressions::compare_type::lte) {
            auto& constant_filter = filter.cast<constant_filter_t>();
            const auto& constant = constant_filter.constant;
            const auto& min = seg_stats.min_value();
            const auto& max = seg_stats.max_value();
            switch (filter.filter_type) {
                case expressions::compare_type::eq:
                    if (constant < min || constant > max) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    break;
                case expressions::compare_type::gt:
                    if (max <= constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (min > constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::gte:
                    if (max < constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (min >= constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::lt:
                    if (min >= constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (max < constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                case expressions::compare_type::lte:
                    if (min > constant) {
                        return filter_propagate_result_t::ALWAYS_FALSE;
                    }
                    if (max <= constant) {
                        return filter_propagate_result_t::ALWAYS_TRUE;
                    }
                    break;
                default:
                    break;
            }
        }
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    uint64_t column_data_t::max_entry() { return count_; }

    void column_data_t::set_start(int64_t new_start) {
        start_ = new_start;
        uint64_t offset = 0;
        for (auto& segment : data_.segments()) {
            segment.start = start_ + static_cast<int64_t>(offset);
            offset += segment.count;
        }
        data_.reinitialize();
    }

    const types::complex_logical_type& column_data_t::root_type() const {
        if (parent_) {
            return parent_->root_type();
        }
        return type_;
    }

    bool column_data_t::has_updates() const {
        std::lock_guard update_guard(update_lock_);
        return updates_.get();
    }

    scan_vector_type
    column_data_t::get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) {
        if (result.get_vector_type() != vector::vector_type::FLAT) {
            return scan_vector_type::SCAN_ENTIRE_VECTOR;
        }
        if (has_updates()) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        uint64_t remaining_in_segment =
            static_cast<uint64_t>(state.current->start) + state.current->count - static_cast<uint64_t>(state.row_index);
        if (remaining_in_segment < scan_count) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        return scan_vector_type::SCAN_FLAT_VECTOR;
    }

    void column_data_t::initialize_scan(column_scan_state& state) {
        state.current = data_.root_segment();
        state.row_index = state.current ? state.current->start : 0;
        state.internal_index = state.row_index;
        state.initialized = false;
        state.scan_state.reset();
    }

    void column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
        state.current = data_.get_segment(row_idx);
        state.row_index = row_idx;
        state.internal_index = state.current->start;
        state.initialized = false;
        state.scan_state.reset();
        state.last_offset = 0;
    }

    uint64_t column_data_t::scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result) {
        auto target_count = vector_count(vector_index);
        return scan(vector_index, state, result, target_count);
    }

    uint64_t column_data_t::scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           bool allow_updates) {
        auto target_count = vector_count(vector_index);
        return scan_committed(vector_index, state, result, allow_updates, target_count);
    }

    uint64_t column_data_t::scan(uint64_t vector_index,
                                 column_scan_state& state,
                                 vector::vector_t& result,
                                 uint64_t scan_count) {
        return scan_vector<false, true>(vector_index, state, result, scan_count);
    }

    uint64_t column_data_t::scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           bool allow_updates,
                                           uint64_t scan_count) {
        if (allow_updates) {
            return scan_vector<true, true>(vector_index, state, result, scan_count);
        } else {
            return scan_vector<true, false>(vector_index, state, result, scan_count);
        }
    }

    void column_data_t::scan_committed_range(uint64_t row_group_start,
                                             uint64_t offset_in_row_group,
                                             uint64_t count,
                                             vector::vector_t& result) {
        column_scan_state child_state;
        initialize_scan_with_offset(child_state, static_cast<int64_t>(row_group_start + offset_in_row_group));
        auto scan_count = scan_vector(child_state, result, count, scan_vector_type::SCAN_FLAT_VECTOR);
        if (has_updates()) {
            assert(result.get_vector_type() == vector::vector_type::FLAT);
            result.flatten(scan_count);
            updates_->fetch_committed_range(static_cast<int64_t>(offset_in_row_group), count, result);
        }
    }

    uint64_t column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        assert(!has_updates());
        return scan_vector(state, result, count, scan_vector_type::SCAN_FLAT_VECTOR);
    }

    uint64_t
    column_data_t::scan_count_with_updates(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        // Capture the write base and the column-relative range start before scanning:
        // scan_vector writes flat values starting at state.result_offset and advances
        // state.row_index. The committed-update overlay is then applied over the same
        // span at the same positions.
        const uint64_t result_offset = state.result_offset;
        const int64_t range_start = state.row_index - start_;
        auto scanned = scan_vector(state, result, count, scan_vector_type::SCAN_FLAT_VECTOR);
        std::lock_guard update_guard(update_lock_);
        if (updates_ && scanned > 0) {
            result.flatten(result_offset + scanned);
            updates_->fetch_committed_range(range_start, scanned, result, result_offset);
        }
        return scanned;
    }

    void column_data_t::select(uint64_t vector_index,
                               column_scan_state& state,
                               vector::vector_t& result,
                               vector::indexing_vector_t& indexing,
                               uint64_t s_count) {
        scan(vector_index, state, result);
        result.slice(indexing, s_count);
    }

    void column_data_t::select_committed(uint64_t vector_index,
                                         column_scan_state& state,
                                         vector::vector_t& result,
                                         vector::indexing_vector_t& indexing,
                                         uint64_t s_count,
                                         bool allow_updates) {
        scan_committed(vector_index, state, result, allow_updates);
        result.slice(indexing, s_count);
    }

    void column_data_t::filter(uint64_t vector_index,
                               column_scan_state& state,
                               vector::vector_t& result,
                               vector::indexing_vector_t& indexing,
                               uint64_t& count,
                               const table_filter_t& filter) {
        uint64_t scan_count = scan(vector_index, state, result);

        vector::unified_vector_format uvf(result.resource(), scan_count);
        result.to_unified_format(scan_count, uvf);
        column_segment_t::filter_indexing(indexing, result, uvf, filter, scan_count, count);
    }

    void column_data_t::filter_scan(uint64_t vector_index,
                                    column_scan_state& state,
                                    vector::vector_t& result,
                                    vector::indexing_vector_t& indexing,
                                    uint64_t count) {
        scan(vector_index, state, result);
        result.slice(indexing, count);
    }

    void column_data_t::filter_scan_committed(uint64_t vector_index,
                                              column_scan_state& state,
                                              vector::vector_t& result,
                                              vector::indexing_vector_t& indexing,
                                              uint64_t count,
                                              bool allow_updates) {
        scan_committed(vector_index, state, result, allow_updates);
        result.slice(indexing, count);
    }

    void column_data_t::skip(column_scan_state& state, uint64_t count) { state.next(count); }

    core::result_wrapper_t<bool> column_data_t::initialize_append(column_append_state& state) {
        auto l = data_.lock();
        if (data_.is_empty(l)) {
            auto created = apend_transient_segment(l, start_);
            if (created.has_error()) {
                return created; // out_of_memory
            }
        }
        auto segment = data_.last_segment(l);
        // A disk-loaded segment is READ-ONLY: its in-memory buffer is the shared, reloadable
        // disk block it was loaded from. Appending into it would write new rows over that buffer,
        // corrupting both this column AND every other column packed into the same partial block.
        //
        // block_offset()==0 is not enough to rule a segment out: the checkpointer packs the FIRST
        // narrow column at offset 0 of the shared partial block, so a disk-loaded segment can have
        // block_offset()==0 and still be shared/read-only (an offset-only guard let the append land
        // directly in the shared block's buffer -> reopen corruption). is_reloadable() is true for any
        // registered disk block, so it forces a fresh appendable transient for EVERY disk-loaded
        // segment regardless of offset. The offset!=0 check stays as a belt-and-braces for
        // non-reloadable shared blocks.
        const bool is_disk_loaded = segment->block && segment->block->is_reloadable();
        if (is_disk_loaded || segment->block_offset() != 0) {
            auto created = apend_transient_segment(l, segment->start + static_cast<int64_t>(segment->count));
            if (created.has_error()) {
                return created; // out_of_memory
            }
            segment = data_.last_segment(l);
        }
        state.current = segment;
        auto init = state.current->initialize_append(state);
        if (init.has_error()) {
            return init;
        }
        return true;
    }

    core::result_wrapper_t<bool>
    column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        statistics_.update(vector, count);
        // Update per-segment statistics (conservative: full vector stats go to current segment)
        if (state.current) {
            base_statistics_t batch_stats(resource_, type_.type());
            batch_stats.update(vector, count);
            if (state.current->segment_statistics().has_stats()) {
                auto merged = state.current->segment_statistics();
                merged.merge(batch_stats);
                state.current->set_segment_statistics(std::move(merged));
            } else {
                state.current->set_segment_statistics(std::move(batch_stats));
            }
        }
        vector::unified_vector_format uvf(vector.resource(), count);
        vector.to_unified_format(count, uvf);
        return append_data(state, uvf, count);
    }

    core::result_wrapper_t<bool>
    column_data_t::append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t append_count) {
        uint64_t offset = 0;
        this->count_ += append_count;
        // Own a partial_block_manager so any segments filled (and re-pointed to disk) during this append are
        // PACKED into shared blocks via the same allocator the checkpoint path uses. A FILLED segment is a
        // whole 256 KiB block, so the allocator gives it a dedicated block (offset 0) -- packing does not
        // change its placement -- but routing it through the pbm keeps a single write-through code path. We
        // flush at the END of this append so every re-pointed filled segment's block is durable BEFORE the
        // append returns (and so before the row-group close / any scan or eviction of it): flush-before-evict.
        // No-op for in-memory tables (transition_segment_to_disk early-returns; nothing is buffered).
        storage::partial_block_manager_t pbm(block_manager_);
        bool any_transitioned = false;
        while (true) {
            // append may raise out_of_memory when a string-overflow allocate fails.
            auto appended = state.current->append(state, uvf, offset, append_count);
            if (appended.has_error()) {
                return appended.convert_error<bool>();
            }
            uint64_t copied_elements = appended.value();
            if (copied_elements == append_count) {
                break;
            }

            {
                auto l = data_.lock();
                // The just-filled segment is at the current tail; capture its index BEFORE appending the next
                // segment so we can re-point it to disk. state.current is moved to the new segment below, so the
                // filled segment is no longer referenced by the append state.
                const uint64_t filled_index = data_.segment_count(l) - 1;
                auto created =
                    apend_transient_segment(l, state.current->start + static_cast<int64_t>(state.current->count));
                if (created.has_error()) {
                    return created; // out_of_memory
                }
                // Write the now-complete segment through to the data file and swap it for a disk-backed,
                // evictable+reloadable segment (disk tables only; no-op for in-memory). A write/alloc failure
                // surfaces as io_error/out_of_memory and aborts the append cleanly.
                auto transitioned = transition_segment_to_disk(l, filled_index, pbm);
                if (transitioned.has_error()) {
                    return transitioned;
                }
                any_transitioned = true;
                state.current = data_.last_segment(l);
                auto init = state.current->initialize_append(state);
                if (init.has_error()) {
                    return init;
                }
            }
            offset += copied_elements;
            append_count -= copied_elements;
        }
        // Make every re-pointed filled segment's packed block durable before returning (flush-before-evict).
        if (any_transitioned) {
            pbm.flush_partial_blocks();
        }
        return true;
    }

    void column_data_t::revert_append(int64_t start_row) {
        auto l = data_.lock();
        auto last_segment = data_.last_segment(l);
        if (start_row >= last_segment->start + static_cast<int64_t>(last_segment->count)) {
            assert(start_row == last_segment->start + static_cast<int64_t>(last_segment->count));
            return;
        }
        uint64_t segment_index = data_.segment_index(l, start_row);
        auto segment = data_.segment_at(l, static_cast<int64_t>(segment_index));
        auto& transient = *segment;

        data_.erase_segments(l, segment_index);

        count_ = static_cast<uint64_t>(start_row - start_);
        segment->next = nullptr;
        transient.revert_append(static_cast<uint64_t>(start_row));
    }

    bool column_data_t::check_predicate(int64_t row_id, const table_filter_t* filter, core::error_t& error) {
        if (updates_ &&
            updates_->has_updates(static_cast<uint64_t>(row_id - start_) / vector::DEFAULT_VECTOR_CAPACITY)) {
            // The vector has some updated rows. Check if THIS specific row is updated.
            if (updates_->row_is_updated(row_id)) {
                // Row is in the update overlay — check against the updated value only
                return updates_->check_row(row_id, filter);
            }
        }
        // STRUCT columns store child data in sub_columns, not in data_ segments — fetch the full value
        // STRUCT/ARRAY/LIST columns store their payload in child columns, not in the
        // data_ segments, so a whole-value predicate (e.g. WHERE v = ARRAY[...]) must
        // materialize the full row value via fetch_row and compare it — the segment path
        // below would call the unimplemented array/list fetch().
        if (type_.to_physical_type() == types::physical_type::STRUCT ||
            type_.to_physical_type() == types::physical_type::ARRAY ||
            type_.to_physical_type() == types::physical_type::LIST) {
            column_fetch_state fetch_state;
            vector::vector_t result(resource_, type_, 1);
            fetch_row(fetch_state, row_id, result, 0);
            if (fetch_state.fetch_error.contains_error()) {
                error = fetch_state.fetch_error;
                return false;
            }
            if (!result.validity().row_is_valid(0)) {
                return false;
            }
            if (auto* set = dynamic_cast<const set_membership_filter_t*>(filter)) {
                return set->contains(result.value(0));
            }
            return filter->cast<constant_filter_t>().compare(result.value(0));
        }
        auto segment = data_.get_segment(row_id);
        // For compressed segments, fetch the actual decompressed value
        auto comp = segment->compression();
        if (comp == compression::compression_type::RLE || comp == compression::compression_type::DICTIONARY) {
            column_fetch_state fetch_state;
            vector::vector_t result(resource_, type_, 1);
            fetch_row(fetch_state, row_id, result, 0);
            if (fetch_state.fetch_error.contains_error()) {
                error = fetch_state.fetch_error;
                return false;
            }
            if (!result.validity().row_is_valid(0)) {
                return false;
            }
            // Dispatch handles both constant_filter_t and set_membership_filter_t.
            if (auto* set = dynamic_cast<const set_membership_filter_t*>(filter)) {
                return set->contains(result.value(0));
            }
            const auto& const_filter = filter->cast<constant_filter_t>();
            return const_filter.compare(result.value(0));
        }
        auto checked = segment->check_predicate(row_id, filter);
        if (checked.has_error()) {
            error = checked.error();
            return false;
        }
        return checked.value();
    }

    bool column_data_t::check_validity(int64_t row_id) {
        // Fetch a single row to check validity
        column_fetch_state fetch_state;
        vector::vector_t result(resource_, type_, 1);
        fetch_row(fetch_state, row_id, result, 0);
        return result.validity().row_is_valid(0);
    }

    uint64_t column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        assert(row_id >= 0);
        assert(row_id >= start_);
        state.row_index = start_ + (row_id - start_) / static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY *
                                                                            vector::DEFAULT_VECTOR_CAPACITY);
        state.current = data_.get_segment(state.row_index);
        state.internal_index = state.current->start;
        return scan_vector(state, result, vector::DEFAULT_VECTOR_CAPACITY, scan_vector_type::SCAN_FLAT_VECTOR);
    }

    void
    column_data_t::fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) {
        auto segment = data_.get_segment(row_id);

        segment->fetch_row(state, row_id, result, result_idx);

        fetch_update_row(row_id, result, result_idx);
    }

    core::result_wrapper_t<bool> column_data_t::update(uint64_t column_index,
                                                       vector::vector_t& update_vector,
                                                       int64_t* row_ids,
                                                       uint64_t update_count) {
        vector::vector_t base_vector(resource_, type_, count_);
        column_scan_state state;
        auto fetch_count = fetch(state, row_ids[0], base_vector);

        base_vector.flatten(fetch_count);
        return update_internal(column_index, update_vector, row_ids, update_count, base_vector);
    }

    core::result_wrapper_t<bool> column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                              vector::vector_t& update_vector,
                                                              int64_t* row_ids,
                                                              uint64_t update_count,
                                                              uint64_t) {
        return column_data_t::update(column_path[0], update_vector, row_ids, update_count);
    }

    void column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                std::vector<uint64_t> col_path,
                                                std::vector<column_segment_info>& result) {
        assert(!col_path.empty());

        std::string col_path_str = "[";
        for (uint64_t i = 0; i < col_path.size(); i++) {
            if (i > 0) {
                col_path_str += ", ";
            }
            col_path_str += std::to_string(col_path[i]);
        }
        col_path_str += "]";

        uint64_t segment_idx = 0;
        auto segment = data_.root_segment();
        while (segment) {
            column_segment_info column_info;
            column_info.row_group_index = row_group_index;
            column_info.column_id = col_path[0];
            column_info.column_path = col_path_str;
            column_info.segment_idx = segment_idx;
            column_info.segment_start = segment->start;
            column_info.segment_count = segment->count;
            column_info.has_updates = has_updates();
            auto segment_state = segment->segment_state();
            if (segment_state) {
                column_info.segment_info = segment_state->segment_info();
                column_info.additional_blocks = segment_state->additional_blocks();
            }
            result.emplace_back(column_info);

            segment_idx++;
            segment = data_.next_segment(segment);
        }
    }

    std::unique_ptr<column_data_t> column_data_t::create_column(std::pmr::memory_resource* resource,
                                                                storage::block_manager_t& block_manager,
                                                                uint64_t column_index,
                                                                int64_t start_row,
                                                                const types::complex_logical_type& type,
                                                                column_data_t* parent) {
        if (type.to_physical_type() == types::physical_type::STRUCT) {
            return std::make_unique<struct_column_data_t>(resource,
                                                          block_manager,
                                                          column_index,
                                                          start_row,
                                                          type,
                                                          parent);
        } else if (type.to_physical_type() == types::physical_type::LIST) {
            return std::make_unique<list_column_data_t>(resource, block_manager, column_index, start_row, type, parent);
        } else if (type.to_physical_type() == types::physical_type::ARRAY) {
            return std::make_unique<array_column_data_t>(resource,
                                                         block_manager,
                                                         column_index,
                                                         start_row,
                                                         type,
                                                         parent);
        } else if (type.to_physical_type() == types::physical_type::BIT) {
            return std::make_unique<validity_column_data_t>(resource, block_manager, column_index, start_row, *parent);
        }
        return std::make_unique<standard_column_data_t>(resource, block_manager, column_index, start_row, type, parent);
    }

    core::result_wrapper_t<bool> column_data_t::apend_transient_segment(std::unique_lock<std::mutex>& l,
                                                                        int64_t start_row) {
        const auto block_size = block_manager_.block_size();
        const auto type_size = type_.size();
        auto vector_segment_size = block_size;

        if (start_row == static_cast<uint64_t>(MAX_ROW_ID)) {
            vector_segment_size = vector::DEFAULT_VECTOR_CAPACITY * type_size;
        }

        uint64_t segment_size = block_size < vector_segment_size ? block_size : vector_segment_size;
        auto new_segment =
            column_segment_t::create_segment(block_manager_.buffer_manager, type_, start_row, segment_size, block_size);
        // create_segment returns an out_of_memory error_t when register_transient_memory fails.
        // Propagate it up the append chain instead of corrupting the segment tree.
        if (new_segment.has_error()) {
            return new_segment.convert_error<bool>();
        }
        allocation_size_ += segment_size;
        data_.append_segment(l, std::move(new_segment.value()));
        return true;
    }

    core::result_wrapper_t<bool> column_data_t::transition_segment_to_disk(std::unique_lock<std::mutex>& l,
                                                                           uint64_t segment_index,
                                                                           storage::partial_block_manager_t& pbm) {
        // In-memory tables have no backing store, so their segments must stay managed (no disk copy ->
        // clean OOM, never a crash). No-op.
        if (block_manager_.in_memory()) {
            return true;
        }

        auto* segment = data_.segment_at(l, static_cast<int64_t>(segment_index));
        if (!segment) {
            return true;
        }

        // Only re-point UNCOMPRESSED, fully-in-memory (managed) segments whose payload is a self-contained raw
        // block at offset 0: a byte copy round-trips losslessly for them. Skip a segment that is already
        // disk-backed (is_reloadable() true), shares a block (block_offset != 0), or is compressed.
        if (segment->block && segment->block->is_reloadable()) {
            return true; // already disk-backed
        }
        if (segment->block_offset() != 0) {
            return true; // shares a block with other segments (loaded) -- not a fresh transient
        }
        if (segment->compression() != compression::compression_type::UNCOMPRESSED) {
            return true;
        }
        // Re-pointable iff the segment's payload is a self-contained raw block at offset 0 that round-trips
        // through a byte copy: fixed-width physical types AND validity bitmaps (BIT). STRING carries overflow
        // blocks / a dictionary in segment_state; STRUCT/ARRAY/LIST keep their payload in child columns -- those
        // stay managed. BIT is included: a validity bitmap is a raw block at offset 0, and a disk-backed
        // validity segment reloads its bitmap from the file like any other block (the 0xFF-initialize in the
        // column_segment_t ctor only fires for INVALID_BLOCK transient segments, not for a registered disk
        // block).
        const auto phys = segment->type.to_physical_type();
        const bool is_raw_copyable = (phys != types::physical_type::STRING && phys != types::physical_type::INVALID &&
                                      phys != types::physical_type::STRUCT && phys != types::physical_type::ARRAY &&
                                      phys != types::physical_type::LIST);
        if (!is_raw_copyable) {
            return true;
        }

        // An EMPTY segment (no rows yet -- e.g. the freshly-opened tail of a row group) has nothing to write
        // through: re-pointing it would pack a 0-byte payload and persist a segment_size==0 disk segment,
        // a degenerate on-disk shape the checkpoint/reload path does not expect. Leave it managed; it stays
        // appendable and will be re-pointed once it fills (append on-fill path) or by a later checkpoint once
        // it holds rows. (Pre-B2 this re-pointed to a full-block dedicated segment; B2's tight packing makes
        // the empty case degenerate, so skip it explicitly.)
        if (segment->count.load() == 0) {
            return true;
        }

        // Snapshot the segment metadata BEFORE touching the pin: the segment is destroyed by the swap below.
        const int64_t seg_start = segment->start;
        const uint64_t seg_count = segment->count.load();
        const uint64_t alloc_segment_size = segment->segment_size();
        const uint64_t block_offset = segment->block_offset();
        assert(alloc_segment_size <= block_manager_.block_size());
        const bool has_stats = segment->segment_statistics().has_stats();
        base_statistics_t seg_stats =
            has_stats ? segment->segment_statistics() : base_statistics_t(resource_, type_.type());

        // The transient segment was allocated for a WHOLE block (segment_size() == block_size) but is filled with
        // only `seg_count` rows. Packing must place the USED payload, not the full allocated block, or every
        // segment would exceed 0.8*block_size and grab a dedicated block (no packing at all). Compute the
        // tightly-used byte extent for this segment's physical layout -- this is exactly the byte range the
        // scan/fetch read paths address (handle.ptr()+block_offset, indexed by row), so copying it round-trips
        // losslessly:
        //   * validity bitmaps (BIT): one STANDARD_MASK_SIZE chunk per DEFAULT_VECTOR_CAPACITY rows (the
        //     layout validity_append writes);
        //   * other raw-copyable fixed-width types: seg_count * type_size.
        // Clamp to the allocated size as a safety net (used can never exceed the segment's own allocation).
        uint64_t used_bytes;
        if (phys == types::physical_type::BIT) {
            const uint64_t vectors =
                (seg_count + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY;
            used_bytes = vectors * vector::validity_mask_t::STANDARD_MASK_SIZE;
        } else {
            used_bytes = seg_count * type_.size();
        }
        if (used_bytes > alloc_segment_size) {
            used_bytes = alloc_segment_size;
        }
        const uint64_t segment_size = used_bytes;

        // Allocate a COMPACT block placement via the partial-block allocator (segment packing): a segment
        // larger than 0.8*block_size gets a dedicated whole block (offset 0), a smaller one is PACKED into a
        // shared partial block at a (possibly non-zero) offset alongside other re-pointed segments. This is
        // the SAME mechanism the checkpoint path uses (row_group_t::write_to_disk -> column flush), so narrow
        // column segments no longer each consume a dedicated 256 KiB block.
        const auto alloc = pbm.get_block_allocation(segment_size);

        // Pin the filled managed segment to read its payload and copy it INTO the partial block's in-memory
        // buffer at the allocated offset. NOTHING reaches the data file here: pbm.write_to_block only fills a
        // zeroed in-memory block buffer; the block is flushed by the caller's pbm.flush_partial_blocks() BEFORE
        // any eviction/reload of the re-pointed segment (flush-before-evict). The pin is released at the end of
        // this scope, BEFORE the swap drops the old segment's block_handle -- otherwise the pin's raw handle
        // would dangle. A pin OOM surfaces as an error_t.
        //
        // On pin OOM we DO NOT free alloc.block_id: a packed block may be SHARED by already-placed segments, so
        // freeing its id would corrupt them. We just return the error; a few reserved bytes in a partial block
        // leak until restart, matching the leak-not-corrupt policy of collect_disk_block_ids below.
        {
            auto& buffer_manager = block_manager_.buffer_manager;
            auto pinned = buffer_manager.pin(segment->block);
            if (pinned.has_error()) {
                return pinned.convert_error<bool>();
            }
            auto* payload = pinned.value().ptr() + block_offset;
            pbm.write_to_block(alloc.block_id, alloc.offset_in_block, payload, segment_size);
        }

        // Build a NEW disk-backed segment over the packed block at the allocated offset. register_block dedupes
        // by block_id (weak_ptr registry): segments packed into the SAME block share ONE block_handle, so the
        // pool evicts/reloads the shared block once. The handle is UNLOADED and is_reloadable() is true: the
        // pool can evict it and block_handle::load() reloads it from the data file (after the caller flushes).
        // The new segment's segment_size is the TIGHT used size; scan/fetch read seg_count rows from it.
        auto block_handle = block_manager_.register_block(alloc.block_id);
        auto new_segment = std::make_unique<column_segment_t>(block_handle,
                                                             type_,
                                                             seg_start,
                                                             seg_count,
                                                             static_cast<uint32_t>(alloc.block_id),
                                                             alloc.offset_in_block,
                                                             segment_size);
        new_segment->set_compression(compression::compression_type::UNCOMPRESSED);
        if (has_stats) {
            new_segment->set_segment_statistics(std::move(seg_stats));
        }

        // Swap the live segment for the disk-backed one under the tree lock. The old managed
        // column_segment_t / block_handle is then unreferenced -> its in-memory buffer is released; the
        // pool can later evict+reload the new disk-backed block. MVCC version info is keyed by row position
        // (unchanged: same start/count) so visibility is preserved.
        data_.replace_segment_at_index(l, segment_index, std::move(new_segment));
        return true;
    }

    void column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        // Collect the ids of disk blocks owned by this column's segments so the SOLE caller
        // (data_table_t::compact's free-list reclaim) can return them to the block manager once the WHOLE
        // collection these segments belong to is being torn down (replaced by the compacted one). Because
        // the entire owning collection is discarded, EVERY reloadable disk block it references is freeable,
        // INCLUDING packed/shared partial blocks: a block packed with several of this collection's segments
        // at distinct offsets is referenced ONLY by this (about-to-drop) collection, so freeing its id once
        // is safe -- no live segment elsewhere points at it, and the compacted collection allocated FRESH,
        // disjoint ids via the write-through allocator.
        //
        // B2 made packing the COMMON case (narrow column segments share blocks), so the previous
        // dedicated-block-only discriminator (block_offset()==0 && segment_size() > 0.8*block) leaked nearly
        // every block on each compaction -> unbounded file growth. We now emit one entry per reloadable
        // segment; the caller DEDUPES before freeing (multiple packed segments report the SAME block id).
        for (auto& segment : const_cast<segment_tree_t<column_segment_t>&>(data_).segments()) {
            if (segment.block && segment.block->is_reloadable()) {
                out.push_back(segment.block->block_id());
            }
        }
    }

    core::result_wrapper_t<bool> column_data_t::transition_to_disk(storage::partial_block_manager_t& pbm) {
        if (block_manager_.in_memory()) {
            return true; // no backing store -> segments stay managed (clean OOM, never a crash)
        }
        auto l = data_.lock();
        const uint64_t count = data_.segment_count(l);
        for (uint64_t i = 0; i < count; i++) {
            auto transitioned = transition_segment_to_disk(l, i, pbm);
            if (transitioned.has_error()) {
                return transitioned; // io_error / out_of_memory
            }
        }
        return true;
    }

    uint64_t column_data_t::scan_vector(column_scan_state& state,
                                        vector::vector_t& result,
                                        uint64_t remaining,
                                        scan_vector_type scan_type) {
        if (scan_type == scan_vector_type::SCAN_FLAT_VECTOR && result.get_vector_type() != vector::vector_type::FLAT) {
            throw std::logic_error("scan_vector called with SCAN_FLAT_VECTOR but result is not a flat vector");
        }
        state.previous_states.clear();
        if (!state.initialized) {
            assert(state.current);
            state.current->initialize_scan(state);
            // initialize_scan records a pin OOM in state.scan_error. Bail with nothing scanned;
            // row_group_t aggregates scan_error and the scan loops stop.
            if (state.has_error()) {
                return 0;
            }
            state.internal_index = state.current->start;
            state.initialized = true;
        }
        assert(data_.has_segment(state.current));
        assert(state.internal_index <= state.row_index);
        if (state.internal_index < state.row_index) {
            state.current->skip(state);
        }
        assert(state.current->type == type_);
        uint64_t initial_remaining = remaining;
        while (remaining > 0) {
            assert(state.row_index >= state.current->start &&
                   state.row_index <= state.current->start + static_cast<int64_t>(state.current->count));
            uint64_t scan_count = std::min(remaining,
                                           static_cast<uint64_t>(state.current->start) + state.current->count -
                                               static_cast<uint64_t>(state.row_index));
            uint64_t result_offset = state.result_offset + initial_remaining - remaining;
            if (scan_count > 0) {
                for (uint64_t i = 0; i < scan_count; i++) {
                    column_fetch_state fetch_state;
                    state.current->fetch_row(fetch_state,
                                             state.row_index + static_cast<int64_t>(i),
                                             result,
                                             result_offset + i);
                    // Propagate a per-row pin OOM up into the scan state and bail.
                    if (fetch_state.fetch_error.contains_error()) {
                        state.scan_error = fetch_state.fetch_error;
                        state.internal_index = state.row_index;
                        return initial_remaining - remaining;
                    }
                }
                state.current->scan(state, scan_count, result, result_offset, scan_type);

                state.row_index += static_cast<int64_t>(scan_count);
                remaining -= scan_count;
            }

            if (remaining > 0) {
                auto next = data_.next_segment(state.current);
                if (!next) {
                    break;
                }
                state.previous_states.emplace_back(std::move(state.scan_state));
                state.current = next;
                state.current->initialize_scan(state);
                if (state.has_error()) {
                    state.internal_index = state.row_index;
                    return initial_remaining - remaining;
                }
                state.segment_checked = false;
                assert(state.row_index >= state.current->start &&
                       state.row_index <= state.current->start + static_cast<int64_t>(state.current->count));
            }
        }
        state.internal_index = state.row_index;
        return initial_remaining - remaining;
    }

    template<bool SCAN_COMMITTED, bool ALLOW_UPDATES>
    uint64_t column_data_t::scan_vector(uint64_t vector_index,
                                        column_scan_state& state,
                                        vector::vector_t& result,
                                        uint64_t target_scan) {
        auto scan_type = get_vector_scan_type(state, target_scan, result);
        auto scan_count = scan_vector(state, result, target_scan, scan_type);
        if (scan_type != scan_vector_type::SCAN_ENTIRE_VECTOR) {
            auto update_index = vector_index - static_cast<uint64_t>(start_) / vector::DEFAULT_VECTOR_CAPACITY;
            fetch_updates(update_index, result, state.result_offset, scan_count, ALLOW_UPDATES, SCAN_COMMITTED);
        }
        return scan_count;
    }

    void column_data_t::clear_updates() {
        std::lock_guard update_guard(update_lock_);
        updates_.reset();
    }

    void column_data_t::fetch_updates(uint64_t vector_index,
                                      vector::vector_t& result,
                                      uint64_t result_offset,
                                      uint64_t scan_count,
                                      bool allow_updates,
                                      bool scan_committed) {
        std::lock_guard update_guard(update_lock_);
        if (!updates_) {
            return;
        }
        if (!allow_updates) {
            throw std::logic_error("Cannot create index with outstanding updates");
        }
        result.flatten(scan_count);
        if (scan_committed) {
            updates_->fetch_committed(vector_index, result_offset, result);
        } else {
            updates_->fetch_updates(vector_index, result_offset, result);
        }
    }

    void column_data_t::fetch_update_row(int64_t row_id, vector::vector_t& result, uint64_t result_idx) {
        std::lock_guard update_guard(update_lock_);
        if (!updates_) {
            return;
        }
        updates_->fetch_row(row_id, result, result_idx);
    }

    core::result_wrapper_t<bool> column_data_t::update_internal(uint64_t column_index,
                                                                vector::vector_t& update_vector,
                                                                int64_t* row_ids,
                                                                uint64_t update_count,
                                                                vector::vector_t& base_vector) {
        std::lock_guard update_guard(update_lock_);
        if (!updates_) {
            updates_ = std::make_unique<update_segment_t>(*this);
        }
        return updates_->update(column_index, update_vector, row_ids, update_count, base_vector);
    }

    uint64_t column_data_t::vector_count(uint64_t vector_index) const {
        uint64_t current_row = vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        return std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY,
                                  static_cast<uint64_t>(start_) + count_ - current_row);
    }

    core::result_wrapper_t<persistent_column_data_t>
    column_data_t::checkpoint(storage::partial_block_manager_t& partial_block_manager) {
        column_data_checkpointer_t checkpointer(*this, partial_block_manager);
        auto persistent = checkpointer.checkpoint();
        if (persistent.has_error()) {
            return persistent;
        }
        // Re-point the still-managed LIVE segments (the open tail that was not filled during append, and
        // so never went through the on-fill write-through) to disk-backed, evictable blocks so the
        // post-checkpoint live table stays bounded. The on-disk metadata returned above is independent of
        // the live tree (it references the partial_block_manager's allocations), so the re-point cannot
        // corrupt the checkpoint. No-op for in-memory tables and for already-disk-backed segments. A
        // write/alloc failure surfaces as io_error/out_of_memory.
        //
        // Use a SEPARATE short-lived partial_block_manager (NOT the checkpoint's `partial_block_manager`,
        // which collection_t::checkpoint already flushed): the re-point packs the live tail's segments into
        // their own shared blocks, and we flush HERE so every re-pointed live segment's block is durable
        // before the post-checkpoint table is scanned/evicted (flush-before-evict).
        storage::partial_block_manager_t repoint_pbm(block_manager_);
        auto repointed = transition_to_disk(repoint_pbm);
        if (repointed.has_error()) {
            return repointed.convert_error<persistent_column_data_t>();
        }
        repoint_pbm.flush_partial_blocks();
        return persistent;
    }

    void column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        auto l = data_.lock();
        for (uint32_t i = 0; i < persistent_data.data_pointers.size(); i++) {
            const auto& dp = persistent_data.data_pointers[i];
            auto block_handle = block_manager_.register_block(dp.block_pointer.block_id);

            auto segment = std::make_unique<column_segment_t>(block_handle,
                                                              type_,
                                                              static_cast<int64_t>(dp.row_start),
                                                              dp.tuple_count,
                                                              static_cast<uint32_t>(dp.block_pointer.block_id),
                                                              dp.block_pointer.offset,
                                                              dp.segment_size);
            segment->set_compression(dp.compression);
            if (i < persistent_data.segment_statistics.size() && persistent_data.segment_statistics[i].has_stats()) {
                segment->set_segment_statistics(persistent_data.segment_statistics[i]);
            }
            data_.append_segment(l, std::move(segment));
        }
        if (!persistent_data.data_pointers.empty()) {
            uint64_t total = 0;
            for (const auto& dp : persistent_data.data_pointers) {
                total += dp.tuple_count;
            }
            count_ = total;
        }
        if (persistent_data.statistics.has_stats()) {
            statistics_ = persistent_data.statistics;
        }
    }

    void column_data_t::initialize_column_validity(const persistent_column_data_t& persistent_data) {
        // Validity is not persisted separately (a checkpoint flushes only the main column's
        // segments); on reopen the bitmap is implicitly all-valid. We materialize one validity
        // segment per main-column data pointer and, on a DISK-backed table, write each all-valid
        // bitmap THROUGH to the data file and swap it for a disk-backed (reloadable) segment via
        // transition_segment_to_disk -- the same mechanism used on the append fill path. Without
        // this the reopen-rebuilt validity segments stay managed (block_id >= MAXIMUM_BLOCK), so
        // the eviction guard pins them all resident and reopening a large table under a small pool
        // exhausts it. The transient segment's column_segment_t ctor 0xFF-fills the bitmap
        // (all-valid) before the transition copies it to disk, so reloaded validity reads all-valid.
        //
        // Own a partial_block_manager so the all-valid validity segments are PACKED into shared blocks
        // (segment packing) and flush it at the end of the loop -- the flush is the flush-before-evict
        // guarantee: every re-pointed validity segment's block is durable before the reopened table is
        // scanned/evicted.
        storage::partial_block_manager_t pbm(block_manager_);
        auto l = data_.lock();
        for (const auto& dp : persistent_data.data_pointers) {
            // Disk-load path: segments are sized for known on-disk tuple counts. An OOM here
            // is not threaded to an agent boundary; assert and skip the row on exhaustion.
            auto created = apend_transient_segment(l, static_cast<int64_t>(dp.row_start));
            assert(!created.has_error() && "initialize_column_validity: transient segment OOM");
            if (created.has_error()) {
                continue;
            }
            const uint64_t seg_index = data_.segment_count(l) - 1;
            auto* seg = data_.last_segment(l);
            if (seg) {
                seg->count = dp.tuple_count;
            }
            // Write the all-valid bitmap through to disk and re-point the live segment to a
            // disk-backed, evictable+reloadable block (no-op for in-memory tables). A write/alloc
            // failure surfaces as io_error/out_of_memory; the disk-load path is not threaded to an
            // agent boundary, so assert and keep the managed segment on failure.
            auto transitioned = transition_segment_to_disk(l, seg_index, pbm);
            assert(!transitioned.has_error() && "initialize_column_validity: write-through failed");
        }
        pbm.flush_partial_blocks();
        if (!persistent_data.data_pointers.empty()) {
            uint64_t total = 0;
            for (const auto& dp : persistent_data.data_pointers) {
                total += dp.tuple_count;
            }
            count_ = total;
        }
    }

} // namespace components::table