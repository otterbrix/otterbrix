#include "array_column_data.hpp"
#include "persistent_column_data.hpp"
#include "row_group.hpp"
#include <components/vector/vector.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::table {

    array_column_data_t::array_column_data_t(std::pmr::memory_resource* resource,
                                             storage::block_manager_t& block_manager,
                                             uint64_t column_index,
                                             int64_t start_row,
                                             types::complex_logical_type type,
                                             column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , child_column(create_column(resource, block_manager, 1, start_row, type_.child_type(), this))
        , validity(resource, block_manager, 0, start_row, *this) {}

    void array_column_data_t::set_start(int64_t new_start) {
        start_ = new_start;
        child_column->set_start(new_start);
        validity.set_start(new_start);
    }

    filter_propagate_result_t array_column_data_t::check_zonemap(column_scan_state&, table_filter_t&) {
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    void array_column_data_t::initialize_scan(column_scan_state& state) {
        assert(state.child_states.size() == 2);

        state.row_index = 0;
        state.current = nullptr;

        validity.initialize_scan(state.child_states[0]);
        child_column->initialize_scan(state.child_states[1]);
    }

    void array_column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
        assert(state.child_states.size() == 2);

        if (row_idx == 0) {
            initialize_scan(state);
            return;
        }

        state.row_index = row_idx;
        state.current = nullptr;

        validity.initialize_scan_with_offset(state.child_states[0], row_idx);

        auto size = array_size();
        auto child_count = static_cast<uint64_t>(row_idx - start_) * size;

        assert(child_count <= child_column->max_entry());
        if (child_count < child_column->max_entry()) {
            const auto child_offset = start_ + static_cast<int64_t>(child_count);
            child_column->initialize_scan_with_offset(state.child_states[1], child_offset);
        }
    }

    uint64_t array_column_data_t::scan(uint64_t vector_index,
                                       column_scan_state& state,
                                       vector::vector_t& result,
                                       uint64_t count) {
        size_t arr_size = array_size();
        // Scan the array-level validity into the result first (mirrors struct/scan_count);
        // without this a row stored as a whole-array NULL reads back as a non-null array.
        // The validity child writes into the parent result vector, so it targets the parent's
        // result base — NOT the element offset: a `+= element_count` bookkeeping drifts it by
        // arr_size per row and folds any multi-vector scan's NULL bits into one chunk.
        state.child_states[0].result_offset = state.result_offset;
        validity.scan(vector_index, state.child_states[0], result, count);
        size_t remaining_count = arr_size * count;
        uint64_t remaining_vector_index = vector_index * arr_size;
        state.child_states[1].result_offset = state.result_offset * arr_size;

        while (remaining_count > 0) {
            if (remaining_count >= vector::DEFAULT_VECTOR_CAPACITY) {
                auto result_count = child_column->scan(remaining_vector_index,
                                                       state.child_states[1],
                                                       result.entry(),
                                                       vector::DEFAULT_VECTOR_CAPACITY);
                remaining_count -= result_count;
                remaining_vector_index++;
                state.child_states[1].result_offset += result_count;
            } else {
                auto result_count =
                    child_column->scan(remaining_vector_index, state.child_states[1], result.entry(), remaining_count);
                remaining_count -= result_count;
                state.child_states[1].result_offset += result_count;
                break;
            }
        }
        uint64_t result_count = (arr_size * count - remaining_count) / arr_size;
        // Validity and elements were read on child states; row_group_t judges only this one.
        state.collect_child_errors();
        return result_count;
    }

    uint64_t array_column_data_t::scan_committed(uint64_t,
                                                 column_scan_state& state,
                                                 vector::vector_t& result,
                                                 bool,
                                                 uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t array_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        state.child_states[0].result_offset = state.result_offset; // see scan(): validity targets the parent base
        auto scan_count = validity.scan_count(state.child_states[0], result, count);
        auto& child_vec = result.entry();
        auto size = array_size();
        state.child_states[1].result_offset = state.result_offset * size;
        child_column->scan_count(state.child_states[1], child_vec, count * size);
        state.collect_child_errors(); // see scan()
        return scan_count;
    }

    void array_column_data_t::skip(column_scan_state& state, uint64_t count) {
        validity.skip(state.child_states[0], count);
        auto size = array_size();
        child_column->skip(state.child_states[1], count * size);
    }

    core::result_wrapper_t<bool> array_column_data_t::initialize_append(column_append_state& state) {
        column_append_state validity_append;
        auto v = validity.initialize_append(validity_append);
        if (v.has_error()) {
            return v; // out_of_memory (rules 2/9)
        }
        state.child_appends.push_back(std::move(validity_append));

        column_append_state child_append;
        auto child = child_column->initialize_append(child_append);
        if (child.has_error()) {
            return child;
        }
        state.child_appends.push_back(std::move(child_append));
        return true;
    }

    core::result_wrapper_t<bool>
    array_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        if (vector.get_vector_type() != vector::vector_type::FLAT) {
            vector::vector_t append_vector(vector);
            append_vector.flatten(count);
            return append(state, append_vector, count);
        }

        auto v = validity.append(state.child_appends[0], vector, count);
        if (v.has_error()) {
            return v; // out_of_memory (rules 2/9)
        }
        auto& child_vec = vector.entry();
        auto size = array_size();
        auto child = child_column->append(state.child_appends[1], child_vec, count * size);
        if (child.has_error()) {
            return child;
        }

        count_ += count;
        return true;
    }

    core::result_wrapper_t<bool> array_column_data_t::revert_append(int64_t start_row) {
        auto v = validity.revert_append(start_row);
        if (v.has_error()) {
            return v;
        }
        // start_row is COLLECTION-ABSOLUTE (see column_data_t::revert_append). The child
        // column shares this column's start_ but is addressed in ELEMENTS from the row
        // group base (see initialize_scan_with_offset), so its absolute truncation row is
        // start_ + surviving_rows * array_size. The old start_row * array_size coincides
        // only in row group 0 (start_ == 0); for any later group it pointed far past the
        // child's end and the stale child tail survived the revert.
        auto size = array_size();
        auto child = child_column->revert_append(start_ + (start_row - start_) * static_cast<int64_t>(size));
        if (child.has_error()) {
            return child;
        }

        count_ = static_cast<uint64_t>(start_row - start_);
        return true;
    }

    uint64_t array_column_data_t::fetch(column_scan_state& state, int64_t, vector::vector_t&) {
        // POINT FETCH OF A WHOLE ARRAY CELL IS NOT IMPLEMENTED, and this override exists to say
        // so rather than to be filled in. Deleting it would be worse than leaving it: an ARRAY
        // node owns NO segments at all (see initialize_column), so the base column_data_t::fetch
        // would dereference an empty segment tree.
        //
        // Nothing calls it. column_data_t::fetch has exactly two call sites: column_data_t::update
        // (on `this`) and struct_column_data_t::fetch (on a field). ARRAY, LIST and STRUCT all
        // override BOTH update and update_column, so column_data_t::update is never entered with a
        // nested node as `this`; struct_column_data_t::fetch therefore has no caller either, and
        // neither has this. No SQL statement names the path: whole-array reads go through
        // scan/scan_count, and the in-place ARRAY update rewrites the element column directly.
        //
        // The refusal travels on the channel the ONE potential caller already reads:
        // column_data_t::update checks state.has_error() right after fetch() and returns
        // state.scan_error. A throw here would unwind into the disk agent's coroutine, whose
        // unhandled_exception() is empty — a hang, not an error (rules 2/9).
        state.scan_error =
            core::error_t(core::error_code_t::unimplemented_yet,
                          std::pmr::string("point fetch of a whole ARRAY cell is not implemented", resource_));
        return 0;
    }

    core::result_wrapper_t<bool> array_column_data_t::update(uint64_t column_index,
                                                             vector::vector_t& update_vector,
                                                             int64_t* row_ids,
                                                             uint64_t update_count) {
        const int64_t arr_size = static_cast<int64_t>(array_size());
        const uint64_t total = static_cast<uint64_t>(arr_size) * update_count;
        std::pmr::vector<int64_t> sub_column_ids(resource_);
        sub_column_ids.reserve(total);

        // Element-space ids, REBASED the way every read leg addresses them (fetch_row,
        // revert_append): element row = start_ + (row - start_) * array_size + i. The
        // un-rebased `row * array_size + i` coincided only in row group 0 (start_ == 0);
        // for any later group the overlay landed on rows the reads never visit.
        for (auto it = row_ids; it != row_ids + update_count; ++it) {
            for (int64_t i = 0; i < arr_size; i++) {
                sub_column_ids.emplace_back(start_ + (*it - start_) * arr_size + i);
            }
        }

        // One child update per element run that stays inside ONE update window, with the
        // element vector SLICED to the same run: update_segment_t::update addresses its update
        // vector by POSITION WITHIN THE CALL, so handing it the whole element vector while the
        // ids came from a later window made it read the wrong slice (that mismatch is what the
        // deleted `+ vector_index * DEFAULT_VECTOR_CAPACITY` hack in initialize_update_data
        // compensated for, correctly ONLY when the ids were dense from element zero). Chunking
        // blindly by 1024 ids had the same alignment assumption; the runs below split on real
        // window boundaries instead.
        auto& child_vector = update_vector.entry();
        const int64_t child_start = child_column->start();
        const int64_t cap = static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t pos = 0;
        while (pos < total) {
            const uint64_t run_start = pos;
            const int64_t window = (sub_column_ids[pos] - child_start) / cap;
            for (pos++; pos < total && (sub_column_ids[pos] - child_start) / cap == window; pos++) {
            }
            const uint64_t run = pos - run_start;
            vector::vector_t window_slice(child_vector, run_start, run);
            window_slice.flatten(run);
            auto child = child_column->update(column_index, window_slice, sub_column_ids.data() + run_start, run);
            if (child.has_error()) {
                return child;
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> array_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                                    vector::vector_t& update_vector,
                                                                    int64_t* row_ids,
                                                                    uint64_t update_count,
                                                                    uint64_t depth) {
        const int64_t arr_size = static_cast<int64_t>(array_size());
        const uint64_t total = static_cast<uint64_t>(arr_size) * update_count;
        std::pmr::vector<int64_t> sub_column_ids(resource_);
        sub_column_ids.reserve(total);

        // Same rebase as update() above.
        for (auto it = row_ids; it != row_ids + update_count; ++it) {
            for (int64_t i = 0; i < arr_size; i++) {
                sub_column_ids.emplace_back(start_ + (*it - start_) * arr_size + i);
            }
        }

        // Same window-run walk as update() above — and ONLY the walk: a whole-range call after
        // the loop would apply the entire update a SECOND time.
        auto& child_vector = update_vector.entry();
        const int64_t child_start = child_column->start();
        const int64_t cap = static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t pos = 0;
        while (pos < total) {
            const uint64_t run_start = pos;
            const int64_t window = (sub_column_ids[pos] - child_start) / cap;
            for (pos++; pos < total && (sub_column_ids[pos] - child_start) / cap == window; pos++) {
            }
            const uint64_t run = pos - run_start;
            vector::vector_t window_slice(child_vector, run_start, run);
            window_slice.flatten(run);
            auto child = child_column->update_column(column_path,
                                                     window_slice,
                                                     sub_column_ids.data() + run_start,
                                                     run,
                                                     depth);
            if (child.has_error()) {
                return child;
            }
        }
        return true;
    }

    void array_column_data_t::fetch_row(column_fetch_state& state,
                                        int64_t row_id,
                                        vector::vector_t& result,
                                        uint64_t result_idx) {
        // state.child(0), not a default-constructed state: a throwaway state lands the validity
        // bitmap's pin OOM in a child nobody reads (see column_fetch_state::child).
        auto& validity_state = state.child(0);
        validity.fetch_row(validity_state, row_id, result, result_idx);
        if (state.absorb_error(validity_state)) {
            return;
        }

        auto& child_vec = result.entry();
        auto& child_type = type_.child_type();

        auto child_state = std::make_unique<column_scan_state>();
        child_state->initialize(child_type);
        auto size = array_size();
        const auto child_offset = start_ + (row_id - start_) * static_cast<int64_t>(size);

        child_column->initialize_scan_with_offset(*child_state, child_offset);
        vector::vector_t child_scan(resource_, child_type, size);
        child_column->scan_count(*child_state, child_scan, size);
        // The elements are read on a SCAN state (the bulk leg owns its strings, so the pins half
        // of the channel does not apply here) — but its scan_error is still an error of THIS
        // fetch, and it was the only place a corrupt element was reported.
        child_state->collect_child_errors();
        if (child_state->has_error()) {
            if (!state.fetch_error.contains_error()) {
                state.fetch_error = child_state->scan_error;
            }
            return;
        }
        vector::vector_ops::copy(child_scan, child_vec, size, 0, result_idx * size);
    }

    void array_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                      std::vector<uint64_t> col_path,
                                                      std::vector<column_segment_info>& result) {
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, col_path, result);
        col_path.back() = 1;
        child_column->get_column_segment_info(row_group_index, col_path, result);
    }

    size_t array_column_data_t::array_size() const {
        return static_cast<const types::array_logical_type_extension*>(type_.extension())->size();
    }

    core::result_wrapper_t<bool>
    array_column_data_t::checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                                             persistent_column_data_t& persistent) {
        // v1 convention: child_columns[0] is the array's own validity bitmap (whole-cell
        // NULLs), child_columns[1] the element column (rows * array_size entries, carrying
        // the element-level validity in its own record).
        auto valid = validity.checkpoint(partial_block_manager);
        if (valid.has_error()) {
            return valid.convert_error<bool>(); // out_of_memory
        }
        persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(std::move(valid.value())));
        auto child = child_column->checkpoint(partial_block_manager);
        if (child.has_error()) {
            return child.convert_error<bool>(); // out_of_memory
        }
        persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(std::move(child.value())));
        return true;
    }

    core::result_wrapper_t<bool>
    array_column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        // An array node owns no segments of its own: its row count comes from the persisted
        // count, its validity is the persisted child_columns[0] bitmap, and the elements
        // (rows * array_size) live in the persisted child_columns[1]. Any other shape is
        // data_corruption — never "assume all-valid".
        count_ = persistent_data.count;
        if (persistent_data.child_columns.size() != 2) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("array column load: checkpoint must carry validity + the element child", resource()));
        }
        auto valid = validity.initialize_column(*persistent_data.child_columns[0]);
        if (valid.has_error()) {
            return valid;
        }
        if (validity.count() != count_) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("array column load: validity row count does not match the column", resource()));
        }
        return child_column->initialize_column(*persistent_data.child_columns[1]);
    }

    void array_column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        // The base walk of the own data_ tree finds nothing (an array node keeps no segments,
        // see initialize_column above); it is kept so every node reports through one path.
        // What a reloaded array column actually owns is its children: the validity bitmap and
        // the element column, each sitting on the blocks initialize_column registered. Without
        // this override compact leaks all of them.
        column_data_t::collect_disk_block_ids(out);
        validity.collect_disk_block_ids(out);
        child_column->collect_disk_block_ids(out);
    }

} // namespace components::table