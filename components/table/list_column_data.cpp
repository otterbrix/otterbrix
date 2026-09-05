#include "list_column_data.hpp"

#include "persistent_column_data.hpp"

namespace components::table {

    list_column_data_t::list_column_data_t(std::pmr::memory_resource* resource,
                                           storage::block_manager_t& block_manager,
                                           uint64_t column_index,
                                           int64_t start_row,
                                           types::complex_logical_type type,
                                           column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , child_column(create_column(resource, block_manager, 1, start_row, type_.child_type(), this))
        , validity(resource, block_manager, 0, start_row, *this) {
        assert(type_.to_physical_type() == types::physical_type::LIST);
    }

    void list_column_data_t::set_start(int64_t new_start) {
        column_data_t::set_start(new_start);
        child_column->set_start(new_start);
        validity.set_start(new_start);
    }

    filter_propagate_result_t list_column_data_t::check_zonemap(column_scan_state&, table_filter_t&) {
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    void list_column_data_t::initialize_scan(column_scan_state& state) {
        column_data_t::initialize_scan(state);

        assert(state.child_states.size() == 2);
        validity.initialize_scan(state.child_states[0]);

        child_column->initialize_scan(state.child_states[1]);
    }

    core::result_wrapper_t<uint64_t> list_column_data_t::fetch_list_offset(int64_t row_idx) {
        auto segment = data_.get_segment(row_idx);
        if (!segment) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("list column: the row id names no offsets segment", resource_));
        }
        column_fetch_state fetch_state;
        vector::vector_t result(resource_, type_, 1);
        segment->fetch_row(fetch_state, row_idx, result, 0U);
        if (fetch_state.fetch_error.contains_error()) {
            // A failed pin used to fall through to the read below and answer garbage.
            return fetch_state.fetch_error;
        }

        return result.data<uint64_t>()[0];
    }

    void list_column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
        if (row_idx == 0) {
            initialize_scan(state);
            return;
        }
        column_data_t::initialize_scan_with_offset(state, row_idx);

        assert(state.child_states.size() == 2);
        validity.initialize_scan_with_offset(state.child_states[0], row_idx);

        uint64_t child_offset = 0;
        if (row_idx != start_) {
            auto fetched = fetch_list_offset(row_idx - 1);
            if (fetched.has_error()) {
                state.scan_error = fetched.error();
                return;
            }
            child_offset = fetched.value();
        }
        assert(child_offset <= child_column->max_entry());
        if (child_offset < child_column->max_entry()) {
            child_column->initialize_scan_with_offset(state.child_states[1],
                                                      start_ + static_cast<int64_t>(child_offset));
        }
        state.last_offset = child_offset;
    }

    uint64_t list_column_data_t::scan(uint64_t, column_scan_state& state, vector::vector_t& result, uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t list_column_data_t::scan_committed(uint64_t,
                                                column_scan_state& state,
                                                vector::vector_t& result,
                                                bool,
                                                uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t list_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        assert(!updates_);

        // set state.result_offset to 0, so scan won`t go out of bounds
        auto prev_state_result_offset = state.result_offset;
        state.result_offset = 0;
        vector::vector_t offset_vector(result.resource(), types::logical_type::UBIGINT, count);
        uint64_t scan_count = scan_vector(state, offset_vector, count, scan_vector_type::SCAN_FLAT_VECTOR);
        assert(scan_count > 0);
        // The validity child writes into `result` (not the local offset_vector), so it targets
        // the REAL result base — the offset saved above, not the temporary 0. Without the sync
        // a scan spanning multiple vectors into one growing chunk folded whole-list NULL bits
        // to offset 0 (see standard_column_data_t::scan).
        state.child_states[0].result_offset = prev_state_result_offset;
        validity.scan_count(state.child_states[0], result, count);
        state.result_offset = prev_state_result_offset;

        vector::unified_vector_format offsets(result.resource(), result.size());
        offset_vector.to_unified_format(scan_count, offsets);
        auto data = offsets.get_data<uint64_t>();

        auto result_data = result.data<types::list_entry_t>();
        auto last_entry = result_data[state.result_offset == 0 ? 0 : state.result_offset - 1];
        auto base_offset = state.last_offset;
        uint64_t current_offset = 0;
        for (uint64_t i = 0; i < scan_count; i++) {
            auto offset_index = offsets.referenced_indexing->get_index(i);
            result_data[i + state.result_offset].offset = current_offset + last_entry.offset + last_entry.length;
            result_data[i + state.result_offset].length = data[offset_index] - current_offset;
            current_offset += result_data[i + state.result_offset].length;
        }

        uint64_t child_scan_count = current_offset;
        result.reserve(child_scan_count + base_offset);

        auto prev_size = result.size();
        if (child_scan_count > 0) {
            auto& child_entry = result.entry();
            if (child_entry.type().to_physical_type() != types::physical_type::STRUCT &&
                child_entry.type().to_physical_type() != types::physical_type::ARRAY &&
                static_cast<uint64_t>(state.child_states[1].row_index) + child_scan_count >
                    static_cast<uint64_t>(child_column->start()) + child_column->max_entry()) {
                // The cumulative offsets read above come off THIS column's own segments, i.e.
                // off disk, so a run that reaches past the element column's end is a corrupt
                // offset stream — not a program error. It reports on the SAME channel every
                // other read failure in this layer uses: the scan state's scan_error, which
                // row_group_t aggregates into collection_scan_state::scan_error and every scan
                // loop bails on. The throw that stood here unwound into the disk agent's
                // coroutine, whose unhandled_exception() is empty, so the statement HUNG
                // instead of failing (rules 2/9).
                state.scan_error = core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string("list column scan: a stored list offset runs past the end of the element column",
                                     resource_));
                // Nothing here may be trusted: the entries written into `result` above were
                // derived from the very offsets this guard rejects.
                return 0;
            }
            state.child_states[1].result_offset = prev_size;
            result.reserve(prev_size + child_scan_count);
            // scan_count (not scan_count_with_updates) so the child's validity sub-column is restored:
            // a NULL list element must survive the round trip. Mirrors the ARRAY child scan.
            child_column->scan_count(state.child_states[1], child_entry, child_scan_count);
        }
        state.last_offset = current_offset;

        // The validity bitmap and every element were read on child states; row_group_t judges
        // only this one.
        state.collect_child_errors();
        result.set_list_size(child_scan_count + prev_size);
        return scan_count;
    }

    void list_column_data_t::skip(column_scan_state& state, uint64_t count) {
        validity.skip(state.child_states[0], count);

        vector::vector_t offset_vector(resource_, types::logical_type::UBIGINT, count);
        auto prev_offset = state.result_offset;
        state.result_offset = 0;
        uint64_t scan_count = scan_vector(state, offset_vector, count, scan_vector_type::SCAN_FLAT_VECTOR);
        state.result_offset = prev_offset;
        assert(scan_count > 0);

        vector::unified_vector_format offsets(resource_, count);
        offset_vector.to_unified_format(scan_count, offsets);
        auto data = offsets.get_data<uint64_t>();
        auto last_entry = data[offsets.referenced_indexing->get_index(scan_count - 1)];
        uint64_t child_scan_count = last_entry - state.last_offset;
        if (child_scan_count == 0) {
            return;
        }
        state.last_offset = last_entry;

        child_column->skip(state.child_states[1], child_scan_count);
    }

    core::result_wrapper_t<bool> list_column_data_t::initialize_append(column_append_state& state) {
        auto base = column_data_t::initialize_append(state);
        if (base.has_error()) {
            return base; // out_of_memory (rules 2/9)
        }

        column_append_state validity_append_state;
        auto v = validity.initialize_append(validity_append_state);
        if (v.has_error()) {
            return v;
        }
        state.child_appends.push_back(std::move(validity_append_state));

        column_append_state child_append_state;
        auto child = child_column->initialize_append(child_append_state);
        if (child.has_error()) {
            return child;
        }
        state.child_appends.push_back(std::move(child_append_state));
        return true;
    }

    core::result_wrapper_t<bool>
    list_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        assert(count > 0);
        vector::unified_vector_format list_data(vector.resource(), count);
        vector.to_unified_format(count, list_data);
        auto& list_validity = list_data.validity;

        auto input_offsets = list_data.get_data<types::list_entry_t>();
        auto start_offset = child_column->max_entry();
        uint64_t child_count = 0;

        vector::validity_mask_t append_mask(resource_, count);
        auto append_offsets = std::unique_ptr<uint64_t[]>(new uint64_t[count]);
        bool child_contiguous = true;
        for (uint64_t i = 0; i < count; i++) {
            auto input_idx = list_data.referenced_indexing->get_index(i);
            if (list_validity.row_is_valid(input_idx)) {
                auto& input_list = input_offsets[input_idx];
                if (input_list.offset != child_count) {
                    child_contiguous = false;
                }
                append_offsets[i] = start_offset + child_count + input_list.length;
                child_count += input_list.length;
            } else {
                append_mask.set_invalid(i);
                append_offsets[i] = start_offset + child_count;
            }
        }
        auto& list_child = vector.entry();
        vector::vector_t child_vector(list_child);
        if (!child_contiguous) {
            vector::indexing_vector_t child_indexing(resource_, child_count);
            uint64_t current_count = 0;
            for (uint64_t i = 0; i < count; i++) {
                auto input_idx = list_data.referenced_indexing->get_index(i);
                if (list_validity.row_is_valid(input_idx)) {
                    auto& input_list = input_offsets[input_idx];
                    for (uint64_t list_idx = 0; list_idx < input_list.length; list_idx++) {
                        child_indexing.set_index(current_count++, input_list.offset + list_idx);
                    }
                }
            }
            assert(current_count == child_count);
            child_vector.slice(list_child, child_indexing, child_count);
        }

        vector::unified_vector_format uvf(resource_, vector.size());
        uvf.referenced_indexing = vector::incremental_indexing_vector(vector.resource());
        uvf.data = reinterpret_cast<std::byte*>(append_offsets.get());

        if (child_count > 0) {
            auto child = child_column->append(state.child_appends[1], child_vector, child_count);
            if (child.has_error()) {
                return child; // out_of_memory (rules 2/9)
            }
        }
        auto base = column_data_t::append_data(state, uvf, count);
        if (base.has_error()) {
            return base;
        }
        uvf.validity = append_mask;
        return validity.append_data(state.child_appends[0], uvf, count);
    }

    core::result_wrapper_t<bool> list_column_data_t::revert_append(int64_t start_row) {
        auto own = column_data_t::revert_append(start_row);
        if (own.has_error()) {
            return own;
        }
        auto v = validity.revert_append(start_row);
        if (v.has_error()) {
            return v;
        }
        // start_row is COLLECTION-ABSOLUTE (see column_data_t::revert_append). The stored
        // offsets are cumulative ELEMENT counts within this row group (append seeds them
        // from child_column->max_entry()), and the child column shares this column's
        // start_, so the child's absolute truncation row is start_ + <end offset of the
        // last surviving entry> — 0 elements survive when the whole group is reverted.
        // The old guard compared the RELATIVE surviving count against the ABSOLUTE start_,
        // so for any row group with start_ > 0 (and for a full revert in group 0) the
        // child was never truncated and the next append's offsets desynced from its data.
        uint64_t child_offset = 0;
        if (start_row > start_) {
            auto fetched = fetch_list_offset(start_row - 1);
            if (fetched.has_error()) {
                // Truncating the child to a GUESSED offset is the desync this function
                // exists to prevent; report instead (rule 6).
                return fetched.convert_error<bool>();
            }
            child_offset = fetched.value();
        }
        return child_column->revert_append(start_ + static_cast<int64_t>(child_offset));
    }

    uint64_t list_column_data_t::fetch(column_scan_state& state, int64_t, vector::vector_t&) {
        // POINT FETCH OF A WHOLE LIST CELL IS NOT IMPLEMENTED, and this override exists to say
        // so rather than to be filled in. Deleting it would be worse than leaving it: the base
        // column_data_t::fetch would scan this node's OWN segments, which hold the cumulative
        // ELEMENT OFFSETS, into a LIST-typed result — silently answering with offsets where the
        // caller asked for lists.
        //
        // Nothing calls it. column_data_t::fetch has exactly two call sites: column_data_t::update
        // (on `this`) and struct_column_data_t::fetch (on a field). LIST, ARRAY and STRUCT all
        // override BOTH update and update_column, so column_data_t::update is never entered with a
        // nested node as `this`; struct_column_data_t::fetch therefore has no caller either, and
        // neither has this. No SQL statement names the path: whole-list reads go through
        // scan_count, and the in-place LIST update builds its pre-image per element in
        // gather_child_update instead of fetching the cell.
        //
        // The refusal travels on the channel the ONE potential caller already reads:
        // column_data_t::update checks state.has_error() right after fetch() and returns
        // state.scan_error. A throw here would unwind into the disk agent's coroutine, whose
        // unhandled_exception() is empty — a hang, not an error (rules 2/9).
        state.scan_error =
            core::error_t(core::error_code_t::unimplemented_yet,
                          std::pmr::string("point fetch of a whole LIST cell is not implemented", resource_));
        return 0;
    }

    core::result_wrapper_t<std::pmr::vector<int64_t>>
    list_column_data_t::gather_child_update(vector::vector_t& update_vector,
                                            int64_t* row_ids,
                                            uint64_t update_count,
                                            vector::vector_t& child_update_out) {
        update_vector.flatten(update_count);
        const auto* update_entries = update_vector.data<types::list_entry_t>();
        auto& update_validity = update_vector.validity();

        std::pmr::vector<int64_t> child_ids(resource_);
        uint64_t total = 0;
        for (uint64_t r = 0; r < update_count; ++r) {
            const auto row_id = row_ids[r];
            uint64_t start_offset = 0;
            if (row_id != start_) {
                auto so = fetch_list_offset(row_id - 1);
                if (so.has_error()) {
                    return so.convert_error<std::pmr::vector<int64_t>>();
                }
                start_offset = so.value();
            }
            auto eo = fetch_list_offset(row_id);
            if (eo.has_error()) {
                return eo.convert_error<std::pmr::vector<int64_t>>();
            }
            const auto stored_length = eo.value() - start_offset;
            const auto new_length = update_validity.row_is_valid(r) ? update_entries[r].length : 0;
            if (new_length != stored_length) {
                // In-place update writes each element over the element the row already owns, so
                // it cannot move the row's neighbours to make room. A length change is a real
                // statement-level refusal, not an internal impossibility: this path is the WAL
                // REPLAY leg of an update (table_storage_adapter_t::update(row_ids, data)), so
                // the offending length arrives from a journal on disk. It rides the
                // result_wrapper_t<bool> both callers below already return, up through
                // row_group_t::update -> collection_t::update -> data_table_t::update. The throw
                // that stood here crossed the disk agent's mailbox boundary and unwound into a
                // coroutine with an empty unhandled_exception() — a hang, not a refusal
                // (rules 2/9).
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string("in-place LIST update cannot change a row's list length",
                                                      resource_));
            }
            total += stored_length;
        }

        child_ids.reserve(total);
        child_update_out = vector::vector_t(resource_, type_.child_type(), total == 0 ? 1 : total);
        auto& update_child = update_vector.entry();
        uint64_t k = 0;
        for (uint64_t r = 0; r < update_count; ++r) {
            const auto row_id = row_ids[r];
            uint64_t start_offset = 0;
            if (row_id != start_) {
                auto so = fetch_list_offset(row_id - 1);
                if (so.has_error()) {
                    return so.convert_error<std::pmr::vector<int64_t>>();
                }
                start_offset = so.value();
            }
            const auto length = update_entries[r].length;
            for (uint64_t j = 0; j < length; ++j) {
                child_update_out.set_value(k, update_child.value(update_entries[r].offset + j));
                child_ids.push_back(start_ + static_cast<int64_t>(start_offset + j));
                ++k;
            }
        }
        return child_ids;
    }

    core::result_wrapper_t<bool> list_column_data_t::update(uint64_t column_index,
                                                            vector::vector_t& update_vector,
                                                            int64_t* row_ids,
                                                            uint64_t update_count) {
        if (update_count == 0) {
            return true;
        }
        vector::vector_t child_update(resource_, type_.child_type());
        VALUE_OR_RETURN(auto child_ids, gather_child_update(update_vector, row_ids, update_count, child_update));
        // One child update per element run inside ONE update window, with the gathered element
        // vector SLICED to the run: update_segment_t::update addresses its update vector by
        // position within the call, so passing the WHOLE gathered vector with ids from a later
        // window read the wrong slice (see array_column_data_t::update for the shared story).
        const uint64_t total = child_ids.size();
        const int64_t child_start = child_column->start();
        const int64_t cap = static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t pos = 0;
        while (pos < total) {
            const uint64_t run_start = pos;
            const int64_t window = (child_ids[pos] - child_start) / cap;
            for (pos++; pos < total && (child_ids[pos] - child_start) / cap == window; pos++) {
            }
            const uint64_t run = pos - run_start;
            vector::vector_t window_slice(child_update, run_start, run);
            window_slice.flatten(run);
            auto child = child_column->update(column_index, window_slice, child_ids.data() + run_start, run);
            if (child.has_error()) {
                return child;
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> list_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                                   vector::vector_t& update_vector,
                                                                   int64_t* row_ids,
                                                                   uint64_t update_count,
                                                                   uint64_t depth) {
        if (update_count == 0) {
            return true;
        }
        vector::vector_t child_update(resource_, type_.child_type());
        VALUE_OR_RETURN(auto child_ids, gather_child_update(update_vector, row_ids, update_count, child_update));
        // Same window-run walk as update() above.
        const uint64_t total = child_ids.size();
        const int64_t child_start = child_column->start();
        const int64_t cap = static_cast<int64_t>(vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t pos = 0;
        while (pos < total) {
            const uint64_t run_start = pos;
            const int64_t window = (child_ids[pos] - child_start) / cap;
            for (pos++; pos < total && (child_ids[pos] - child_start) / cap == window; pos++) {
            }
            const uint64_t run = pos - run_start;
            vector::vector_t window_slice(child_update, run_start, run);
            window_slice.flatten(run);
            auto child =
                child_column->update_column(column_path, window_slice, child_ids.data() + run_start, run, depth);
            if (child.has_error()) {
                return child;
            }
        }
        return true;
    }

    void list_column_data_t::fetch_row(column_fetch_state& state,
                                       int64_t row_id,
                                       vector::vector_t& result,
                                       uint64_t result_idx) {
        uint64_t start_offset = 0;
        if (row_id != start_) {
            auto so = fetch_list_offset(row_id - 1);
            if (so.has_error()) {
                state.fetch_error = so.error();
                return;
            }
            start_offset = so.value();
        }
        auto eo = fetch_list_offset(row_id);
        if (eo.has_error()) {
            state.fetch_error = eo.error();
            return;
        }
        auto end_offset = eo.value();
        // state.child(0), not a default-constructed state: the validity bitmap's pin OOM used to
        // land in a child nobody read (see column_fetch_state::child).
        auto& validity_state = state.child(0);
        validity.fetch_row(validity_state, row_id, result, result_idx);
        if (state.absorb_error(validity_state)) {
            // The lengths below are read off a mask this call failed to fill; stop instead.
            return;
        }

        auto& res_validity = result.validity();
        auto list_data = result.data<types::list_entry_t>();
        auto& list_entry = list_data[result_idx];
        list_entry.offset = result.size();
        list_entry.length = end_offset - start_offset;
        if (!res_validity.row_is_valid(result_idx)) {
            assert(list_entry.length == 0);
            return;
        }

        auto child_scan_count = list_entry.length;
        if (child_scan_count > 0) {
            auto child_state = std::make_unique<column_scan_state>();
            auto& child_type = result.type().child_type();
            vector::vector_t child_scan(result.resource(), child_type, child_scan_count);
            child_state->initialize(child_type);
            child_column->initialize_scan_with_offset(*child_state, start_ + static_cast<int64_t>(start_offset));
            assert(child_type.to_physical_type() == types::physical_type::STRUCT ||
                   static_cast<uint64_t>(child_state->row_index) + child_scan_count - static_cast<uint64_t>(start_) <=
                       child_column->max_entry());
            // scan_count (validity-aware) so NULL list elements survive a point fetch too.
            child_column->scan_count(*child_state, child_scan, child_scan_count);
            // The elements are read on a SCAN state (the bulk leg owns its strings, so the
            // pins half of the channel does not apply here) — but its scan_error is still an
            // error of THIS fetch, and it was the only place a corrupt element was reported.
            child_state->collect_child_errors();
            if (child_state->has_error()) {
                if (!state.fetch_error.contains_error()) {
                    state.fetch_error = child_state->scan_error;
                }
                return;
            }

            result.append(child_scan, child_scan_count);
        }
    }

    void list_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                     std::vector<uint64_t> col_path,
                                                     std::vector<column_segment_info>& result) {
        column_data_t::get_column_segment_info(row_group_index, col_path, result);
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, col_path, result);
        col_path.back() = 1;
        child_column->get_column_segment_info(row_group_index, col_path, result);
    }

    core::result_wrapper_t<bool>
    list_column_data_t::checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                                            persistent_column_data_t& persistent) {
        // v1 convention: child_columns[0] is the list's own validity bitmap (whole-cell
        // NULLs), child_columns[1] the element column (whose own record carries the
        // element-level validity in turn).
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
    list_column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        // Own segments hold the per-row cumulative child offsets; the validity bitmap is the
        // persisted child_columns[0], the element column child_columns[1]. Any other shape is
        // data_corruption — never "assume all-valid".
        auto own = column_data_t::initialize_column(persistent_data);
        if (own.has_error()) {
            return own;
        }
        if (persistent_data.child_columns.size() != 2) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("list column load: checkpoint must carry validity + the element child", resource()));
        }
        auto valid = validity.initialize_column(*persistent_data.child_columns[0]);
        if (valid.has_error()) {
            return valid;
        }
        if (validity.count() != count()) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("list column load: validity row count does not match the column", resource()));
        }
        return child_column->initialize_column(*persistent_data.child_columns[1]);
    }

    void list_column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        // The base walk covers the own offsets segments; a reloaded list column additionally
        // owns its validity bitmap and its element column, each sitting on the blocks
        // initialize_column registered. Before F6 this override did not exist and compact
        // leaked both children (only offset blocks — and whatever the B2 packer happened to
        // co-locate with them — were reclaimed).
        column_data_t::collect_disk_block_ids(out);
        validity.collect_disk_block_ids(out);
        child_column->collect_disk_block_ids(out);
    }

} // namespace components::table