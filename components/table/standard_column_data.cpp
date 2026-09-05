#include "standard_column_data.hpp"

#include "persistent_column_data.hpp"

namespace components::table {

    standard_column_data_t::standard_column_data_t(std::pmr::memory_resource* resource,
                                                   storage::block_manager_t& block_manager,
                                                   uint64_t column_index,
                                                   int64_t start_row,
                                                   types::complex_logical_type type,
                                                   column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , validity(resource, block_manager, 0, start_row, *this) {}

    void standard_column_data_t::set_start(int64_t new_start) {
        column_data_t::set_start(new_start);
        validity.set_start(new_start);
    }

    scan_vector_type standard_column_data_t::get_vector_scan_type(column_scan_state& state,
                                                                  uint64_t scan_count,
                                                                  vector::vector_t& result) {
        auto scan_type = column_data_t::get_vector_scan_type(state, scan_count, result);
        if (scan_type == scan_vector_type::SCAN_FLAT_VECTOR) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        if (state.child_states.empty()) {
            return scan_type;
        }
        return validity.get_vector_scan_type(state.child_states[0], scan_count, result);
    }

    void standard_column_data_t::initialize_scan(column_scan_state& state) {
        column_data_t::initialize_scan(state);

        assert(state.child_states.size() == 1);
        validity.initialize_scan(state.child_states[0]);
    }

    void standard_column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
        column_data_t::initialize_scan_with_offset(state, row_idx);

        assert(state.child_states.size() == 1);
        validity.initialize_scan_with_offset(state.child_states[0], row_idx);
    }

    uint64_t standard_column_data_t::scan(uint64_t vector_index,
                                          column_scan_state& state,
                                          vector::vector_t& result,
                                          uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);
        // The validity child writes into the SAME result vector as the main data, so its scan
        // state must target the same result base. Without this sync a scan that spans multiple
        // vectors into one growing chunk (collection_scan_state::scan — compact's rebuild) wrote
        // every vector's NULL bits at offset 0, folding the whole table's NULL pattern into the
        // first 1024 rows (and reading every later row as non-NULL) while the data landed right.
        state.child_states[0].result_offset = state.result_offset;
        auto scan_count = column_data_t::scan(vector_index, state, result, target_count);
        validity.scan(vector_index, state.child_states[0], result, target_count);
        // The validity child reads on its own state; row_group_t only judges this one.
        state.collect_child_errors();
        return scan_count;
    }

    uint64_t standard_column_data_t::scan_committed(uint64_t vector_index,
                                                    column_scan_state& state,
                                                    vector::vector_t& result,
                                                    bool allow_updates,
                                                    uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);
        state.child_states[0].result_offset = state.result_offset; // see scan(): validity targets the same base
        auto scan_count = column_data_t::scan_committed(vector_index, state, result, allow_updates, target_count);
        validity.scan_committed(vector_index, state.child_states[0], result, allow_updates, target_count);
        state.collect_child_errors(); // see scan()
        return scan_count;
    }

    uint64_t standard_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        state.child_states[0].result_offset = state.result_offset; // see scan(): validity targets the same base
        auto scan_count = column_data_t::scan_count(state, result, count);
        validity.scan_count(state.child_states[0], result, count);
        state.collect_child_errors(); // see scan()
        return scan_count;
    }

    core::result_wrapper_t<bool> standard_column_data_t::initialize_append(column_append_state& state) {
        auto base = column_data_t::initialize_append(state);
        if (base.has_error()) {
            return base; // out_of_memory (rules 2/9)
        }
        column_append_state child_append;
        auto child = validity.initialize_append(child_append);
        if (child.has_error()) {
            return child;
        }
        state.child_appends.push_back(std::move(child_append));
        return true;
    }

    core::result_wrapper_t<bool> standard_column_data_t::append_data(column_append_state& state,
                                                                     vector::unified_vector_format& uvf,
                                                                     uint64_t count) {
        auto base = column_data_t::append_data(state, uvf, count);
        if (base.has_error()) {
            return base; // out_of_memory (rules 2/9)
        }
        return validity.append_data(state.child_appends[0], uvf, count);
    }

    core::result_wrapper_t<bool> standard_column_data_t::revert_append(int64_t start_row) {
        auto own = column_data_t::revert_append(start_row);
        if (own.has_error()) {
            return own;
        }
        return validity.revert_append(start_row);
    }

    core::result_wrapper_t<bool> standard_column_data_t::transition_to_disk(storage::partial_block_manager_t& pbm) {
        // Pack BOTH the main data segments and the validity child's segments through the SAME `pbm`, so a
        // narrow column and its validity bitmap can share blocks. The caller flushes `pbm` once.
        auto base = column_data_t::transition_to_disk(pbm);
        if (base.has_error()) {
            return base; // io_error / out_of_memory (rules 2/9)
        }
        return validity.transition_to_disk(pbm);
    }

    void standard_column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        column_data_t::collect_disk_block_ids(out);
        validity.collect_disk_block_ids(out);
    }

    uint64_t standard_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        if (state.child_states.empty()) {
            column_scan_state child_state;
            state.child_states.push_back(std::move(child_state));
        }
        auto scan_count = column_data_t::fetch(state, row_id, result);
        validity.fetch(state.child_states[0], row_id, result);
        state.collect_child_errors(); // column_data_t::update reads state.scan_error after this
        return scan_count;
    }

    core::result_wrapper_t<bool> standard_column_data_t::update(uint64_t column_index,
                                                                vector::vector_t& update_vector,
                                                                int64_t* row_ids,
                                                                uint64_t update_count) {
        auto base = column_data_t::update(column_index, update_vector, row_ids, update_count);
        if (base.has_error()) {
            return base;
        }
        return validity.update(column_index, update_vector, row_ids, update_count);
    }

    core::result_wrapper_t<bool> standard_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                                       vector::vector_t& update_vector,
                                                                       int64_t* row_ids,
                                                                       uint64_t update_count,
                                                                       uint64_t depth) {
        if (depth >= column_path.size()) {
            // The FULL update — this class's own override — never the base leg directly: the
            // base updates only the data column, and a value written over a backfilled-NULL
            // row without its validity bit stays invisible. Dormant until
            // collection_t::update_column started routing paths here (entry #16); the ALTER
            // extension test read NULL where it wrote a value.
            return update(column_path[0], update_vector, row_ids, update_count);
        } else {
            return validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        }
    }

    void standard_column_data_t::fetch_row(column_fetch_state& state,
                                           int64_t row_id,
                                           vector::vector_t& result,
                                           uint64_t result_idx) {
        // state.child(0), not a default-constructed state: the validity bitmap's own pin OOM
        // used to be recorded in a child nobody read (see column_fetch_state::child).
        auto& validity_state = state.child(0);
        validity.fetch_row(validity_state, row_id, result, result_idx);
        if (state.absorb_error(validity_state)) {
            return; // reading the value under a NULL mask we could not read answers nothing
        }
        column_data_t::fetch_row(state, row_id, result, result_idx);
    }

    void standard_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                         std::vector<uint64_t> col_path,
                                                         std::vector<column_segment_info>& result) {
        column_data_t::get_column_segment_info(row_group_index, col_path, result);
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, std::move(col_path), result);
    }

    core::result_wrapper_t<bool>
    standard_column_data_t::checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                                                persistent_column_data_t& persistent) {
        // The validity bitmap is persisted as the single child column (v1 convention:
        // child_columns[0] is validity). Its segments are flushed through the SAME
        // partial_block_manager as the main data, so the bitmap is packed into shared
        // blocks and lands disk-backed + reloadable on reload.
        auto child = validity.checkpoint(partial_block_manager);
        if (child.has_error()) {
            return child.convert_error<bool>(); // out_of_memory
        }
        persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(std::move(child.value())));
        return true;
    }

    core::result_wrapper_t<bool>
    standard_column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        auto own = column_data_t::initialize_column(persistent_data);
        if (own.has_error()) {
            return own;
        }
        // v1 checkpoints carry the REAL validity bitmap as the single child column. A missing
        // or mismatched record is data_corruption — never "assume all-valid": that silent
        // fallback is exactly the bug that lost every checkpointed NULL.
        if (persistent_data.child_columns.size() != 1) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("column load: checkpoint does not carry the validity child", resource()));
        }
        auto valid = validity.initialize_column(*persistent_data.child_columns[0]);
        if (valid.has_error()) {
            return valid;
        }
        if (validity.count() != count()) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("column load: validity row count does not match the column", resource()));
        }
        return true;
    }

} // namespace components::table