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
        auto scan_count = column_data_t::scan(vector_index, state, result, target_count);
        validity.scan(vector_index, state.child_states[0], result, target_count);
        return scan_count;
    }

    uint64_t standard_column_data_t::scan_committed(uint64_t vector_index,
                                                    column_scan_state& state,
                                                    vector::vector_t& result,
                                                    bool allow_updates,
                                                    uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);
        auto scan_count = column_data_t::scan_committed(vector_index, state, result, allow_updates, target_count);
        validity.scan_committed(vector_index, state.child_states[0], result, allow_updates, target_count);
        return scan_count;
    }

    uint64_t standard_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        auto scan_count = column_data_t::scan_count(state, result, count);
        validity.scan_count(state.child_states[0], result, count);
        return scan_count;
    }

    void standard_column_data_t::initialize_append(column_append_state& state) {
        column_data_t::initialize_append(state);
        column_append_state child_append;
        validity.initialize_append(child_append);
        state.child_appends.push_back(std::move(child_append));
    }

    void standard_column_data_t::append_data(column_append_state& state,
                                             vector::unified_vector_format& uvf,
                                             uint64_t count) {
        column_data_t::append_data(state, uvf, count);
        validity.append_data(state.child_appends[0], uvf, count);
    }

    void standard_column_data_t::revert_append(int64_t start_row) {
        column_data_t::revert_append(start_row);
        validity.revert_append(start_row);
    }

    uint64_t standard_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        if (state.child_states.empty()) {
            column_scan_state child_state;
            state.child_states.push_back(std::move(child_state));
        }
        auto scan_count = column_data_t::fetch(state, row_id, result);
        validity.fetch(state.child_states[0], row_id, result);
        return scan_count;
    }

    void standard_column_data_t::update(uint64_t column_index,
                                        vector::vector_t& update_vector,
                                        int64_t* row_ids,
                                        uint64_t update_count) {
        column_data_t::update(column_index, update_vector, row_ids, update_count);
        validity.update(column_index, update_vector, row_ids, update_count);
    }

    void standard_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                               vector::vector_t& update_vector,
                                               int64_t* row_ids,
                                               uint64_t update_count,
                                               uint64_t depth) {
        if (depth >= column_path.size()) {
            column_data_t::update(column_path[0], update_vector, row_ids, update_count);
        } else {
            validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        }
    }

    void standard_column_data_t::fetch_row(column_fetch_state& state,
                                           int64_t row_id,
                                           vector::vector_t& result,
                                           uint64_t result_idx) {
        if (state.child_states.empty()) {
            auto child_state = std::make_unique<column_fetch_state>();
            state.child_states.push_back(std::move(child_state));
        }
        validity.fetch_row(*state.child_states[0], row_id, result, result_idx);
        column_data_t::fetch_row(state, row_id, result, result_idx);
    }

    void standard_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                         std::vector<uint64_t> col_path,
                                                         std::vector<column_segment_info>& result) {
        column_data_t::get_column_segment_info(row_group_index, col_path, result);
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, std::move(col_path), result);
    }

    void standard_column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        column_data_t::initialize_column(persistent_data);

        // create matching transient validity segments for each data segment
        validity.initialize_column_validity(persistent_data);
    }

} // namespace components::table