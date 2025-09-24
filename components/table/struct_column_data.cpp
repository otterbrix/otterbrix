#include "struct_column_data.hpp"

#include "row_group.hpp"

namespace components::table {

    struct_column_data_t::struct_column_data_t(std::pmr::memory_resource* resource,
                                               storage::block_manager_t& block_manager,
                                               uint64_t column_index,
                                               uint64_t start_row,
                                               types::complex_logical_type type,
                                               column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , validity(resource, block_manager, 0, start_row, *this) {
        assert(type_.to_physical_type() == types::physical_type::STRUCT);
        auto& child_types = type_.child_types();
        assert(!child_types.empty());
        if (type_.type() != types::logical_type::UNION && type_.is_unnamed()) {
            throw std::logic_error("A table cannot be created from an unnamed struct");
        }
        uint64_t sub_column_index = 1;
        for (auto& child_type : child_types) {
            sub_columns.push_back(
                create_column(resource, block_manager, sub_column_index, start_row, child_type, this));
            sub_column_index++;
        }
    }

    void struct_column_data_t::set_start(uint64_t new_start) {
        start_ = new_start;
        for (auto& sub_column : sub_columns) {
            sub_column->set_start(new_start);
        }
        validity.set_start(new_start);
    }

    uint64_t struct_column_data_t::max_entry() { return sub_columns[0]->max_entry(); }

    void struct_column_data_t::initialize_scan(column_scan_state& state) {
        assert(state.child_states.size() == sub_columns.size() + 1);
        state.row_index = 0;
        state.current = nullptr;

        validity.initialize_scan(state.child_states[0]);

        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            if (!state.scan_child_column[i]) {
                continue;
            }
            sub_columns[i]->initialize_scan(state.child_states[i + 1]);
        }
    }

    void struct_column_data_t::initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) {
        assert(state.child_states.size() == sub_columns.size() + 1);
        state.row_index = row_idx;
        state.current = nullptr;

        validity.initialize_scan_with_offset(state.child_states[0], row_idx);

        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            if (!state.scan_child_column[i]) {
                continue;
            }
            sub_columns[i]->initialize_scan_with_offset(state.child_states[i + 1], row_idx);
        }
    }

    uint64_t struct_column_data_t::scan(uint64_t vector_index,
                                        column_scan_state& state,
                                        vector::vector_t& result,
                                        uint64_t target_count) {
        auto scan_count = validity.scan(vector_index, state.child_states[0], result, target_count);
        auto& child_entries = result.entries();
        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            auto& target_vector = *child_entries[i];
            if (!state.scan_child_column[i]) {
                target_vector.set_vector_type(vector::vector_type::CONSTANT);
                target_vector.set_null(true);
                continue;
            }
            state.child_states[i + 1].result_offset = state.result_offset;
            sub_columns[i]->scan(vector_index, state.child_states[i + 1], target_vector, target_count);
        }
        return scan_count;
    }

    uint64_t struct_column_data_t::scan_committed(uint64_t vector_index,
                                                  column_scan_state& state,
                                                  vector::vector_t& result,
                                                  bool allow_updates,
                                                  uint64_t target_count) {
        auto scan_count =
            validity.scan_committed(vector_index, state.child_states[0], result, allow_updates, target_count);
        auto& child_entries = result.entries();
        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            auto& target_vector = *child_entries[i];
            if (!state.scan_child_column[i]) {
                target_vector.set_vector_type(vector::vector_type::CONSTANT);
                target_vector.set_null(true);
                continue;
            }
            sub_columns[i]->scan_committed(vector_index,
                                           state.child_states[i + 1],
                                           target_vector,
                                           allow_updates,
                                           target_count);
        }
        return scan_count;
    }

    uint64_t struct_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        auto scan_count = validity.scan_count(state.child_states[0], result, count);
        auto& child_entries = result.entries();
        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            auto& target_vector = *child_entries[i];
            if (!state.scan_child_column[i]) {
                target_vector.set_vector_type(vector::vector_type::CONSTANT);
                target_vector.set_null(true);
                continue;
            }
            sub_columns[i]->scan_count(state.child_states[i + 1], target_vector, count);
        }
        return scan_count;
    }

    void struct_column_data_t::skip(column_scan_state& state, uint64_t count) {
        validity.skip(state.child_states[0], count);

        for (uint64_t child_idx = 0; child_idx < sub_columns.size(); child_idx++) {
            if (!state.scan_child_column[child_idx]) {
                continue;
            }
            sub_columns[child_idx]->skip(state.child_states[child_idx + 1], count);
        }
    }

    void struct_column_data_t::initialize_append(column_append_state& state) {
        column_append_state validity_append;
        validity.initialize_append(validity_append);
        state.child_appends.push_back(std::move(validity_append));

        for (auto& sub_column : sub_columns) {
            column_append_state child_append;
            sub_column->initialize_append(child_append);
            state.child_appends.push_back(std::move(child_append));
        }
    }

    void struct_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        if (vector.get_vector_type() != vector::vector_type::FLAT) {
            vector::vector_t append_vector(vector);
            append_vector.flatten(count);
            append(state, append_vector, count);
            return;
        }

        validity.append(state.child_appends[0], vector, count);

        auto& child_entries = vector.entries();
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            sub_columns[i]->append(state.child_appends[i + 1], *child_entries[i], count);
        }
        count_ += count;
    }

    void struct_column_data_t::revert_append(int64_t start_row) {
        validity.revert_append(start_row);
        for (auto& sub_column : sub_columns) {
            sub_column->revert_append(start_row);
        }
        count_ = static_cast<uint64_t>(start_row) - start_;
    }

    uint64_t struct_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        auto& child_entries = result.entries();
        for (uint64_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
            column_scan_state child_state;
            state.child_states.push_back(std::move(child_state));
        }
        uint64_t scan_count = validity.fetch(state.child_states[0], row_id, result);
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            sub_columns[i]->fetch(state.child_states[i + 1], row_id, *child_entries[i]);
        }
        return scan_count;
    }

    void struct_column_data_t::update(uint64_t column_index,
                                      vector::vector_t& update_vector,
                                      int64_t* row_ids,
                                      uint64_t update_count) {
        validity.update(column_index, update_vector, row_ids, update_count);
        auto& child_entries = update_vector.entries();
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            sub_columns[i]->update(column_index, *child_entries[i], row_ids, update_count);
        }
    }

    void struct_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                             vector::vector_t& update_vector,
                                             int64_t* row_ids,
                                             uint64_t update_count,
                                             uint64_t depth) {
        if (depth >= column_path.size()) {
            throw std::runtime_error("Attempting to directly update a struct column - this should not be possible");
        }
        auto update_column = column_path[depth];
        if (update_column == 0) {
            validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        } else {
            if (update_column > sub_columns.size()) {
                throw std::runtime_error("update column_path out of range");
            }
            sub_columns[update_column - 1]->update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        }
    }

    void struct_column_data_t::fetch_row(column_fetch_state& state,
                                         int64_t row_id,
                                         vector::vector_t& result,
                                         uint64_t result_idx) {
        auto& child_entries = result.entries();
        for (uint64_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
            auto child_state = std::make_unique<column_fetch_state>();
            state.child_states.push_back(std::move(child_state));
        }
        validity.fetch_row(*state.child_states[0], row_id, result, result_idx);
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            sub_columns[i]->fetch_row(*state.child_states[i + 1], row_id, *child_entries[i], result_idx);
        }
    }

    void struct_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                       std::vector<uint64_t> col_path,
                                                       std::vector<column_segment_info>& result) {
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, col_path, result);
        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            col_path.back() = i + 1;
            sub_columns[i]->get_column_segment_info(row_group_index, col_path, result);
        }
    }

} // namespace components::table