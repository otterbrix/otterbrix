#include "struct_column_data.hpp"

#include "persistent_column_data.hpp"
#include "row_group.hpp"

namespace components::table {

    struct_column_data_t::struct_column_data_t(std::pmr::memory_resource* resource,
                                               storage::block_manager_t& block_manager,
                                               uint64_t column_index,
                                               int64_t start_row,
                                               types::complex_logical_type type,
                                               column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , validity(resource, block_manager, 0, start_row, *this) {
        assert(type_.to_physical_type() == types::physical_type::STRUCT);
        auto& child_types = type_.child_types();
        assert(!child_types.empty());
        // The "a table cannot be created from an unnamed struct" precondition used to throw HERE,
        // where nothing can refuse: a constructor has no return value, the object graph is half
        // built, and the throw unwound across the disk agent's mailbox into a coroutine with an
        // empty unhandled_exception() — a hang, not a refusal (rules 2/9). It now lives in
        // column_data_t::validate_column_type, asked BEFORE any node is built at the one write
        // gate that already owns an error channel (collection_t::initialize_append). Same rule,
        // same UNION exemption, one canonical statement of it.
        uint64_t sub_column_index = 1;
        for (auto& child_type : child_types) {
            sub_columns.push_back(
                create_column(resource, block_manager, sub_column_index, start_row, child_type, this));
            sub_column_index++;
        }
    }

    void struct_column_data_t::set_start(int64_t new_start) {
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

    void struct_column_data_t::initialize_scan_with_offset(column_scan_state& state, int64_t row_idx) {
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
        // Validity and every field write at the parent's result base. Without the sync a scan
        // spanning multiple vectors into one growing chunk folded all NULL bits to offset 0
        // (see standard_column_data_t::scan).
        state.child_states[0].result_offset = state.result_offset;
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
        // Every byte came off a child state; row_group_t only ever looks at THIS one.
        state.collect_child_errors();
        return scan_count;
    }

    uint64_t struct_column_data_t::scan_committed(uint64_t vector_index,
                                                  column_scan_state& state,
                                                  vector::vector_t& result,
                                                  bool allow_updates,
                                                  uint64_t target_count) {
        state.child_states[0].result_offset = state.result_offset; // see scan(): children target the same base
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
            state.child_states[i + 1].result_offset = state.result_offset;
            sub_columns[i]->scan_committed(vector_index,
                                           state.child_states[i + 1],
                                           target_vector,
                                           allow_updates,
                                           target_count);
        }
        state.collect_child_errors(); // see scan()
        return scan_count;
    }

    uint64_t struct_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        state.child_states[0].result_offset = state.result_offset; // see scan(): children target the same base
        auto scan_count = validity.scan_count(state.child_states[0], result, count);
        auto& child_entries = result.entries();
        for (uint64_t i = 0; i < sub_columns.size(); i++) {
            auto& target_vector = *child_entries[i];
            if (!state.scan_child_column[i]) {
                target_vector.set_vector_type(vector::vector_type::CONSTANT);
                target_vector.set_null(true);
                continue;
            }
            state.child_states[i + 1].result_offset = state.result_offset;
            sub_columns[i]->scan_count(state.child_states[i + 1], target_vector, count);
        }
        state.collect_child_errors(); // see scan()
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

    core::result_wrapper_t<bool> struct_column_data_t::initialize_append(column_append_state& state) {
        column_append_state validity_append;
        auto v = validity.initialize_append(validity_append);
        if (v.has_error()) {
            return v; // out_of_memory (rules 2/9)
        }
        state.child_appends.push_back(std::move(validity_append));

        for (auto& sub_column : sub_columns) {
            column_append_state child_append;
            auto child = sub_column->initialize_append(child_append);
            if (child.has_error()) {
                return child;
            }
            state.child_appends.push_back(std::move(child_append));
        }
        return true;
    }

    core::result_wrapper_t<bool>
    struct_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        if (vector.get_vector_type() != vector::vector_type::FLAT) {
            vector::vector_t append_vector(vector);
            append_vector.flatten(count);
            return append(state, append_vector, count);
        }

        auto v = validity.append(state.child_appends[0], vector, count);
        if (v.has_error()) {
            return v; // out_of_memory (rules 2/9)
        }

        auto& child_entries = vector.entries();
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            auto child = sub_columns[i]->append(state.child_appends[i + 1], *child_entries[i], count);
            if (child.has_error()) {
                return child;
            }
        }
        count_ += count;
        return true;
    }

    void struct_column_data_t::revert_append(int64_t start_row) {
        // start_row is COLLECTION-ABSOLUTE (see column_data_t::revert_append). Struct
        // children are row-aligned with the parent and share its start_, so — unlike the
        // LIST/ARRAY element-space children — the absolute row passes through unchanged.
        validity.revert_append(start_row);
        for (auto& sub_column : sub_columns) {
            sub_column->revert_append(start_row);
        }
        count_ = static_cast<uint64_t>(start_row - start_);
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
        state.collect_child_errors(); // the fields read on child states; column_data_t::update reads this one
        return scan_count;
    }

    core::result_wrapper_t<bool> struct_column_data_t::update(uint64_t column_index,
                                                              vector::vector_t& update_vector,
                                                              int64_t* row_ids,
                                                              uint64_t update_count) {
        auto v = validity.update(column_index, update_vector, row_ids, update_count);
        if (v.has_error()) {
            return v;
        }
        auto& child_entries = update_vector.entries();
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            auto child = sub_columns[i]->update(column_index, *child_entries[i], row_ids, update_count);
            if (child.has_error()) {
                return child;
            }
        }
        return true;
    }

    core::result_wrapper_t<bool> struct_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                                                     vector::vector_t& update_vector,
                                                                     int64_t* row_ids,
                                                                     uint64_t update_count,
                                                                     uint64_t depth) {
        if (depth >= column_path.size()) {
            // The path ran out ON a struct node, i.e. the caller asked to overwrite a whole
            // struct cell through the sub-column update path. There is nothing to write here: a
            // struct node owns no segments, every byte lives in a field. The path is supplied by
            // whoever called row_group_t::update_column, so this is a caller error and it belongs
            // on the result_wrapper_t<bool> this function already returns — the throw would have
            // crossed the disk agent's mailbox and unwound into a coroutine with an empty
            // unhandled_exception(), hanging the statement instead of failing it (rules 2/9).
            // (row_group_t::update_column has no caller today: collection_t::update_column hands
            // its path to row_group_t::UPDATE instead. Reported, not patched here.)
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string("struct column update: the column path ends on the struct itself; name a field",
                                 resource()));
        }
        auto update_column = column_path[depth];
        if (update_column == 0) {
            return validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        } else {
            if (update_column > sub_columns.size()) {
                // Same channel, same reason: the path names a field this struct does not have.
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string("struct column update: the column path names field " +
                                                          std::to_string(update_column) + " of a struct with " +
                                                          std::to_string(sub_columns.size()) + " fields",
                                                      resource()));
            }
            return sub_columns[update_column - 1]->update_column(column_path,
                                                                 update_vector,
                                                                 row_ids,
                                                                 update_count,
                                                                 depth + 1);
        }
    }

    void struct_column_data_t::fetch_row(column_fetch_state& state,
                                         int64_t row_id,
                                         vector::vector_t& result,
                                         uint64_t result_idx) {
        auto& child_entries = result.entries();
        // state.child(i), NOT a default-constructed column_fetch_state per child. A struct node
        // owns no segments: every byte of this cell is read on a CHILD's state, so the child is
        // the one that must know result_outlives_pins (or a big string in a field goes into the
        // caller's chunk as a view into a pin that dies with `state`) and the child is the one
        // whose fetch_error nobody above was reading.
        auto& validity_state = state.child(0);
        validity.fetch_row(validity_state, row_id, result, result_idx);
        if (state.absorb_error(validity_state)) {
            // First error aborts, as in row_group_t's gather: the remaining fields would only
            // add cells nobody may trust to a chunk the caller must discard anyway.
            return;
        }
        for (uint64_t i = 0; i < child_entries.size(); i++) {
            auto& field_state = state.child(i + 1);
            sub_columns[i]->fetch_row(field_state, row_id, *child_entries[i], result_idx);
            // The field absorbed its OWN children before returning, so this one check answers
            // for every level below it -- a string two structs down included.
            if (state.absorb_error(field_state)) {
                return;
            }
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

    core::result_wrapper_t<bool>
    struct_column_data_t::checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                                              persistent_column_data_t& persistent) {
        // v1 convention: child_columns[0] is the struct's own validity bitmap (the whole-cell
        // NULLs), then one child per field — the same order initialize_column consumes.
        auto valid = validity.checkpoint(partial_block_manager);
        if (valid.has_error()) {
            return valid.convert_error<bool>(); // out_of_memory
        }
        persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(std::move(valid.value())));
        for (auto& sub_column : sub_columns) {
            auto child = sub_column->checkpoint(partial_block_manager);
            if (child.has_error()) {
                return child.convert_error<bool>(); // out_of_memory
            }
            persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(std::move(child.value())));
        }
        return true;
    }

    core::result_wrapper_t<bool>
    struct_column_data_t::initialize_column(const persistent_column_data_t& persistent_data) {
        // A struct node owns no segments of its own: its row count comes from the persisted
        // count, its validity is the persisted child_columns[0] bitmap (whole-cell NULLs),
        // and every field lives in a persisted sub-column after it. A record with the wrong
        // child count is data_corruption — the shapes are fixed by the checkpoint writer.
        count_ = persistent_data.count;
        if (persistent_data.child_columns.size() != sub_columns.size() + 1) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("struct column load: checkpoint must carry validity + one child per field",
                                 resource()));
        }
        auto valid = validity.initialize_column(*persistent_data.child_columns[0]);
        if (valid.has_error()) {
            return valid;
        }
        if (validity.count() != count_) {
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string("struct column load: validity row count does not match the column", resource()));
        }
        for (size_t i = 0; i < sub_columns.size(); i++) {
            auto field = sub_columns[i]->initialize_column(*persistent_data.child_columns[i + 1]);
            if (field.has_error()) {
                return field;
            }
        }
        return true;
    }

    void struct_column_data_t::collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const {
        // The base walk of the own data_ tree finds nothing (a struct node keeps no segments,
        // see initialize_column above); it is kept so every node reports through one path.
        // What a reloaded struct column actually owns is its children: the validity bitmap and
        // one sub-column per field, each sitting on the blocks initialize_column registered.
        // Before F6 this override did not exist and compact leaked all of them.
        column_data_t::collect_disk_block_ids(out);
        validity.collect_disk_block_ids(out);
        for (const auto& sub_column : sub_columns) {
            sub_column->collect_disk_block_ids(out);
        }
    }

} // namespace components::table