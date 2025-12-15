#include "column_data.hpp"

#include <components/types/types.hpp>

#include "array_column_data.hpp"
#include "column_state.hpp"
#include "list_column_data.hpp"
#include "row_group.hpp"
#include "standard_column_data.hpp"
#include "storage/block_manager.hpp"
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
        , resource_(resource) {}

    filter_propagate_result_t column_data_t::check_zonemap(column_scan_state&, table_filter_t&) {
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

    void column_data_t::initialize_append(column_append_state& state) {
        auto l = data_.lock();
        if (data_.is_empty(l)) {
            apend_transient_segment(l, start_);
        }
        auto segment = data_.last_segment(l);
        state.current = segment;
        state.current->initialize_append(state);
    }

    void column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        vector::unified_vector_format uvf(vector.resource(), count);
        vector.to_unified_format(count, uvf);
        append_data(state, uvf, count);
    }

    void
    column_data_t::append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t append_count) {
        uint64_t offset = 0;
        this->count_ += append_count;
        while (true) {
            uint64_t copied_elements = state.current->append(state, uvf, offset, append_count);
            if (copied_elements == append_count) {
                break;
            }

            {
                auto l = data_.lock();
                apend_transient_segment(l, state.current->start + static_cast<int64_t>(state.current->count));
                state.current = data_.last_segment(l);
                state.current->initialize_append(state);
            }
            offset += copied_elements;
            append_count -= copied_elements;
        }
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

    bool column_data_t::check_predicate(int64_t row_id, const table_filter_t* filter) {
        auto segment = data_.get_segment(row_id);
        if (updates_ &&
            updates_->has_updates(static_cast<uint64_t>(row_id - start_) / vector::DEFAULT_VECTOR_CAPACITY)) {
            return updates_->check_row(row_id, filter);
        } else {
            return segment->check_predicate(row_id, filter);
        }
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

    void column_data_t::update(uint64_t column_index,
                               vector::vector_t& update_vector,
                               int64_t* row_ids,
                               uint64_t update_count) {
        vector::vector_t base_vector(resource_, type_, count_);
        column_scan_state state;
        auto fetch_count = fetch(state, row_ids[0], base_vector);

        base_vector.flatten(fetch_count);
        update_internal(column_index, update_vector, row_ids, update_count, base_vector);
    }

    void column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                      vector::vector_t& update_vector,
                                      int64_t* row_ids,
                                      uint64_t update_count,
                                      uint64_t depth) {
        assert(depth >= column_path.size());
        column_data_t::update(column_path[0], update_vector, row_ids, update_count);
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

    void column_data_t::apend_transient_segment(std::unique_lock<std::mutex>& l, int64_t start_row) {
        const auto block_size = block_manager_.block_size();
        const auto type_size = type_.size();
        auto vector_segment_size = block_size;

        if (start_row == static_cast<uint64_t>(MAX_ROW_ID)) {
            vector_segment_size = vector::DEFAULT_VECTOR_CAPACITY * type_size;
        }

        uint64_t segment_size = block_size < vector_segment_size ? block_size : vector_segment_size;
        allocation_size_ += segment_size;
        auto new_segment =
            column_segment_t::create_segment(block_manager_.buffer_manager, type_, start_row, segment_size, block_size);
        data_.append_segment(l, std::move(new_segment));
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

    void column_data_t::update_internal(uint64_t column_index,
                                        vector::vector_t& update_vector,
                                        int64_t* row_ids,
                                        uint64_t update_count,
                                        vector::vector_t& base_vector) {
        std::lock_guard update_guard(update_lock_);
        if (!updates_) {
            updates_ = std::make_unique<update_segment_t>(*this);
        }
        updates_->update(column_index, update_vector, row_ids, update_count, base_vector);
    }

    uint64_t column_data_t::vector_count(uint64_t vector_index) const {
        uint64_t current_row = vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        return std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY,
                                  static_cast<uint64_t>(start_) + count_ - current_row);
    }

} // namespace components::table