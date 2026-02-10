#include "collection.hpp"

#include <components/vector/data_chunk.hpp>
#include <queue>

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
        row_groups_ = std::make_shared<row_group_segment_tree_t>(*this);
    }

    uint64_t collection_t::total_rows() const { return total_rows_.load(); }

    const std::pmr::vector<types::complex_logical_type>& collection_t::types() const { return types_; }

    void collection_t::adopt_types(std::pmr::vector<types::complex_logical_type> types) {
        assert(types_.empty() && "adopt_types can only be called on schema-less collection");
        types_ = std::move(types);
    }

    void collection_t::append_row_group(std::unique_lock<std::mutex>& l, int64_t start_row) {
        assert(start_row >= row_start_);
        auto new_row_group = std::make_unique<row_group_t>(this, start_row, 0U);
        new_row_group->initialize_empty(types_);
        row_groups_->append_segment(l, std::move(new_row_group));
    }

    row_group_t* collection_t::row_group(int64_t index) { return row_groups_->segment_at(index); }

    void collection_t::initialize_scan(collection_scan_state& state, const std::vector<storage_index_t>&) {
        auto row_group = row_groups_->root_segment();
        state.row_groups = row_groups_.get();
        state.max_row = row_start_ + static_cast<int64_t>(total_rows_.load());
        state.initialize(types_);
        if (!row_group) {
            return;
        }
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
        auto row_group = row_groups_->get_segment(start_row);
        assert(row_group);
        state.row_groups = row_groups_.get();
        state.max_row = end_row;
        state.initialize(types_);
        uint64_t start_vector = static_cast<uint64_t>(start_row - row_group->start) / vector::DEFAULT_VECTOR_CAPACITY;
        if (!row_group->initialize_scan_with_offset(state, start_vector)) {
            throw std::logic_error("Failed to initialize row group scan with offset");
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

    bool collection_t::scan(const std::vector<storage_index_t>& column_ids,
                            const std::function<bool(vector::data_chunk_t& chunk)>& fun) {
        std::pmr::vector<types::complex_logical_type> scan_types(resource_);
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            scan_types.push_back(types_[column_ids[i].primary_index()]);
        }
        vector::data_chunk_t chunk(resource_, scan_types);

        table_scan_state state(resource_);
        state.initialize(column_ids, nullptr);
        initialize_scan(state.local_state, column_ids);

        while (true) {
            chunk.reset();
            state.local_state.scan(chunk);
            if (chunk.size() == 0) {
                return true;
            }
            if (!fun(chunk)) {
                return false;
            }
        }
    }

    bool collection_t::scan(const std::function<bool(vector::data_chunk_t& chunk)>& fun) {
        std::vector<storage_index_t> column_ids;
        column_ids.reserve(types_.size());
        for (uint64_t i = 0; i < types_.size(); i++) {
            column_ids.emplace_back(i);
        }
        return scan(column_ids, fun);
    }

    void collection_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state) {
        auto row_ids = row_identifiers.data<int64_t>();
        uint64_t count = 0;
        for (uint64_t i = 0; i < fetch_count; i++) {
            auto row_id = row_ids[i];
            row_group_t* row_group;
            {
                uint64_t segment_index;
                auto l = row_groups_->lock();
                if (!row_groups_->try_segment_index(l, row_id, segment_index)) {
                    continue;
                }
                row_group = row_groups_->segment_at(l, static_cast<int64_t>(segment_index));
            }
            row_group->fetch_row(state, column_ids, row_id, result, count);
            count++;
        }
        result.set_cardinality(count);
    }

    bool collection_t::is_empty() const {
        auto l = row_groups_->lock();
        return is_empty(l);
    }

    uint64_t collection_t::calculate_size() {
        uint64_t res = 0;
        auto row_group = row_groups_->root_segment();
        if (!row_group) {
            return 0;
        }
        while (row_group) {
            res += row_group->calculate_size();
            row_group = row_groups_->next_segment(row_group);
        }
        return res;
    }

    bool collection_t::is_empty(std::unique_lock<std::mutex>& l) const { return row_groups_->is_empty(l); }

    void collection_t::initialize_append(table_append_state& state) {
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
        state.start_row_group->initialize_append(state.append_state);
    }

    bool collection_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
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
                current_row_group->append(state.append_state, chunk, append_count);
                allocation_size_ += current_row_group->allocation_size() - previous_allocation_size;
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
            last_row_group->initialize_append(state.append_state);
        }
        state.current_row += int64_t(total_append_count);
        return new_row_group;
    }

    void collection_t::finalize_append(table_append_state& state) {
        auto remaining = state.total_append_count;
        auto row_group = state.start_row_group;
        while (remaining > 0) {
            auto append_count = std::min<uint64_t>(remaining, row_group_size_ - row_group->count);
            row_group->append_version_info(append_count);
            remaining -= append_count;
            row_group = row_groups_->next_segment(row_group);
        }
        total_rows_ += state.total_append_count;

        state.total_append_count = 0;
        state.start_row_group = nullptr;
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

    uint64_t collection_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count) {
        uint64_t delete_count = 0;
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(ids[start]);
            for (pos++; pos < count; pos++) {
                assert(ids[pos] >= 0);
                if (ids[pos] < row_group->start) {
                    break;
                }
                if (ids[pos] >= row_group->start + static_cast<int64_t>(row_group->count)) {
                    break;
                }
            }
            delete_count += row_group->delete_rows(table, ids + start, pos - start);
        } while (pos < count);
        return delete_count;
    }

    void collection_t::update(int64_t* ids, const std::vector<uint64_t>& column_ids, vector::data_chunk_t& updates) {
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(ids[pos]);
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
            row_group->update(updates, ids, start, pos - start, column_ids);
        } while (pos < updates.size());
    }

    void collection_t::update_column(vector::vector_t& row_ids,
                                     const std::vector<uint64_t>& column_path,
                                     vector::data_chunk_t& updates) {
        uint64_t pos = 0;
        do {
            uint64_t start = pos;
            auto row_group = row_groups_->get_segment(row_ids.data<int64_t>()[pos]);
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
            row_group->update(updates, row_ids.data<int64_t>(), start, pos - start, column_path);
        } while (pos < updates.size());
    }

    std::vector<column_segment_info> collection_t::get_column_segment_info() {
        std::vector<column_segment_info> result;
        for (auto& row_group : row_groups_->segments()) {
            row_group.get_column_segment_info(row_group.index, result);
        }
        return result;
    }

    std::shared_ptr<collection_t> collection_t::add_column(column_definition_t& new_column) {
        auto new_types = types_;
        new_types.push_back(new_column.type());
        auto result = std::make_shared<collection_t>(resource_,
                                                     block_manager_,
                                                     std::move(new_types),
                                                     row_start_,
                                                     total_rows_.load(),
                                                     row_group_size_);

        vector::vector_t default_vector(resource_, new_column.type());

        for (auto& current_row_group : row_groups_->segments()) {
            auto new_row_group =
                current_row_group.add_column(result.get(), new_column, new_column.default_value(), default_vector);

            result->row_groups_->append_segment(std::move(new_row_group));
        }
        return result;
    }

    std::shared_ptr<collection_t> collection_t::remove_column(uint64_t col_idx) {
        assert(col_idx < types_.size());
        auto new_types = types_;
        new_types.erase(new_types.begin() + static_cast<int64_t>(col_idx));

        auto result = std::make_shared<collection_t>(resource_,
                                                     block_manager_,
                                                     std::move(new_types),
                                                     row_start_,
                                                     total_rows_.load(),
                                                     row_group_size_);

        for (auto& current_row_group : row_groups_->segments()) {
            auto new_row_group = current_row_group.remove_column(result.get(), col_idx);
            result->row_groups_->append_segment(std::move(new_row_group));
        }
        return result;
    }

} // namespace components::table