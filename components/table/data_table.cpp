#include "data_table.hpp"

#include <components/table/storage/partial_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <unordered_set>

#include "row_group.hpp"

namespace components::table {

    data_table_t::data_table_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::vector<column_definition_t> column_definitions,
                               std::string name)
        : resource_(resource)
        , column_definitions_(std::move(column_definitions))
        , is_root_(true)
        , name_(std::move(name)) {
        this->row_groups_ = std::make_shared<collection_t>(resource_, block_manager, copy_types(), 0);
    }

    data_table_t::data_table_t(data_table_t& parent, column_definition_t& new_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }
        column_definitions_.emplace_back(new_column.copy());

        std::lock_guard parent_lock(parent.append_lock_);

        this->row_groups_ = parent.row_groups_->add_column(new_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent, uint64_t removed_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard parent_lock(parent.append_lock_);

        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }

        assert(removed_column < column_definitions_.size());
        column_definitions_.erase(column_definitions_.begin() + static_cast<int64_t>(removed_column));

        uint64_t storage_idx = 0;
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            auto& col = column_definitions_[i];
            col.set_oid(i);
            col.set_storage_oid(storage_idx++);
        }

        this->row_groups_ = parent.row_groups_->remove_column(removed_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent,
                               uint64_t changed_idx,
                               const types::complex_logical_type& target_type,
                               const std::vector<storage_index_t>&)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard lock(append_lock_);
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }

        column_definitions_[changed_idx].type() = target_type;

        parent.is_root_ = false;
    }

    [[nodiscard]] std::pmr::vector<types::complex_logical_type> data_table_t::copy_types() const {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(column_definitions_.size());
        for (auto& it : column_definitions_) {
            types.push_back(it.type());
        }
        return types;
    }

    const std::vector<column_definition_t>& data_table_t::columns() const { return column_definitions_; }

    void data_table_t::adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) {
        assert(column_definitions_.empty() && "adopt_schema can only be called on schema-less table");
        column_definitions_.reserve(types.size());
        for (const auto& type : types) {
            column_definitions_.emplace_back(type.alias(), type);
        }
        row_groups_->adopt_types(std::pmr::vector<types::complex_logical_type>(types, resource_));
    }

    void data_table_t::overlay_not_null(const std::string& col_name) {
        for (auto& col : column_definitions_) {
            if (col.name() == col_name) {
                col.set_not_null(true);
                return;
            }
        }
    }

    void data_table_t::initialize_scan(table_scan_state& state,
                                       const std::vector<storage_index_t>& column_ids,
                                       const table_filter_t* filter) {
        state.initialize(column_ids, filter);
        row_groups_->initialize_scan(state.table_state, column_ids);
    }

    void data_table_t::initialize_scan_with_offset(table_scan_state& state,
                                                   const std::vector<storage_index_t>& column_ids,
                                                   int64_t start_row,
                                                   int64_t end_row) {
        state.initialize(column_ids);
        row_groups_->initialize_scan_with_offset(state.table_state, column_ids, start_row, end_row);
    }

    uint64_t data_table_t::row_group_size() const { return row_groups_->row_group_size(); }

    std::shared_ptr<collection_t> data_table_t::row_group() const { return row_groups_; }

    uint64_t data_table_t::calculate_size() { return row_groups_->calculate_size(); }

    void data_table_t::cleanup_versions(uint64_t lowest_active_start_time) {
        row_groups_->cleanup_versions(lowest_active_start_time);
    }

    void data_table_t::compact() {
        auto total = row_groups_->total_rows();
        if (total == 0) {
            return;
        }

        auto types = row_groups_->types();
        auto new_collection = std::make_shared<collection_t>(
            resource_,
            row_groups_->block_manager(),
            std::pmr::vector<types::complex_logical_type>(types.begin(), types.end(), resource_),
            0);

        {
            table_append_state append_state(resource_);
            new_collection->initialize_append(append_state);

            // Scan committed non-deleted rows from old collection
            std::vector<storage_index_t> column_ids;
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                column_ids.emplace_back(i);
            }

            table_scan_state state(resource_);
            initialize_scan_with_offset(state, column_ids, 0, static_cast<int64_t>(total));

            auto scan_types = copy_types();
            vector::data_chunk_t chunk(resource_, scan_types, vector::DEFAULT_VECTOR_CAPACITY);
            while (true) {
                state.table_state.scan_committed(chunk,
                                                  table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
                if (chunk.size() == 0) {
                    break;
                }
                new_collection->append(chunk, append_state);
                chunk.reset();
            }

            new_collection->finalize_append(append_state, transaction_data{0, 0});
        }
        // scan state and buffer handles destroyed before swapping collection

        // Swap old collection with compacted one
        row_groups_ = std::move(new_collection);
    }

    void data_table_t::scan(vector::data_chunk_t& result, table_scan_state& state) { state.table_state.scan(result); }

    bool data_table_t::create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        return state.table_state.scan_committed(result, type);
    }

    std::string data_table_t::table_name() const { return name_; }

    void data_table_t::set_table_name(std::string new_name) { name_ = std::move(new_name); }

    void data_table_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state) {
        row_groups_->fetch(result, column_ids, row_identifiers, fetch_count, state);
    }

    std::unique_ptr<constraint_state> data_table_t::initialize_constraint_state(
        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        return std::make_unique<constraint_state>(bound_constraints);
    }

    void data_table_t::append_lock(table_append_state& state) {
        state.append_lock = std::unique_lock(append_lock_);
        if (!is_root_) {
            throw std::logic_error("Transaction conflict: adding entries to a table that has been altered!");
        }
        state.row_start = static_cast<int64_t>(row_groups_->total_rows());
        state.current_row = state.row_start;
    }

    void data_table_t::initialize_append(table_append_state& state) {
        if (!state.append_lock) {
            throw std::logic_error("data_table_t::append_lock should be called before data_table_t::initialize_append");
        }
        row_groups_->initialize_append(state);
    }

    void data_table_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        assert(is_root_);
        row_groups_->append(chunk, state);
    }

    void data_table_t::finalize_append(table_append_state& state, transaction_data txn) {
        row_groups_->finalize_append(state, txn);
    }

    void data_table_t::commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) {
        row_groups_->commit_append(commit_id, row_start, count);
    }

    void data_table_t::revert_append(int64_t row_start, uint64_t count) {
        row_groups_->revert_append(row_start, count);
    }

    void data_table_t::commit_all_deletes(uint64_t txn_id, uint64_t commit_id) {
        row_groups_->commit_all_deletes(txn_id, commit_id);
    }

    void data_table_t::scan_table_segment(int64_t row_start,
                                          uint64_t count,
                                          const std::function<void(vector::data_chunk_t& chunk)>& function) {
        if (count == 0) {
            return;
        }
        int64_t end = row_start + static_cast<int64_t>(count);

        std::vector<storage_index_t> column_ids;
        std::pmr::vector<types::complex_logical_type> types(resource_);
        for (uint64_t i = 0; i < this->column_definitions_.size(); i++) {
            auto& col = this->column_definitions_[i];
            column_ids.emplace_back(i);
            types.push_back(col.type());
        }
        vector::data_chunk_t chunk(resource_, types);

        create_index_scan_state state(resource_);

        initialize_scan_with_offset(state, column_ids, row_start, row_start + static_cast<int64_t>(count));
        auto row_start_aligned = state.table_state.row_group->start +
                                 static_cast<int64_t>(state.table_state.vector_index * vector::DEFAULT_VECTOR_CAPACITY);

        int64_t current_row = row_start_aligned;
        while (current_row < end) {
            state.table_state.scan_committed(chunk, table_scan_type::COMMITTED_ROWS);
            if (chunk.size() == 0) {
                break;
            }
            int64_t end_row = current_row + static_cast<int64_t>(chunk.size());
            int64_t chunk_start = std::max(current_row, row_start);
            int64_t chunk_end = std::min(end_row, end);
            assert(chunk_start < chunk_end);
            uint64_t chunk_count = static_cast<uint64_t>(chunk_end - chunk_start);
            if (chunk_count != chunk.size()) {
                assert(chunk_count <= chunk.size());
                uint64_t start_in_chunk;
                if (current_row >= row_start) {
                    start_in_chunk = 0;
                } else {
                    start_in_chunk = static_cast<uint64_t>(row_start - current_row);
                }
                vector::indexing_vector_t indexing(resource_, start_in_chunk, chunk_count);
                chunk.slice(indexing, chunk_count);
            }
            function(chunk);
            chunk.reset();
            current_row = end_row;
        }
    }

    std::shared_ptr<parallel_table_scan_state_t>
    data_table_t::create_parallel_scan_state(const std::vector<storage_index_t>& column_ids,
                                              const table_filter_t* filter) {
        auto total_rg = row_groups_->row_group_tree()->segment_count();
        return std::make_shared<parallel_table_scan_state_t>(column_ids, filter, total_rg);
    }

    bool data_table_t::next_parallel_chunk(parallel_table_scan_state_t& parallel_state,
                                            table_scan_state& local_state,
                                            vector::data_chunk_t& result) {
        while (true) {
            auto rg_idx = parallel_state.next_row_group_idx.fetch_add(1);
            if (rg_idx >= parallel_state.total_row_groups) {
                return false;
            }

            auto* rg = row_groups_->row_group_tree()->segment_at(static_cast<int64_t>(rg_idx));
            if (!rg) {
                return false;
            }

            local_state.initialize(parallel_state.column_ids, parallel_state.filter);
            int64_t max_row = rg->start + static_cast<int64_t>(rg->count);
            collection_t::initialize_scan_in_row_group(
                local_state.local_state, *row_groups_, *rg, 0, max_row);

            result.reset();
            local_state.local_state.scan_committed(result, table_scan_type::COMMITTED_ROWS);
            if (result.size() > 0) {
                return true;
            }
            // Empty row group (all deleted) — skip and try next
        }
    }

    void data_table_t::merge_storage(collection_t& data) { row_groups_->merge_storage(data); }

    std::unique_ptr<table_delete_state>
    data_table_t::initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        std::pmr::vector<types::complex_logical_type> types(resource_);
        auto result = std::make_unique<table_delete_state>(resource_);
        if (result->has_delete_constraints) {
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                result->col_ids.emplace_back(column_definitions_[i].storage_oid());
                types.emplace_back(column_definitions_[i].type());
            }
            result->constraint = std::make_unique<constraint_state>(bound_constraints);
        }
        return result;
    }

    uint64_t data_table_t::delete_rows(table_delete_state&, vector::vector_t& row_identifiers,
                                       uint64_t count, uint64_t transaction_id) {
        assert(row_identifiers.type().type() == types::logical_type::BIGINT);
        if (count == 0) {
            return 0;
        }

        row_identifiers.flatten(count);
        auto ids = row_identifiers.data<int64_t>();

        uint64_t pos = 0;
        uint64_t delete_count = 0;
        while (pos < count) {
            uint64_t start = pos;
            bool is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
            for (pos++; pos < count; pos++) {
                bool row_is_transaction_delete = static_cast<uint64_t>(ids[pos]) >= MAX_ROW_ID;
                if (row_is_transaction_delete != is_transaction_delete) {
                    break;
                }
            }
            uint64_t current_offset = start;
            uint64_t current_count = pos - start;

            vector::vector_t offset_ids(row_identifiers, current_offset, pos);
            delete_count += row_groups_->delete_rows(*this, ids + current_offset, current_count, transaction_id);
        }
        return delete_count;
    }

    std::unique_ptr<table_update_state>
    data_table_t::initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        auto result = std::make_unique<table_update_state>();
        result->constraint = initialize_constraint_state(bound_constraints);
        return result;
    }

    void data_table_t::update(table_update_state&,
                              vector::vector_t& row_ids,
                              // const std::vector<uint64_t>& column_ids,
                              vector::data_chunk_t& data) {
        assert(row_ids.type().to_physical_type() == types::physical_type::INT64);

        uint64_t count = data.size();
        if (count == 0) {
            return;
        }
        vector::vector_t max_row_id_vec(resource_, types::logical_value_t(resource_, static_cast<int64_t>(MAX_ROW_ID)), count);
        vector::vector_t row_ids_slice(resource_, types::logical_type::BIGINT, count);
        vector::data_chunk_t updates_slice(resource_, data.types(), count);
        vector::indexing_vector_t sel_local_update(resource_, count);
        vector::indexing_vector_t sel_global_update(resource_, count);

        auto update_count = count - vector::vector_ops::compare<std::greater_equal<>>(row_ids,
                                                                                      max_row_id_vec,
                                                                                      count,
                                                                                      &sel_local_update,
                                                                                      &sel_global_update);
        if (update_count > 0) {
            updates_slice.slice(data, sel_global_update, update_count);
            updates_slice.flatten();
            row_ids_slice.slice(row_ids, sel_global_update, update_count);
            row_ids_slice.flatten(update_count);

            // For now ids are fixed
            std::vector<uint64_t> column_ids;
            column_ids.reserve(column_count());
            for (size_t i = 0; i < column_count(); i++) {
                column_ids.emplace_back(i);
            }
            row_groups_->update(row_ids_slice.data<int64_t>(), column_ids, updates_slice);
        }
    }

    void data_table_t::update_column(vector::vector_t& row_ids,
                                     const std::vector<uint64_t>& column_path,
                                     vector::data_chunk_t& updates) {
        assert(row_ids.type().type() == types::logical_type::BIGINT);
        assert(updates.column_count() == 1);
        if (updates.size() == 0) {
            return;
        }

        if (!is_root_) {
            throw std::logic_error("Transaction conflict: cannot update a table that has been altered!");
        }

        updates.flatten();
        row_ids.flatten(updates.size());
        row_groups_->update_column(row_ids, column_path, updates);
    }

    uint64_t data_table_t::column_count() const { return column_definitions_.size(); }

    std::vector<column_segment_info> data_table_t::get_column_segment_info() {
        return row_groups_->get_column_segment_info();
    }

    void data_table_t::checkpoint(storage::metadata_writer_t& writer) {
        storage::partial_block_manager_t partial_block_manager(row_groups_->block_manager());

        auto row_group_pointers = row_groups_->checkpoint(partial_block_manager);

        // write table metadata
        writer.write_string(name_);

        // write column definitions
        writer.write<uint32_t>(static_cast<uint32_t>(column_definitions_.size()));
        for (const auto& col : column_definitions_) {
            writer.write_string(col.name());
            writer.write<uint8_t>(static_cast<uint8_t>(col.type().type()));
            writer.write<uint8_t>(col.is_not_null() ? 1 : 0);
        }

        // write row group count and pointers
        writer.write<uint32_t>(static_cast<uint32_t>(row_group_pointers.size()));
        for (const auto& rgp : row_group_pointers) {
            rgp.serialize(writer);
        }

        writer.flush();
    }

    std::unique_ptr<data_table_t>
    data_table_t::load_from_disk(std::pmr::memory_resource* resource,
                                  storage::block_manager_t& block_manager,
                                  storage::metadata_reader_t& reader) {
        auto name = reader.read_string();

        auto col_count = reader.read<uint32_t>();
        std::vector<column_definition_t> columns;
        columns.reserve(col_count);
        for (uint32_t i = 0; i < col_count; i++) {
            auto col_name = reader.read_string();
            auto logical_type = static_cast<types::logical_type>(reader.read<uint8_t>());
            auto not_null = reader.read<uint8_t>() != 0;
            types::complex_logical_type col_type(logical_type);
            col_type.set_alias(col_name);
            columns.emplace_back(col_name, std::move(col_type), not_null);
        }

        auto table = std::make_unique<data_table_t>(resource, block_manager, std::move(columns), std::move(name));

        uint64_t total_loaded_rows = 0;
        auto rg_count = reader.read<uint32_t>();
        for (uint32_t i = 0; i < rg_count; i++) {
            auto pointer = storage::row_group_pointer_t::deserialize(reader);

            // create a new row group and populate from disk pointer
            auto* rg = table->row_groups_->append_row_group(static_cast<int64_t>(pointer.row_start));
            if (rg) {
                rg->create_from_pointer(pointer);
                total_loaded_rows += pointer.tuple_count;
            }
        }
        table->row_groups_->set_total_rows(total_loaded_rows);

        return table;
    }

} // namespace components::table