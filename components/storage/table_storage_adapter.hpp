#pragma once

#include "storage.hpp"
#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>

namespace components::storage {

    class table_storage_adapter_t final : public storage_t {
    public:
        explicit table_storage_adapter_t(table::data_table_t& table, std::pmr::memory_resource* resource)
            : table_(table)
            , resource_(resource) {}

        std::pmr::vector<types::complex_logical_type> types() const override {
            return table_.copy_types();
        }

        const std::vector<table::column_definition_t>& columns() const override {
            return table_.columns();
        }

        size_t column_count() const override {
            return table_.column_count();
        }

        bool has_schema() const override {
            return !table_.columns().empty();
        }

        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& t) override {
            table_.adopt_schema(t);
        }

        void overlay_not_null(const std::string& col_name) override {
            table_.overlay_not_null(col_name);
        }

        uint64_t total_rows() const override {
            return table_.row_group()->total_rows();
        }

        uint64_t calculate_size() override {
            return table_.calculate_size();
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int limit) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(
                    std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int limit,
                  table::transaction_data txn) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(
                    std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void fetch(vector::data_chunk_t& output,
                   const vector::vector_t& row_ids,
                   uint64_t count) override {
            table::column_fetch_state state;
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_.fetch(output, column_indices, row_ids, count, state);
        }

        void scan_segment(int64_t start,
                          uint64_t count,
                          const std::function<void(vector::data_chunk_t& chunk)>& callback) override {
            table_.scan_table_segment(start, count, callback);
        }

        uint64_t parallel_scan(const std::function<void(vector::data_chunk_t& chunk)>& callback) override {
            std::vector<table::storage_index_t> column_ids;
            column_ids.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_ids.emplace_back(static_cast<int64_t>(i));
            }
            auto parallel_state = table_.create_parallel_scan_state(column_ids);
            auto types = table_.copy_types();
            uint64_t total_rows = 0;
            while (true) {
                table::table_scan_state local_state(resource_);
                vector::data_chunk_t result(resource_, types, vector::DEFAULT_VECTOR_CAPACITY);
                if (!table_.next_parallel_chunk(*parallel_state, local_state, result)) {
                    break;
                }
                total_rows += result.size();
                callback(result);
            }
            return total_rows;
        }

        uint64_t append(vector::data_chunk_t& data) override {
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<uint64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, table::transaction_data{0, 0});
            return start_row;
        }

        uint64_t append(vector::data_chunk_t& data, table::transaction_data txn) override {
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<uint64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, txn);
            return start_row;
        }

        void update(vector::vector_t& row_ids,
                    vector::data_chunk_t& data) override {
            auto update_state = table_.initialize_update({});
            table_.update(*update_state, row_ids, data);
        }

        std::pair<int64_t, uint64_t> update(vector::vector_t& row_ids,
                    vector::data_chunk_t& data,
                    table::transaction_data txn) override {
            auto count = static_cast<uint64_t>(data.size());
            if (count == 0) return {0, 0};

            // Step 1: Mark old rows as deleted with txn_id
            auto delete_state = table_.initialize_delete({});
            table_.delete_rows(*delete_state, row_ids, count, txn.transaction_id);

            // Step 2: Append new rows with txn version stamps
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<int64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, txn);

            return {start_row, count};
        }

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count);
        }

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, txn_id);
        }

        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) override {
            table_.commit_append(commit_id, row_start, count);
        }

        void revert_append(int64_t row_start, uint64_t count) override {
            table_.revert_append(row_start, count);
        }

        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id) override {
            table_.commit_all_deletes(txn_id, commit_id);
        }

        std::pmr::memory_resource* resource() const override {
            return resource_;
        }

        table::data_table_t& table() { return table_; }

    private:
        table::data_table_t& table_;
        std::pmr::memory_resource* resource_;
    };

} // namespace components::storage
