#pragma once

#include "storage.hpp"
#include <cstdio>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/table_state.hpp>

namespace components::storage {

    // Presents columns ALTER TABLE ADD COLUMN published but no INSERT has materialized yet as trailing DEFAULT/NULL.
    class table_storage_adapter_t final : public storage_t {
    public:
        explicit table_storage_adapter_t(table::data_table_t& table, std::pmr::memory_resource* resource)
            : table_(table)
            , resource_(resource) {}

        std::pmr::vector<types::complex_logical_type> types(const table::transaction_data& txn) const override {
            return table_.visible_types(txn);
        }

        std::pmr::vector<types::complex_logical_type> types() const override {
            return table_.copy_types();
        }

        const std::vector<table::column_definition_t>& columns() const override { return table_.columns(); }

        size_t column_count() const override { return table_.column_count(); }

        bool has_schema() const override { return !table_.columns().empty(); }

        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& t) override { table_.adopt_schema(t); }


        uint64_t total_rows() const override { return table_.row_group()->total_rows(); }

        uint64_t calculate_size() override { return table_.calculate_size(); }

        void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) override {
            auto column_indices = begin_read(nullptr, table::transaction_data::committed());
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, table::transaction_data::committed(), filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int64_t limit,
                  table::transaction_data txn) override {
            auto column_indices = begin_read(nullptr, txn);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, txn, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols) override {
            auto column_indices = begin_read(&projected_cols, table::transaction_data::committed());
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, table::transaction_data::committed(), filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols,
                            table::transaction_data txn) override {
            auto column_indices = begin_read(&projected_cols, txn);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, txn, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        [[nodiscard]] core::result_wrapper_t<bool> scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                                                                const table::table_filter_t* filter,
                                                                int64_t limit,
                                                                const std::vector<size_t>* projected_cols,
                                                                table::transaction_data txn) override {
            auto column_indices = begin_read(projected_cols, txn);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, txn, filter);
            auto chunk_types = types();
            table_.scan_batched(chunk_types, projected_cols, batches, state, resource_);
            // scan_batched stays void; errors land in state.table_state.scan_error, surfaced here as a value.
            if (state.table_state.has_error()) {
                return state.table_state.scan_error;
            }
            // Always emit at least one chunk so downstream operators can read types/column_count.
            if (batches.empty()) {
                if (projected_cols) {
                    batches.emplace_back(resource_, chunk_types, *projected_cols, vector::DEFAULT_VECTOR_CAPACITY);
                } else {
                    batches.emplace_back(resource_, chunk_types, vector::DEFAULT_VECTOR_CAPACITY);
                }
                batches.back().set_cardinality(0);
            }
            if (limit >= 0) {
                uint64_t budget = static_cast<uint64_t>(limit);
                size_t keep = 0;
                for (; keep < batches.size(); ++keep) {
                    if (batches[keep].size() <= budget) {
                        budget -= batches[keep].size();
                    } else {
                        batches[keep].set_cardinality(budget);
                        ++keep;
                        budget = 0;
                        break;
                    }
                }
                batches.erase(batches.begin() + static_cast<std::ptrdiff_t>(keep), batches.end());
            }
            return true;
        }

        // Re-seeks a TRANSIENT table_scan_state and destructs it on return, so only the position crosses the mailbox.
        [[nodiscard]] core::result_wrapper_t<bool> fetch_next_batch(vector::data_chunk_t& output,
                                                                    scan_position_t& pos,
                                                                    const table::table_filter_t* filter,
                                                                    const std::vector<size_t>* projected_cols,
                                                                    table::transaction_data txn) override {
            if (pos.drained || pos.next_row >= pos.max_row) {
                pos.drained = true;
                return true;
            }
            auto column_indices = begin_read(projected_cols, txn);
            auto read =
                table_.fetch_next_batch(output, column_indices, filter, txn, pos.next_row, pos.max_row, pos.drained);
            if (read.has_error()) {
                return read;
            }
            return read;
        }

        [[nodiscard]] core::result_wrapper_t<bool> fetch(vector::data_chunk_t& output,
                                                         const vector::vector_t& row_ids,
                                                         uint64_t count,
                                                         const std::vector<size_t>& projected_cols,
                                                         const table::transaction_data& txn,
                                                         table::fetch_visibility_t visibility) override {
            table::column_fetch_state state;
            // Without this, string cells stay views into blocks that can be evicted once `state`'s pins die at return.
            state.result_outlives_pins = true;
#ifdef DEV_MODE
            // Guards the flag above, not the fetch itself.
            if (!state.result_outlives_pins) {
                uint64_t string_cols = 0;
                for (size_t i = 0; i < table_.column_count(); i++) {
                    if (output.data[i].type().to_physical_type() == types::physical_type::STRING) {
                        string_cols++;
                    }
                }
                table::note_escaping_borrowed_cells(string_cols * count);
            }
#endif
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            // The list stays FULL WIDTH: the fetch mapping is positional, a shorter one shifts columns by ordinal.
            table_.fetch(output, column_indices, row_ids, count, state, projected_cols, txn, visibility);
            if (state.fetch_error.contains_error()) {
                return state.fetch_error;
            }
            return true;
        }

        // On replay, NDEBUG strips asserts, so a failure must return an error, not be waved through as "a hard bug".
        [[nodiscard]] core::result_wrapper_t<uint64_t> append(vector::data_chunk_t& data,
                                                              table::transaction_data txn) override {
            table::table_append_state append_state(resource_);
            auto lock_r = table_.append_lock(append_state);
            if (lock_r.has_error()) {
                return lock_r.convert_error<uint64_t>();
            }
            auto init_r = table_.initialize_append(append_state);
            if (init_r.has_error()) {
                return init_r.convert_error<uint64_t>();
            }
            auto start_row = static_cast<uint64_t>(append_state.current_row);
            auto app_r = table_.append(data, append_state);
            if (app_r.has_error()) {
                return app_r.convert_error<uint64_t>();
            }
            table_.finalize_append(append_state, txn);
            return start_row;
        }

        [[nodiscard]] core::result_wrapper_t<appended_range_t>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) override {
            auto count = static_cast<uint64_t>(data.size());
            if (count == 0)
                return appended_range_t{0, 0};

            if (data.column_count() > table_.column_count()) {
                std::pmr::string what{"update: the payload carries ", resource_};
                what.append(std::to_string(data.column_count()).c_str());
                what.append(" column(s) and the table holds ");
                what.append(std::to_string(table_.column_count()).c_str());
                what.append(" — it names a column this storage does not have");
                return core::error_t{core::error_code_t::schema_error, std::move(what)};
            }

            auto delete_state = table_.initialize_delete({});
            // An update is a delete then an append: if the delete refuses, appending would leave
            // both the old and the new row.
            if (auto deleted = table_.delete_rows(*delete_state, row_ids, count, txn.transaction_id);
                deleted.has_error()) {
                return deleted.convert_error<appended_range_t>();
            }

            table::table_append_state append_state(resource_);
            auto lock_r = table_.append_lock(append_state);
            if (lock_r.has_error()) {
                return lock_r.convert_error<appended_range_t>();
            }
            auto init_r = table_.initialize_append(append_state);
            if (init_r.has_error()) {
                return init_r.convert_error<appended_range_t>();
            }
            auto start_row = static_cast<int64_t>(append_state.current_row);
            auto app_r = table_.append(data, append_state);
            if (app_r.has_error()) {
                return app_r.convert_error<appended_range_t>();
            }
            table_.finalize_append(append_state, txn);

            return appended_range_t{start_row, count};
        }


        core::result_wrapper_t<uint64_t>
        delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, txn_id);
        }

        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) override {
            table_.commit_append(commit_id, row_start, count);
        }

        core::error_t revert_append(int64_t row_start, uint64_t count) override {
            auto reverted = table_.revert_append(row_start, count);
            if (reverted.has_error()) {
                return reverted.error();
            }
            return core::error_t::no_error();
        }

        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id) override {
            table_.commit_all_deletes(txn_id, commit_id);
        }

        void revert_all_deletes(uint64_t txn_id) override { table_.revert_all_deletes(txn_id); }

        std::pmr::memory_resource* resource() const override { return resource_; }

        table::data_table_t& table() { return table_; }

    private:
        std::vector<table::storage_index_t> begin_read(const std::vector<size_t>* projected_cols,
                                                       const table::transaction_data& txn) const {
            return storage_indices(projected_cols, txn);
        }

        // nullptr means every materialized column; the result may legitimately come back EMPTY, not an error.
        std::vector<table::storage_index_t> storage_indices(const std::vector<size_t>* projected_cols,
                                                            const table::transaction_data& txn) const {
            std::vector<table::storage_index_t> out;
            const size_t visible = table_.visible_columns(txn).size();
            if (projected_cols != nullptr) {
                out.reserve(projected_cols->size());
                for (size_t idx : *projected_cols) {
                    if (idx < visible) {
                        out.emplace_back(static_cast<int64_t>(idx));
                    }
                }
                return out;
            }
            out.reserve(visible);
            for (size_t i = 0; i < visible; i++) {
                out.emplace_back(static_cast<int64_t>(i));
            }
            return out;
        }

        table::data_table_t& table_;
        std::pmr::memory_resource* resource_;
    };

} // namespace components::storage