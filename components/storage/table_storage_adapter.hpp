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
        // `unmaterialized` is borrowed (may be null), owned by the storage entry that outlives every adapter it builds.
        explicit table_storage_adapter_t(table::data_table_t& table,
                                         std::pmr::memory_resource* resource,
                                         const std::vector<table::column_definition_t>* unmaterialized = nullptr)
            : table_(table)
            , resource_(resource)
            , unmaterialized_(unmaterialized) {}

        std::pmr::vector<types::complex_logical_type> types() const override {
            auto t = table_.copy_types();
            for (const auto& col : unmaterialized_columns()) {
                t.push_back(col.type());
            }
            return t;
        }

        // PHYSICAL, deliberately not widened: append must see an unmaterialized column as absent to materialize it.
        const std::vector<table::column_definition_t>& columns() const override { return table_.columns(); }

        size_t column_count() const override { return table_.column_count(); }

        bool has_schema() const override { return !table_.columns().empty(); }

        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& t) override { table_.adopt_schema(t); }


        uint64_t total_rows() const override { return table_.row_group()->total_rows(); }

        uint64_t calculate_size() override { return table_.calculate_size(); }

        void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) override {
            auto column_indices = begin_read(nullptr);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            fill_unmaterialized(output, output.size());
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int64_t limit,
                  table::transaction_data txn) override {
            auto column_indices = begin_read(nullptr);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            fill_unmaterialized(output, output.size());
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols) override {
            auto column_indices = begin_read(&projected_cols);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            fill_unmaterialized(output, output.size());
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols,
                            table::transaction_data txn) override {
            auto column_indices = begin_read(&projected_cols);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            fill_unmaterialized(output, output.size());
        }

        [[nodiscard]] core::result_wrapper_t<bool> scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                                                                const table::table_filter_t* filter,
                                                                int64_t limit,
                                                                const std::vector<size_t>* projected_cols,
                                                                table::transaction_data txn) override {
            auto column_indices = begin_read(projected_cols);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
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
            for (auto& batch : batches) {
                fill_unmaterialized(batch, batch.size());
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
            auto column_indices = begin_read(projected_cols);
            auto read =
                table_.fetch_next_batch(output, column_indices, filter, txn, pos.next_row, pos.max_row, pos.drained);
            if (read.has_error()) {
                return read;
            }
            fill_unmaterialized(output, output.size());
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
            fill_unmaterialized(output, output.size());
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

        // Recover-then-report: a value here means its materializing INSERT was already refused upstream.
        [[nodiscard]] core::error_t update(vector::vector_t& row_ids, vector::data_chunk_t& data) override {
            core::error_t lost = trim_unmaterialized_payload_for_replay(data);
            const auto requested = data.size();
            auto update_state = table_.initialize_update({});
            auto upd_r = table_.update(*update_state, row_ids, data);
            if (upd_r.has_error()) {
                return core::error_on(resource_, upd_r.error());
            }
            // {0, applied-count}, since data_table_t::update filters row ids at or past MAX_ROW_ID.
            const uint64_t applied = upd_r.value().second;
            if (applied != requested) {
                std::pmr::string what{"replay update applied ", resource_};
                what.append(std::to_string(applied).c_str());
                what.append(" of ");
                what.append(std::to_string(requested).c_str());
                what.append(" journalled row update(s); the rest named rows this storage cannot hold");
                if (lost.contains_error()) {
                    what.append("; additionally: ");
                    what.append(lost.what.c_str());
                }
                return core::error_t{core::error_code_t::io_error, std::move(what)};
            }
            return lost;
        }

        [[nodiscard]] core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) override {
            auto count = static_cast<uint64_t>(data.size());
            if (count == 0)
                return std::pair<int64_t, uint64_t>{0, 0};

            if (auto trimmed = trim_unmaterialized_payload(data); trimmed.contains_error()) {
                return trimmed;
            }

            auto delete_state = table_.initialize_delete({});
            // An update is a delete then an append: if the delete refuses, appending would leave
            // both the old and the new row.
            if (auto deleted = table_.delete_rows(*delete_state, row_ids, count, txn.transaction_id);
                deleted.has_error()) {
                return deleted.convert_error<std::pair<int64_t, uint64_t>>();
            }

            table::table_append_state append_state(resource_);
            auto lock_r = table_.append_lock(append_state);
            if (lock_r.has_error()) {
                return lock_r.convert_error<std::pair<int64_t, uint64_t>>();
            }
            auto init_r = table_.initialize_append(append_state);
            if (init_r.has_error()) {
                return init_r.convert_error<std::pair<int64_t, uint64_t>>();
            }
            auto start_row = static_cast<int64_t>(append_state.current_row);
            auto app_r = table_.append(data, append_state);
            if (app_r.has_error()) {
                return app_r.convert_error<std::pair<int64_t, uint64_t>>();
            }
            table_.finalize_append(append_state, txn);

            return std::pair<int64_t, uint64_t>{start_row, count};
        }

        core::result_wrapper_t<uint64_t> delete_rows(vector::vector_t& row_ids, uint64_t count) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, 0);
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
        static inline const std::vector<table::column_definition_t> no_unmaterialized_columns_{};

        const std::vector<table::column_definition_t>& unmaterialized_columns() const noexcept {
            return unmaterialized_ != nullptr ? *unmaterialized_ : no_unmaterialized_columns_;
        }

        // Trailing columns are dropped; a value equal to DEFAULT is fill_unmaterialized's fill read back, not a write.
        [[nodiscard]] core::error_t trim_unmaterialized_payload(vector::data_chunk_t& data) const {
            const size_t physical = table_.column_count();
            if (data.column_count() <= physical) {
                return core::error_t::no_error();
            }
            const auto& declared = unmaterialized_columns();
            for (size_t i = physical; i < data.column_count(); i++) {
                const size_t declared_idx = i - physical;
                const auto* published =
                    declared_idx < declared.size() ? &declared[declared_idx].default_value_opt() : nullptr;
                for (uint64_t row = 0; row < data.size(); row++) {
                    if (data.is_null(i, row)) {
                        continue;
                    }
                    if (published != nullptr && published->has_value() && data.data[i].value(row) == **published) {
                        continue;
                    }
                    std::pmr::string what{"UPDATE writes column '", resource_};
                    what.append(declared_idx < declared.size() ? declared[declared_idx].name().c_str() : "?");
                    what.append("', which ALTER TABLE ADD COLUMN has published in the catalog and no INSERT "
                                "has materialized in the storage yet; insert a row carrying it first");
                    return core::error_t{core::error_code_t::unimplemented_yet, std::move(what)};
                }
            }
            // erase, not resize: vector_t is not default-constructible, so resize() does not compile.
            data.data.erase(data.data.begin() + static_cast<std::ptrdiff_t>(physical), data.data.end());
            return core::error_t::no_error();
        }

        // Replay-side mirror of the trim above: columns are dropped unconditionally; the answer names what was lost.
        [[nodiscard]] core::error_t trim_unmaterialized_payload_for_replay(vector::data_chunk_t& data) const {
            const size_t physical = table_.column_count();
            if (data.column_count() <= physical) {
                return core::error_t::no_error();
            }
            const auto& declared = unmaterialized_columns();
            std::pmr::string lost_columns{resource_};
            for (size_t i = physical; i < data.column_count(); i++) {
                const size_t published_idx = i - physical;
                const auto* published =
                    published_idx < declared.size() ? &declared[published_idx].default_value_opt() : nullptr;
                for (uint64_t row = 0; row < data.size(); row++) {
                    if (data.is_null(i, row)) {
                        continue;
                    }
                    if (published != nullptr && published->has_value() && data.data[i].value(row) == **published) {
                        continue;
                    }
                    if (!lost_columns.empty()) {
                        lost_columns.append(", ");
                    }
                    lost_columns.append("'");
                    // The chunk's alias is the WAL column name; the declared list may lag a failed upstream replay.
                    const size_t declared_idx = i - physical;
                    if (data.data[i].type().has_alias()) {
                        lost_columns.append(data.data[i].type().alias().c_str());
                    } else if (declared_idx < declared.size()) {
                        lost_columns.append(declared[declared_idx].name().c_str());
                    } else {
                        lost_columns.append("?");
                    }
                    lost_columns.append("'");
                    break;
                }
            }
            data.data.erase(data.data.begin() + static_cast<std::ptrdiff_t>(physical), data.data.end());
            if (lost_columns.empty()) {
                return core::error_t::no_error();
            }
            std::pmr::string what{"replay update restored the row's materialized columns, but the journalled "
                                  "value(s) for unmaterialized column(s) ",
                                  resource_};
            what.append(lost_columns.c_str());
            what.append(" were dropped — the column's materialising INSERT did not replay");
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(what)};
        }

        // Also publishes the dropped ordinals so the pushed-down predicate answers them the same as the projection.
        std::vector<table::storage_index_t> begin_read(const std::vector<size_t>* projected_cols) const {
            table_.row_group()->publish_unmaterialized_columns(&unmaterialized_columns());
            return storage_indices(projected_cols);
        }

        // nullptr means every materialized column; the result may legitimately come back EMPTY, not an error.
        std::vector<table::storage_index_t> storage_indices(const std::vector<size_t>* projected_cols) const {
            std::vector<table::storage_index_t> out;
            const size_t physical = table_.column_count();
            if (projected_cols != nullptr) {
                out.reserve(projected_cols->size());
                for (size_t idx : *projected_cols) {
                    if (idx < physical) {
                        out.emplace_back(static_cast<int64_t>(idx));
                    }
                }
                return out;
            }
            out.reserve(physical);
            for (size_t i = 0; i < physical; i++) {
                out.emplace_back(static_cast<int64_t>(i));
            }
            return out;
        }

        // Same device as PostgreSQL's pg_attribute.attmissingval: the constant is what add_column later backfills.
        void fill_unmaterialized(vector::data_chunk_t& chunk, uint64_t rows) const {
            const auto& declared = unmaterialized_columns();
            if (declared.empty() || rows == 0) {
                return;
            }
            const size_t physical = table_.column_count();
            for (size_t i = 0; i < declared.size(); i++) {
                const size_t idx = physical + i;
                if (idx >= chunk.column_count()) {
                    break;
                }
                auto& column = chunk.data[idx];
                if (column.data() == nullptr && column.auxiliary() == nullptr) {
                    continue;
                }
                table::fill_published_default(column, &declared[i], rows);
            }
        }

        table::data_table_t& table_;
        std::pmr::memory_resource* resource_;
        const std::vector<table::column_definition_t>* unmaterialized_;
    };

} // namespace components::storage