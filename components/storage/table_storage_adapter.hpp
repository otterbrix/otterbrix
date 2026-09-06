#pragma once

#include "storage.hpp"
#include <cstdio>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/table_state.hpp>

namespace components::storage {

    // Gap between catalog and storage: ALTER TABLE ADD COLUMN publishes a pg_attribute row
    // immediately; the physical column is materialized later, by the first INSERT that
    // carries it (agent_disk stage 1b) — see test_alter_rename_column::
    // rename_and_unmaterialized_add_column_are_distinguishable.
    // This adapter closes that gap for every reader by presenting unmaterialized columns as
    // trailing constant columns (DEFAULT or NULL) — the same constant row_group_t::add_column
    // later backfills, so the answer doesn't change across materialization.
    // columns()/has_schema() stay PHYSICAL: the append path's schema-growth stage must still
    // see the column as absent to materialize it.
    class table_storage_adapter_t final : public storage_t {
    public:
        // `unmaterialized` is borrowed (may be null), owned by the storage entry, which
        // outlives every adapter it builds.
        explicit table_storage_adapter_t(table::data_table_t& table,
                                         std::pmr::memory_resource* resource,
                                         const std::vector<table::column_definition_t>* unmaterialized = nullptr)
            : table_(table)
            , resource_(resource)
            , unmaterialized_(unmaterialized) {}

        // Catalog width: materialized columns at their storage ordinals, then columns
        // pg_attribute has published but no INSERT has materialized yet — so a projected
        // ordinal past the physical schema still addresses a real (DEFAULT/NULL) column.
        std::pmr::vector<types::complex_logical_type> types() const override {
            auto t = table_.copy_types();
            for (const auto& col : unmaterialized_columns()) {
                t.push_back(col.type());
            }
            return t;
        }

        // PHYSICAL schema — deliberately not widened. The append path's schema-growth/
        // column-expansion stages must see an unmaterialized column as absent to materialize it.
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
            // CATALOG width, not the physical one: the chunks are addressed by the caller's
            // (catalog) ordinals, and an unmaterialized column must be a real column in them.
            auto chunk_types = types();
            table_.scan_batched(chunk_types, projected_cols, batches, state, resource_);
            // data_table_t::scan_batched keeps its void shape and leaves any buffer-pool OOM /
            // data_corruption in state.table_state.scan_error; surface it here as a value so the
            // agent_disk scan reply can carry it across the mailbox. On error the partially-filled
            // batches are discarded (the caller turns this into an error cursor).
            if (state.table_state.has_error()) {
                return state.table_state.scan_error;
            }
            // Always emit at least one (possibly empty) chunk so downstream operators
            // can read types/column_count from chunks.front().
            if (batches.empty()) {
                if (projected_cols) {
                    batches.emplace_back(resource_, chunk_types, *projected_cols, vector::DEFAULT_VECTOR_CAPACITY);
                } else {
                    batches.emplace_back(resource_, chunk_types, vector::DEFAULT_VECTOR_CAPACITY);
                }
                batches.back().set_cardinality(0);
            }
            // Apply LIMIT post-hoc by truncating trailing batches and the boundary chunk.
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
                // erase trailing batches; data_chunk_t is non-default-constructible so
                // resize() doesn't compile.
                batches.erase(batches.begin() + static_cast<std::ptrdiff_t>(keep), batches.end());
            }
            for (auto& batch : batches) {
                fill_unmaterialized(batch, batch.size());
            }
            return true;
        }

        // Streaming fetch-next (STEP 3 / index-resume). Re-seeks a TRANSIENT table_scan_state to
        // pos.next_row, reads ONE batch, advances pos, then lets the scan state (and its column
        // pins) destruct on return — so nothing crosses the mailbox but the position. The source
        // row consumed is tracked by the scan state's (row_group->start + vector_index*CAP),
        // independent of how many rows the filter matched, so the cursor never re-reads a row.
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
            // data_table_t owns the transient-scan-state seek + single-batch read + position
            // advance (it has row_group.hpp; the scan state and its pins live and die inside that
            // call, so nothing pinned survives this round-trip).
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
            // The chunk we fill is returned to the caller and then moved across a mailbox; the pins
            // taken below die with `state` when this function returns. Without this flag the string
            // leg writes views BORROWED from those blocks, and once the pin is gone the block can be
            // evicted — or spilled to the scratch file and reloaded at a different address — leaving
            // the caller reading freed memory. row_group_t's gather sets the same flag for the same
            // reason.
            state.result_outlives_pins = true;
#ifdef DEV_MODE
            // Guards the line above rather than the fetch itself: drop the flag and every string
            // cell below goes back to being a view into a block this call stops pinning.
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
            // The list stays FULL WIDTH and the projection is applied as a skip below it, because the
            // fetch mapping is positional: a shorter list would compact the chunk and shift every
            // column a consumer addresses by ordinal.
            table_.fetch(output, column_indices, row_ids, count, state, projected_cols, txn, visibility);
            // state.fetch_error carries buffer-pool OOM / data_corruption from the string leg;
            // on error the partially-filled chunk is meaningless and must not be shipped.
            if (state.fetch_error.contains_error()) {
                return state.fetch_error;
            }
            fill_unmaterialized(output, output.size());
            return true;
        }

        // Returns the start_row on success, or write_conflict / out_of_memory surfaced by the
        // table-layer append chain. The agent_disk append handler reads the wrapper and turns
        // any error into a graceful txn abort.
        // Also the replay append: NDEBUG strips asserts, so treating a replay failure as "a
        // hard bug" would return a start_row for an append that never happened. The
        // direct-write caller passes transaction_data{0, 0}.
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

        // Replay leg: rewrites IN PLACE (vs. MVCC delete+append below). The WAL record carries
        // the CATALOG-wide chunk while storage is narrower, so it's trimmed first.
        // Recover-then-report, not refuse-up-front: a value in an unmaterialized column at
        // replay time means that column's materializing INSERT was already refused (and
        // logged) earlier in the replay, so refusing here too would restore less than the
        // silent trim does. The materialized part is written unconditionally; the answer names
        // what could not be restored. Returns error_t, not void, so this can't be swallowed by
        // an NDEBUG-only assert and reported as "restored".
        [[nodiscard]] core::error_t update(vector::vector_t& row_ids, vector::data_chunk_t& data) override {
            core::error_t lost = trim_unmaterialized_payload_for_replay(data);
            const auto requested = data.size();
            auto update_state = table_.initialize_update({});
            auto upd_r = table_.update(*update_state, row_ids, data);
            if (upd_r.has_error()) {
                return core::error_on(resource_, upd_r.error());
            }
            // {0, applied-count} is the half of the answer a void signature cannot carry:
            // data_table_t::update filters row ids at or past MAX_ROW_ID, so "applied to 0 of
            // them" would otherwise read exactly like "applied to all of them".
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

        // Returns {start_row, count} on success, or write_conflict / out_of_memory surfaced by
        // the table-layer delete+append MVCC update; agent_disk surfaces it.
        [[nodiscard]] core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) override {
            auto count = static_cast<uint64_t>(data.size());
            if (count == 0)
                return std::pair<int64_t, uint64_t>{0, 0};

            if (auto trimmed = trim_unmaterialized_payload(data); trimmed.contains_error()) {
                return trimmed;
            }

            // Step 1: Mark old rows as deleted with txn_id
            auto delete_state = table_.initialize_delete({});
            table_.delete_rows(*delete_state, row_ids, count, txn.transaction_id);

            // Step 2: Append new rows with txn version stamps
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

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, 0);
        }

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, txn_id);
        }

        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) override {
            table_.commit_append(commit_id, row_start, count);
        }

        void revert_append(int64_t row_start, uint64_t count) override {
            // Void contract can't propagate the refusal further up, but result_wrapper_t is
            // [[nodiscard]] at the class, so silently dropping it is a -Werror break — report
            // to stderr instead.
            auto reverted = table_.revert_append(row_start, count);
            if (reverted.has_error()) {
                std::fprintf(stderr,
                             "components::storage::table_storage_adapter_t::revert_append: rollback of rows "
                             "[%lld, +%llu) could not complete: %s\n",
                             static_cast<long long>(row_start),
                             static_cast<unsigned long long>(count),
                             reverted.error().what.c_str());
            }
        }

        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id) override {
            table_.commit_all_deletes(txn_id, commit_id);
        }

        void revert_all_deletes(uint64_t txn_id) override { table_.revert_all_deletes(txn_id); }

        std::pmr::memory_resource* resource() const override { return resource_; }

        table::data_table_t& table() { return table_; }

    private:
        // Empty stand-in for a null `unmaterialized_` so every reader below can take a reference
        // and never branch on the pointer.
        static inline const std::vector<table::column_definition_t> no_unmaterialized_columns_{};

        const std::vector<table::column_definition_t>& unmaterialized_columns() const noexcept {
            return unmaterialized_ != nullptr ? *unmaterialized_ : no_unmaterialized_columns_;
        }

        // Write-side mirror of types(): an update payload is shaped by the read that produced
        // it, so it arrives at CATALOG width. data_table_t can write only the PHYSICAL schema,
        // so trailing columns are dropped here — but only once checked to carry NOTHING NEW: a
        // value that differs from the column's own DEFAULT is the statement's own write and
        // gets refused (only the append path's schema-growth stage may materialize a column). A
        // value EQUAL to the DEFAULT is fill_unmaterialized's own fill read back, not a write,
        // so it's dropped silently — otherwise `UPDATE t SET a=9 WHERE extra IS NOT NULL` would
        // error on a table whose only sin is having a DEFAULT.
        // Pinned by integration/cpp/test/test_alter_add_column_unmaterialized.cpp:
        // `UPDATE ... SET extra = 42` on an unmaterialized column errors with rows unchanged.
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
                        continue; // the READ's own fill, echoed back — see the note above
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

        // Replay-side mirror of the trim above, refusal turned into a report: trailing columns
        // are dropped unconditionally so the materialized part of the row still gets restored;
        // the answer names any journalled value that had to be dropped with them. See the
        // replay `update` above for the full reasoning.
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
                    // Equal to the published DEFAULT = the read's own fill echoed back, not a
                    // written value — dropping it silently avoids a false loss report.
                    if (published != nullptr && published->has_value() && data.data[i].value(row) == **published) {
                        continue;
                    }
                    if (!lost_columns.empty()) {
                        lost_columns.append(", ");
                    }
                    lost_columns.append("'");
                    // The chunk's own alias is the WAL record's column name and is always
                    // present; the declared list only knows columns already published to this
                    // entry, which a failed upstream replay may not have done yet.
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
            // erase, not resize: vector_t is not default-constructible, so resize() does not compile.
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

        // Every read entry point starts here. It drops ordinals no row group can read
        // (storage_indices, below), leaving those columns for fill_unmaterialized to answer
        // AFTER the scan; and it publishes the same list to the collection so the pushed-down
        // filter (row_group_t::evaluate_predicate) can answer them DURING the scan. Skipping the
        // publish would answer the projection with DEFAULT but the predicate with NULL — the
        // split default_answers_the_predicate_leg pins.
        // Published per read, not once: data_table_t::compact installs a new collection under
        // this adapter without rebuilding it.
        std::vector<table::storage_index_t> begin_read(const std::vector<size_t>* projected_cols) const {
            table_.row_group()->publish_unmaterialized_columns(&unmaterialized_columns());
            return storage_indices(projected_cols);
        }

        // Catalog ordinals reduced to what a row group can read; nullptr means every
        // materialized column. An ordinal past the physical schema is dropped on purpose (it
        // names an unmaterialized column, answered by fill_unmaterialized instead), so the
        // result may legitimately come back EMPTY — a row-count-only scan, not an error.
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

        // Projection leg's half of the answer (row_group_t::evaluate_predicate is the predicate
        // leg's); both go through fill_published_default. Same device as PostgreSQL 11+'s
        // pg_attribute.attmissingval — no heap rewrite, and the constant is exactly what
        // row_group_t::add_column later backfills, so the answer doesn't move at that boundary.
        // A buffer-less placeholder column is skipped: nothing reads it.
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
        // BORROWED, may be null. See the note on the class.
        const std::vector<table::column_definition_t>* unmaterialized_;
    };

} // namespace components::storage