#pragma once

#include "storage.hpp"
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/table_state.hpp>

namespace components::storage {

    // COLUMNS THE CATALOG HAS AND THE STORAGE DOES NOT.
    //
    // ALTER TABLE ADD COLUMN writes a pg_attribute row and stops; the physical column is
    // materialized later, by the first INSERT that carries it (agent_disk stage 1b). Between the
    // two the catalog names a column no row group holds — a legal, durable, and after a restart
    // re-entered state, pinned by test_alter_rename_column's
    // rename_and_unmaterialized_add_column_are_distinguishable.
    //
    // This adapter is the seam where that gap is closed for EVERY reader at once: the storage
    // scan, the pushed-down filter, the aggregate-pushdown reduce (which reads through
    // fetch_next_batch) and the row-id gather all take their column count and their column
    // ordinals from here. The unmaterialized columns are presented as trailing all-NULL columns
    // — NULL being exactly what row_group_t::add_column will backfill into the rows that predate
    // the column when the materializing INSERT finally arrives, so the answer does not change
    // across that boundary.
    //
    // They are NOT added to columns()/has_schema(): those describe the PHYSICAL schema and drive
    // the append path's schema-growth and column-expansion stages, which must go on seeing the
    // column as absent so the next carrying INSERT materializes it.
    class table_storage_adapter_t final : public storage_t {
    public:
        // `unmaterialized` is BORROWED and may be null; it is owned by the storage entry, which
        // outlives every adapter it builds (the adapter is rebuilt on add_column / drop_column).
        explicit table_storage_adapter_t(table::data_table_t& table,
                                         std::pmr::memory_resource* resource,
                                         const std::vector<table::column_definition_t>* unmaterialized = nullptr)
            : table_(table)
            , resource_(resource)
            , unmaterialized_(unmaterialized) {}

        // The CATALOG's width: the materialized columns at their storage ordinals, then the
        // columns pg_attribute has published and no INSERT has materialized yet. Every chunk this
        // adapter fills is built from this list, so a projected ordinal past the physical schema
        // addresses a real (all-NULL) column instead of falling off the end.
        std::pmr::vector<types::complex_logical_type> types() const override {
            auto t = table_.copy_types();
            for (const auto& col : unmaterialized_columns()) {
                t.push_back(col.type());
            }
            return t;
        }

        // PHYSICAL schema, deliberately NOT widened by the unmaterialized columns. These three
        // drive the append path (schema growth, column expansion, NOT NULL) and the keyed catalog
        // reads, all of which must go on seeing an unmaterialized column as absent — that is what
        // makes the next INSERT that carries it materialize it.
        const std::vector<table::column_definition_t>& columns() const override { return table_.columns(); }

        size_t column_count() const override { return table_.column_count(); }

        bool has_schema() const override { return !table_.columns().empty(); }

        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& t) override { table_.adopt_schema(t); }


        uint64_t total_rows() const override { return table_.row_group()->total_rows(); }

        uint64_t calculate_size() override { return table_.calculate_size(); }

        void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) override {
            auto column_indices = storage_indices(nullptr);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            null_fill_unmaterialized(output, output.size());
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int64_t limit,
                  table::transaction_data txn) override {
            auto column_indices = storage_indices(nullptr);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            null_fill_unmaterialized(output, output.size());
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols) override {
            auto column_indices = storage_indices(&projected_cols);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            null_fill_unmaterialized(output, output.size());
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols,
                            table::transaction_data txn) override {
            auto column_indices = storage_indices(&projected_cols);
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
            null_fill_unmaterialized(output, output.size());
        }

        [[nodiscard]] core::result_wrapper_t<bool> scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                                                                const table::table_filter_t* filter,
                                                                int64_t limit,
                                                                const std::vector<size_t>* projected_cols,
                                                                table::transaction_data txn) override {
            auto column_indices = storage_indices(projected_cols);
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
                null_fill_unmaterialized(batch, batch.size());
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
            auto column_indices = storage_indices(projected_cols);
            // data_table_t owns the transient-scan-state seek + single-batch read + position
            // advance (it has row_group.hpp; the scan state and its pins live and die inside that
            // call, so nothing pinned survives this round-trip).
            auto read =
                table_.fetch_next_batch(output, column_indices, filter, txn, pos.next_row, pos.max_row, pos.drained);
            if (read.has_error()) {
                return read;
            }
            null_fill_unmaterialized(output, output.size());
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
            // The string leg records buffer-pool OOM / data_corruption in
            // state.fetch_error; surface it as a value so the agent_disk fetch
            // reply can carry it across the mailbox (same shape as
            // fetch_next_batch's scan_error above). On error the partially
            // filled chunk is meaningless — the caller must not ship it.
            if (state.fetch_error.contains_error()) {
                return state.fetch_error;
            }
            null_fill_unmaterialized(output, output.size());
            return true;
        }

        // Returns the start_row on success, or write_conflict / out_of_memory surfaced by the
        // table-layer append chain. The agent_disk append handler reads the wrapper and turns
        // any error into a graceful txn abort.
        //
        // THIS IS ALSO THE REPLAY APPEND. There used to be a second `append(data)` above with
        // this exact body, every wrapper bound to a [[maybe_unused]] local and asserted rather
        // than returned, on the reasoning that "replay records are already schema-aligned and
        // single-threaded, so a failure here is a hard bug". out_of_memory is not a bug, and
        // under NDEBUG the asserts are not there at all: the caller got the start_row of an
        // append that never happened. The direct-write caller passes transaction_data{0, 0},
        // which is what that overload's finalize_append used.
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

        // Replay leg — an IN-PLACE update, unlike the MVCC delete+append below it. The
        // journalled payload was written by the txn update below, which already refused any
        // value in an unmaterialized column, so the trim here SHOULD only be dropping all-NULL
        // columns — but it still has to happen: the WAL record carries the CATALOG-wide chunk,
        // and at replay time the storage is narrower still.
        //
        // "SHOULD" IS NOT A CHANNEL, AND THAT IS WHAT CHANGED. Both refusals used to be
        // asserts over [[maybe_unused]] locals — absent entirely under NDEBUG — so a replayed
        // committed row that this leg declined to write was reported to
        // agent_disk_t::direct_update_sync as written, and from there to base_spaces' replay
        // loop as restored. Recovery cannot tell "there was nothing to do" from "I could not do
        // it" unless this says so.
        //
        // AND ON THIS PATH THE ANSWER IS RECOVER-THEN-REPORT, NOT REFUSE-UP-FRONT. A value in
        // an unmaterialized column at REPLAY time means the column's materialising INSERT was
        // itself refused earlier in the replay (and logged) — the value has no column to land
        // in either way. The row's materialized columns are still addressable, and the
        // NDEBUG build this channel replaced DID restore them (data_table_t::update builds
        // its column list from its own column_count() and never reads the chunk's trailing
        // columns), so refusing before table_.update would restore LESS than the silent code
        // it replaced. The trim is applied unconditionally, the materialized part is written
        // IN PLACE, and the answer names the value that could not be restored — the txn
        // overload below keeps the up-front refusal, because there the statement can still
        // be refused BEFORE anything is journalled.
        [[nodiscard]] core::error_t update(vector::vector_t& row_ids, vector::data_chunk_t& data) override {
            core::error_t lost = trim_unmaterialized_payload_for_replay(data);
            const auto requested = data.size();
            auto update_state = table_.initialize_update({});
            auto upd_r = table_.update(*update_state, row_ids, data);
            if (upd_r.has_error()) {
                return core::error_on(resource_, upd_r.error());
            }
            // {0, applied-count} is the half of the answer the old void signature dropped:
            // data_table_t::update filters row ids at or past MAX_ROW_ID, so "applied to 0
            // of them" used to report exactly like "applied to all of them".
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

        void revert_append(int64_t row_start, uint64_t count) override { table_.revert_append(row_start, count); }

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

        // WRITE-SIDE MIRROR OF types(). An update payload is shaped by the READ that produced it,
        // so it arrives at the CATALOG's width — one column per pg_attribute column, including the
        // ones no row group holds. data_table_t can only write the PHYSICAL schema, so those
        // trailing columns are dropped here.
        //
        // Dropping them is sound only while they carry NOTHING, and that is checked rather than
        // assumed: a value in one of them is an UPDATE that would first have to materialize the
        // column, which only the append path's schema-growth stage can do (it owns the
        // PHYSICAL_ADD_COLUMN WAL record that keeps replay in schema-then-rows order). Writing the
        // row and silently losing that value is exactly what rule 6 forbids, so the statement is
        // refused instead. The refusal reaches the agent before any WAL record is written for the
        // update (operator_update journals only after storage_update succeeds), so a refused
        // statement leaves nothing behind.
        [[nodiscard]] core::error_t trim_unmaterialized_payload(vector::data_chunk_t& data) const {
            const size_t physical = table_.column_count();
            if (data.column_count() <= physical) {
                return core::error_t::no_error();
            }
            const auto& declared = unmaterialized_columns();
            for (size_t i = physical; i < data.column_count(); i++) {
                for (uint64_t row = 0; row < data.size(); row++) {
                    if (data.is_null(i, row)) {
                        continue;
                    }
                    std::pmr::string what{"UPDATE writes column '", resource_};
                    const size_t declared_idx = i - physical;
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

        // REPLAY-SIDE MIRROR of the trim above, with the refusal turned into a report: the
        // trailing columns are dropped UNCONDITIONALLY (recovery goes on to restore the
        // materialized part of the row), and the answer names any journalled value that had
        // to be dropped with them, so the replay loop can say what was lost instead of
        // either losing it silently (the pre-channel shape) or refusing the whole row (which
        // restores less than the silent shape did). See the replay `update` for the full
        // reasoning.
        [[nodiscard]] core::error_t trim_unmaterialized_payload_for_replay(vector::data_chunk_t& data) const {
            const size_t physical = table_.column_count();
            if (data.column_count() <= physical) {
                return core::error_t::no_error();
            }
            const auto& declared = unmaterialized_columns();
            std::pmr::string lost_columns{resource_};
            for (size_t i = physical; i < data.column_count(); i++) {
                for (uint64_t row = 0; row < data.size(); row++) {
                    if (data.is_null(i, row)) {
                        continue;
                    }
                    if (!lost_columns.empty()) {
                        lost_columns.append(", ");
                    }
                    lost_columns.append("'");
                    // The payload's own alias is the WAL record's name for the column and is
                    // always present on a replayed chunk; the declared list only knows columns
                    // pg_attribute has published to THIS entry, which a failed upstream replay
                    // may never have done.
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

        // The caller's (catalog) ordinals reduced to the ones a row group can actually read.
        // `projected_cols == nullptr` means "every materialized column".
        //
        // An ordinal at or past the physical schema is DROPPED here on purpose: it names a column
        // pg_attribute has and no INSERT has materialized, which null_fill_unmaterialized answers.
        // The result may legitimately be EMPTY — a projection naming only such columns — and that
        // is a row-count-only scan, not an error (see table_scan_state::column_ids). It used to be
        // an empty list handed to a scan that asserted against exactly that.
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

        // Answer the unmaterialized columns: NULL in every row, which is what the materializing
        // INSERT will backfill into these same rows (row_group_t::add_column fills pre-existing
        // rows from the column definition's default, and the definition stage 1b builds carries
        // none). The shape is the append path's own missing-column fill — a typed vector with an
        // all-invalid validity mask — so the two agree by construction.
        //
        // A column the projected chunk ctor left as a buffer-less placeholder is skipped: nothing
        // reads it, and it is dropped at the cursor boundary.
        void null_fill_unmaterialized(vector::data_chunk_t& chunk, uint64_t rows) const {
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
                column.validity().set_all_invalid(rows);
            }
        }

        table::data_table_t& table_;
        std::pmr::memory_resource* resource_;
        // BORROWED, may be null. See the note on the class.
        const std::vector<table::column_definition_t>* unmaterialized_;
    };

} // namespace components::storage