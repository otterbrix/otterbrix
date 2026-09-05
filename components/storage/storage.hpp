#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

namespace components::storage {

    // ACTIVE (scan_position_t + storage_t::fetch_next_batch below): the per-batch bounded scan
    // transport, driven by the streaming scan sources via storage_fetch_next_batch.
    // Position-only resume cursor for the streaming fetch-next scan (STEP 3). Holds the
    // absolute next source row to read (row offset from the table start) and the source-row
    // upper bound for this scan; NO pins, NO live scan state — the whole point is that nothing
    // survives a mailbox round-trip. fetch_next_batch re-seeks from `next_row` each call,
    // reads ONE batch, then advances `next_row` and reports `drained`. `next_row >= max_row`
    // (or drained) means the scan is exhausted.
    struct scan_position_t {
        int64_t next_row{0}; // absolute source row to resume from
        int64_t max_row{0};  // exclusive source-row upper bound (table total_rows snapshot)
        bool drained{false}; // set once the underlying scan reports no more rows
    };

    class storage_t {
    public:
        virtual ~storage_t() = default;

        virtual std::pmr::vector<types::complex_logical_type> types() const = 0;
        virtual const std::vector<table::column_definition_t>& columns() const = 0;
        virtual size_t column_count() const = 0;
        virtual bool has_schema() const = 0;
        virtual void adopt_schema(const std::pmr::vector<types::complex_logical_type>& types) = 0;

        virtual uint64_t total_rows() const = 0;
        virtual uint64_t calculate_size() = 0;

        virtual void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) = 0;
        virtual void scan(vector::data_chunk_t& output,
                          const table::table_filter_t* filter,
                          int64_t limit,
                          table::transaction_data /*txn*/) {
            scan(output, filter, limit);
        }

        // Scan only a subset of columns. Caller is expected to have constructed `output`
        // as a sparse data_chunk_t with placeholder vectors for columns outside projected_cols.
        // Default implementation falls back to full scan.
        virtual void scan_projected(vector::data_chunk_t& output,
                                    const table::table_filter_t* filter,
                                    int limit,
                                    const std::vector<size_t>& /*projected_cols*/) {
            scan(output, filter, limit);
        }
        virtual void scan_projected(vector::data_chunk_t& output,
                                    const table::table_filter_t* filter,
                                    int limit,
                                    const std::vector<size_t>& projected_cols,
                                    table::transaction_data /*txn*/) {
            scan_projected(output, filter, limit, projected_cols);
        }

        // Batched scan: emit one ≤DEFAULT_VECTOR_CAPACITY chunk per scan vector directly,
        // avoiding the accumulate-then-split round-trip. `projected_cols == nullptr` means
        // scan all columns; otherwise sparse projection.
        // Returns a buffer-pool OOM / data_corruption error_t surfaced by the table-layer
        // scan; true on success. Default implementation does a regular scan into one chunk
        // (the void scan path leaves no scan_error), so it always reports success; subclasses
        // that drive a batched scan override to read state.table_state.scan_error.
        [[nodiscard]] virtual core::result_wrapper_t<bool> scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                                                                        const table::table_filter_t* filter,
                                                                        int64_t limit,
                                                                        const std::vector<size_t>* projected_cols,
                                                                        table::transaction_data txn) {
            auto t = types();
            vector::data_chunk_t one(resource(), t);
            if (projected_cols) {
                scan_projected(one, filter, static_cast<int>(limit), *projected_cols, txn);
            } else {
                scan(one, filter, limit, txn);
            }
            if (one.size() > 0) {
                batches.push_back(std::move(one));
            }
            return true;
        }

        // Streaming fetch-next (STEP 3 / index-resume). Reads ONE ≤DEFAULT_VECTOR_CAPACITY batch
        // starting at `pos.next_row`, applying `filter`/`projected_cols`/`txn` exactly as
        // scan_batched does, then advances `pos.next_row` past the SOURCE rows consumed and sets
        // `pos.drained` when the scan reaches `pos.max_row`. `output` is filled in place (the
        // caller constructs it with the projected schema). A live cursor is built transiently
        // inside this call and destroyed before it returns, so ZERO buffer pins survive — the
        // resume position alone (pos) is what crosses the mailbox between calls. Returns a
        // buffer-pool OOM / data_corruption error surfaced by the table-layer scan, else true.
        // Default fallback: one scan into `output` from next_row==0 (no resume), then drained.
        [[nodiscard]] virtual core::result_wrapper_t<bool> fetch_next_batch(vector::data_chunk_t& output,
                                                                            scan_position_t& pos,
                                                                            const table::table_filter_t* filter,
                                                                            const std::vector<size_t>* projected_cols,
                                                                            table::transaction_data txn) {
            if (pos.drained) {
                return true;
            }
            if (projected_cols) {
                scan_projected(output, filter, -1, *projected_cols, txn);
            } else {
                scan(output, filter, -1, txn);
            }
            pos.next_row = pos.max_row;
            pos.drained = true;
            return true;
        }

        // projected_cols holds storage chunk indices; EMPTY means every column, which is the same
        // contract fetch_next_batch already uses. Columns outside the set keep their ordinal slot in
        // the output chunk and are left as buffer-less stubs, so a consumer indexes the result the
        // same way whether or not it asked for a projection.
        // Returns the buffer-pool OOM / data_corruption the point-fetch left in
        // column_fetch_state::fetch_error (same value-shape as fetch_next_batch's
        // scan_error above), else true. An error nobody reads is the same silent
        // failure the old abort was replaced to avoid — every caller must check.
        //
        // VISIBILITY IS PART OF THE CALL, and neither parameter has a default (C4b):
        //   SNAPSHOT — the row must be visible to `txn`. Rows that are not are DROPPED, so
        //              the reply is SHORTER than the request and cannot be paired with it by
        //              position; `output.row_ids` names the rows actually carried, in order.
        //   RAW      — no visibility question at all. The only legitimate user is the CREATE
        //              INDEX backfill, which reads deleted rows to recover old key columns.
        // An empty `txn` is NOT the raw mode: it means "see every COMMITTED row", so a
        // committed delete still hides the row from it.
        [[nodiscard]] virtual core::result_wrapper_t<bool> fetch(vector::data_chunk_t& output,
                                                                 const vector::vector_t& row_ids,
                                                                 uint64_t count,
                                                                 const std::vector<size_t>& projected_cols,
                                                                 const table::transaction_data& txn,
                                                                 table::fetch_visibility_t visibility) = 0;

        // THE REPLAY APPEND IS THE TXN APPEND WITH transaction_data{0, 0}, AND NOTHING ELSE.
        // There used to be a second, argument-less `append(data)` beside this one whose body was
        // this body with every result_wrapper_t bound to a [[maybe_unused]] local and asserted
        // instead of returned. Under NDEBUG those asserts are not compiled at all, so the one
        // caller that took it -- the direct-write leg of agent_disk_t::storage_append_inner --
        // read a start_row out of an append state whose append had failed, and answered with it.
        // The duplicate is gone; the direct-write leg passes transaction_data{0, 0} explicitly,
        // which is byte-for-byte the finalize_append the deleted overload performed.
        //
        // Returns write_conflict / out_of_memory surfaced by the table-layer append chain; the
        // start_row on success.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t> append(vector::data_chunk_t& data,
                                                                      table::transaction_data txn) = 0;

        // THE REPLAY UPDATE, and NOT a duplicate of the txn one below it: this writes the rows
        // IN PLACE (update_segment_t), where the txn overload does an MVCC delete + append. It
        // stays, and what it grows is the answer. It was `void`, so
        // agent_disk_t::direct_update_sync -- the WAL-replay update router -- could only end in
        // `return no_error()`, and every refusal underneath (a payload naming a column the
        // storage has not materialised, out_of_memory, write_conflict) was an assert that
        // vanishes under NDEBUG. A committed row recovery declined to restore was reported to
        // base_spaces' replay loop as restored.
        //
        // CONTRACT: recover-then-report. The materialized part of the payload is applied in
        // place; the answer is no_error only when the WHOLE payload landed -- a journalled
        // value dropped with an unmaterialized column, or a row id the storage cannot hold
        // (fewer rows applied than named), comes back as an error NAMING the loss, after the
        // restorable part is already written. The txn overload keeps refusing up front
        // instead: there the statement can still be declined before anything is journalled.
        [[nodiscard]] virtual core::error_t update(vector::vector_t& row_ids, vector::data_chunk_t& data) = 0;
        // Returns write_conflict / out_of_memory from the table-layer update; on success
        // {start_row, affected-row count}. PURE, like the txn append above it: the default
        // body that used to sit here forwarded to the replay overload and answered a
        // constant {0, 0} for the count -- a fallback whose only consumer was a test double,
        // which now overrides this itself.
        [[nodiscard]] virtual core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) = 0;

        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) = 0;

        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t /*txn_id*/) {
            return delete_rows(row_ids, count);
        }
        virtual void commit_append(uint64_t /*commit_id*/, int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void revert_append(int64_t /*row_start*/, uint64_t /*count*/) {}
        virtual void commit_all_deletes(uint64_t /*txn_id*/, uint64_t /*commit_id*/) {}
        virtual void revert_all_deletes(uint64_t /*txn_id*/) {}

        virtual std::pmr::memory_resource* resource() const = 0;
    };

} // namespace components::storage