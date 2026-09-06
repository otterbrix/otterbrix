#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/update_passkey.hpp>
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
        // Error, if any, is in output's column_fetch_state::fetch_error — `true` alone does
        // not mean no error, callers must check it.
        // SNAPSHOT: rows invisible to `txn` are dropped, so the reply is SHORTER than the
        // request; `output.row_ids` names what actually came back. RAW: no visibility check,
        // used only by CREATE INDEX backfill to recover deleted rows' key columns. An empty
        // `txn` is NOT raw — it means "every committed row", so a committed delete still hides.
        [[nodiscard]] virtual core::result_wrapper_t<bool> fetch(vector::data_chunk_t& output,
                                                                 const vector::vector_t& row_ids,
                                                                 uint64_t count,
                                                                 const std::vector<size_t>& projected_cols,
                                                                 const table::transaction_data& txn,
                                                                 table::fetch_visibility_t visibility) = 0;

        // No default `append(data)` overload: an assert-based fallback compiles away under
        // NDEBUG, silently reusing a failed append's start_row. The replay path passes
        // transaction_data{0, 0} explicitly instead. Returns write_conflict / out_of_memory
        // from the table-layer append chain; start_row on success.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t> append(vector::data_chunk_t& data,
                                                                      table::transaction_data txn) = 0;

        // Replay update: rewrites rows IN PLACE (vs. the txn overload's MVCC delete+append
        // below). Returns error_t, not void, so a refusal (unmaterialised column, out_of_memory,
        // write_conflict) can't be swallowed by an NDEBUG-only assert and reported as restored.
        // Recover-then-report: the materialized part of the payload is written even on
        // failure; no_error means the WHOLE payload landed. The txn overload instead refuses
        // up front, before anything is journalled.
        // Passkey-gated: no undo and no conflict detection below (see update_passkey.hpp), so
        // only a caller that can mint the access token compiles.
        [[nodiscard]] virtual core::error_t update(table::nontransactional_update_access_t access,
                                                   vector::vector_t& row_ids,
                                                   vector::data_chunk_t& data) = 0;
        // Returns write_conflict / out_of_memory from the table-layer update; on success
        // {start_row, affected-row count}. No default body: forwarding to the replay overload
        // could only fake a {0, 0} count.
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