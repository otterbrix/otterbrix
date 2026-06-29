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
        virtual void overlay_not_null(const std::string& col_name) = 0;

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

        virtual void fetch(vector::data_chunk_t& output, const vector::vector_t& row_ids, uint64_t count) = 0;

        virtual void scan_segment(int64_t start,
                                  uint64_t count,
                                  const std::function<void(vector::data_chunk_t& chunk)>& callback) = 0;

        virtual uint64_t append(vector::data_chunk_t& data) = 0;

        virtual void update(vector::vector_t& row_ids, vector::data_chunk_t& data) = 0;
        // Returns write_conflict / out_of_memory from the table-layer update; on success
        // {0, affected-row count}. Default fallback drives the void overload (replay path:
        // no error surfacing).
        [[nodiscard]] virtual core::result_wrapper_t<std::pair<int64_t, uint64_t>>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data /*txn*/) {
            update(row_ids, data);
            return std::pair<int64_t, uint64_t>{0, 0};
        }

        virtual uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) = 0;

        // Txn-aware overloads with default fallbacks. Returns write_conflict / out_of_memory
        // surfaced by the table-layer append chain; the start_row on success.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t> append(vector::data_chunk_t& data,
                                                                      table::transaction_data /*txn*/) {
            return append(data);
        }
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