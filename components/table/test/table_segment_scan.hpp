#pragma once

// Test-side whole-table drain (the old data_table_t::scan_table_segment).
//
// A1 reduced the production disk-read contract to exactly two legs — streaming-by-predicate
// (storage_fetch_next_batch) and point-by-row-id (storage_fetch) — and removed
// storage_scan_segment, the method's only production consumer. The shadow-paging tests keep
// it as the canonical "read the whole table back" idiom, so the body moved here VERBATIM
// (rebuilt on the public API: columns(), row_group()->initialize_scan_with_offset(),
// table_scan_state::initialize(), scan_committed()); it must stay observationally identical
// because the tests assert on the exact rows it yields.
//
// The callback is a template parameter, not std::function (rule 14).

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory_resource>
#include <vector>

#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>

namespace otterbrix_test {

    // Scans committed rows [row_start, row_start + count) of `table`, handing each
    // ≤DEFAULT_VECTOR_CAPACITY chunk to `function(vector::data_chunk_t&)`.
    template<typename Callback>
    void scan_table_segment(components::table::data_table_t& table,
                            int64_t row_start,
                            uint64_t count,
                            Callback&& function) {
        if (count == 0) {
            return;
        }
        auto collection = table.row_group();
        auto* resource = collection->resource();
        int64_t end = row_start + static_cast<int64_t>(count);

        std::vector<components::table::storage_index_t> column_ids;
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        const auto& columns = table.columns();
        for (uint64_t i = 0; i < columns.size(); i++) {
            auto& col = columns[i];
            column_ids.emplace_back(i);
            types.push_back(col.type());
        }
        components::vector::data_chunk_t chunk(resource, types);

        components::table::create_index_scan_state state(resource);

        // The two halves of the old (private) data_table_t::initialize_scan_with_offset.
        state.initialize(column_ids);
        collection->initialize_scan_with_offset(state.table_state,
                                                column_ids,
                                                row_start,
                                                row_start + static_cast<int64_t>(count));
        // vector_index is stamped in collection-absolute space by initialize_scan_with_offset, so
        // vector_index*CAP is already the vector-aligned absolute start row (do NOT re-add row_group
        // start — that would double-count the group origin).
        auto row_start_aligned = static_cast<int64_t>(state.table_state.vector_index *
                                                      components::vector::DEFAULT_VECTOR_CAPACITY);

        int64_t current_row = row_start_aligned;
        while (current_row < end) {
            state.table_state.scan_committed(chunk, components::table::table_scan_type::COMMITTED_ROWS);
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
                components::vector::indexing_vector_t indexing(resource, start_in_chunk, chunk_count);
                chunk.slice(indexing, chunk_count);
            }
            function(chunk);
            chunk.reset();
            current_row = end_row;
        }
    }

} // namespace otterbrix_test
