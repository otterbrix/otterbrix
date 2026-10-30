#pragma once

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <vector>

namespace components::operators {

    // A scan's schema'd 0-row chunk, shaped as its real batches: the projected columns of `types` in projection
    // order, or every column when nothing is projected.
    vector::data_chunk_t make_guard_chunk(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<types::complex_logical_type>& types,
                                          const std::vector<size_t>& projected_cols);

    // A page holds the columns the plan reads, of the types it was planned with (`planned` empty: nothing to compare
    // against, a table without declared columns).
    core::error_t check_scan_page(std::pmr::memory_resource* resource,
                                  const std::pmr::string& table,
                                  const std::pmr::vector<types::complex_logical_type>& planned,
                                  const vector::data_chunk_t& page);

    // A chunk a storage answers (a page, a receipt's rows, a host's rows) holds at most DEFAULT_VECTOR_CAPACITY rows, as
    // every chunk does: slicing is the storage's job. Checked before the chunk is read or copied.
    core::error_t check_chunk_width(std::pmr::memory_resource* resource, const vector::data_chunk_t& chunk);

} // namespace components::operators
