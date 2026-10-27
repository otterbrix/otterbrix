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

} // namespace components::operators
