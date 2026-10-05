#pragma once

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <vector>

namespace components::operators {

    // A scan's schema'd 0-row chunk. Pruned scans emit full-width chunks whose non-projected columns are buffer-less
    // placeholders, so column ordinals stay stable plan-wide (PR #477); the guard has the same shape as real batches,
    // since operators above index it by table ordinal.
    vector::data_chunk_t make_guard_chunk(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<types::complex_logical_type>& types,
                                          const std::vector<size_t>& projected_cols);

} // namespace components::operators
