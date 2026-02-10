#pragma once
#include <components/vector/data_chunk.hpp>

namespace components::operators::impl {

    using value_matrix_t = std::pmr::vector<std::pmr::vector<types::logical_value_t>>;

    value_matrix_t transpose(std::pmr::memory_resource* resource, const vector::data_chunk_t& chunk);
    vector::data_chunk_t transpose(std::pmr::memory_resource* resource,
                                   const value_matrix_t& matrix,
                                   const std::pmr::vector<types::complex_logical_type>& types);

} // namespace components::operators::impl