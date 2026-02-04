#include "transformation.hpp"

namespace components::table::operators::impl {

    value_matrix_t transpose(std::pmr::memory_resource* resource, const vector::data_chunk_t& chunk) {
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> matrix(resource);
        matrix.reserve(chunk.size());
        for (size_t i = 0; i < chunk.size(); i++) {
            matrix.emplace_back(std::pmr::vector<types::logical_value_t>{resource});
            matrix.back().reserve(chunk.column_count());
        }
        for (size_t i = 0; i < chunk.size(); i++) {
            for (size_t j = 0; j < chunk.column_count(); j++) {
                matrix[i].emplace_back(chunk.value(j, i));
            }
        }

        return matrix;
    }

    vector::data_chunk_t transpose(std::pmr::memory_resource* resource,
                                   const value_matrix_t& matrix,
                                   const std::pmr::vector<types::complex_logical_type>& types) {
        auto chunk = vector::data_chunk_t(resource, types, matrix.size());
        for (size_t i = 0; i < matrix.size(); i++) {
            for (size_t j = 0; j < types.size(); j++) {
                chunk.set_value(j, i, matrix[i][j]);
            }
        }
        chunk.set_cardinality(matrix.size());
        return chunk;
    }

} // namespace components::table::operators::impl