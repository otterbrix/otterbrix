#include "guard_chunk.hpp"

namespace components::operators {

    vector::data_chunk_t make_guard_chunk(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<types::complex_logical_type>& types,
                                          const std::vector<size_t>& projected_cols) {
        if (projected_cols.empty()) {
            return vector::data_chunk_t{resource, types, 0};
        }
        std::pmr::vector<types::complex_logical_type> projected_types(resource);
        projected_types.reserve(projected_cols.size());
        for (size_t column : projected_cols) {
            projected_types.push_back(types[column]);
        }
        return vector::data_chunk_t{resource, projected_types, 0};
    }

} // namespace components::operators
