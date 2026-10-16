#include "row_id_batch.hpp"

#include <cassert>

namespace components::table {

    row_ids_t make_row_id_window(std::pmr::memory_resource* resource, uint64_t count) {
        assert(count <= vector::DEFAULT_VECTOR_CAPACITY && "make_row_id_window: a window holds at most "
                                                           "DEFAULT_VECTOR_CAPACITY ids");
        return row_ids_t{vector::vector_t(resource,
                                                types::complex_logical_type{types::logical_type::BIGINT},
                                                /*create_data=*/true,
                                                /*zero_data=*/false,
                                                count),
                               count};
    }

    row_id_batch_t make_row_id_batch(std::pmr::memory_resource* resource, std::span<const int64_t> ids) {
        uint64_t position = 0;
        return make_row_id_batch(resource, ids.size(), [&] { return ids[position++]; });
    }

    row_id_batch_t make_row_id_range(std::pmr::memory_resource* resource, int64_t start, uint64_t count) {
        int64_t next = start;
        return make_row_id_batch(resource, count, [&] { return next++; });
    }

} // namespace components::table
