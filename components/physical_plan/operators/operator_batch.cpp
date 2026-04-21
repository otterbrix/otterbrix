#include "operator_batch.hpp"

namespace components::operators {
    operator_batch_t::operator_batch_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks)
        : read_only_operator_t(resource, log_t{}, operator_type::batch) {
        set_output(make_operator_data(resource, std::move(chunks)));
        mark_executed();
    }
} // namespace components::operators
