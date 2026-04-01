#include "operator_batch.hpp"

namespace components::operators {
    operator_batch_t::operator_batch_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks)
        : read_only_operator_t(resource, log_t{}, operator_type::batch)
        , chunks_(std::move(chunks)) {}
} // namespace components::operators
