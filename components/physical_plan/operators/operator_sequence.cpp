#include "operator_sequence.hpp"

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             std::vector<operator_ptr> steps)
        : read_write_operator_t(resource, log, operator_type::sequence)
        , steps_(std::move(steps)) {}

} // namespace components::operators