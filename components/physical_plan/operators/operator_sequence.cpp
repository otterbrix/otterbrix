#include "operator_sequence.hpp"

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::sequence) {}

} // namespace components::operators