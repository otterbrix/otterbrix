#include "node_sequence.hpp"

namespace components::logical_plan {

    node_sequence_t::node_sequence_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::sequence_t) {}

    hash_t node_sequence_t::hash_impl() const { return 0; }

    std::string node_sequence_t::to_string_impl() const {
        return "$sequence[" + std::to_string(children().size()) + "]";
    }

} // namespace components::logical_plan