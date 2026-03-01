#include "node_drop_sequence.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_sequence_t::node_drop_sequence_t(std::pmr::memory_resource* resource, const collection_full_name_t& name)
        : node_t(resource, node_type::drop_sequence_t, name) {}

    hash_t node_drop_sequence_t::hash_impl() const { return 0; }

    std::string node_drop_sequence_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_sequence: " << database_name() << "." << collection_name();
        return stream.str();
    }

    node_drop_sequence_ptr make_node_drop_sequence(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& name) {
        return {new node_drop_sequence_t{resource, name}};
    }

} // namespace components::logical_plan
