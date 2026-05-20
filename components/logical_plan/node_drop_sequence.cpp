#include "node_drop_sequence.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_sequence_t::node_drop_sequence_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::drop_sequence_t) {}

    hash_t node_drop_sequence_t::hash_impl() const { return static_cast<hash_t>(relation_oid_); }

    std::string node_drop_sequence_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_sequence: <oid:" << static_cast<std::uint64_t>(relation_oid_) << ">";
        return stream.str();
    }

    node_drop_sequence_ptr make_node_drop_sequence(std::pmr::memory_resource* resource) {
        return {new node_drop_sequence_t{resource}};
    }

} // namespace components::logical_plan
