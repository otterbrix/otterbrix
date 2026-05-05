#include "node_sequence.hpp"

#include <sstream>

namespace components::logical_plan {

    node_sequence_t::node_sequence_t(std::pmr::memory_resource*    resource,
                                      const collection_full_name_t& collection)
        : node_t(resource, node_type::sequence_t, collection) {}

    hash_t      node_sequence_t::hash_impl()      const { return 0; }
    std::string node_sequence_t::to_string_impl() const {
        std::ostringstream s;
        s << "$sequence[" << children_.size() << "]: " << collection_name();
        return s.str();
    }

} // namespace components::logical_plan
