#include "node_allocate_oids.hpp"

#include <sstream>

namespace components::logical_plan {

    node_allocate_oids_t::node_allocate_oids_t(std::pmr::memory_resource* resource, std::size_t count)
        : node_t(resource, node_type::allocate_oids_t)
        , count_(count) {}

    hash_t node_allocate_oids_t::hash_impl() const { return 0; }

    std::string node_allocate_oids_t::to_string_impl() const {
        std::stringstream s;
        s << "$allocate_oids(" << count_ << ")";
        return s.str();
    }

    node_allocate_oids_ptr make_node_allocate_oids(std::pmr::memory_resource* resource, std::size_t count) {
        return {new node_allocate_oids_t{resource, count}};
    }

} // namespace components::logical_plan