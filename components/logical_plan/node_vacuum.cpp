#include "node_vacuum.hpp"

namespace components::logical_plan {

    node_vacuum_t::node_vacuum_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::vacuum_t, collection_full_name_t{}) {}

    hash_t node_vacuum_t::hash_impl() const { return 0; }

    std::string node_vacuum_t::to_string_impl() const {
        return "$vacuum";
    }

    void node_vacuum_t::serialize_impl(serializer::msgpack_serializer_t* /*serializer*/) const {
    }

    node_vacuum_ptr make_node_vacuum(std::pmr::memory_resource* resource) {
        return {new node_vacuum_t{resource}};
    }

} // namespace components::logical_plan
