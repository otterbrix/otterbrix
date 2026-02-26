#include "node_checkpoint.hpp"

namespace components::logical_plan {

    node_checkpoint_t::node_checkpoint_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::checkpoint_t, collection_full_name_t{}) {}

    hash_t node_checkpoint_t::hash_impl() const { return 0; }

    std::string node_checkpoint_t::to_string_impl() const {
        return "$checkpoint";
    }

    node_checkpoint_ptr make_node_checkpoint(std::pmr::memory_resource* resource) {
        return {new node_checkpoint_t{resource}};
    }

} // namespace components::logical_plan
