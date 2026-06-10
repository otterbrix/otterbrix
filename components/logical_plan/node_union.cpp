#include "node_union.hpp"

namespace components::logical_plan {

    node_union_t::node_union_t(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all)
        : node_t(resource, node_type::union_t)
        , all_(all) {
        append_child(std::move(left));
        append_child(std::move(right));
    }

    hash_t node_union_t::hash_impl() const { return 0; }

    std::string node_union_t::to_string_impl() const { return all_ ? "$union_all" : "$union"; }

    node_union_ptr make_node_union(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all) {
        return {new node_union_t{resource, std::move(left), std::move(right), all}};
    }

} // namespace components::logical_plan