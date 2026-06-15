#include "node_set_ef_search.hpp"

namespace components::logical_plan {

    node_set_ef_search_t::node_set_ef_search_t(std::pmr::memory_resource* resource, std::size_t ef_search)
        : node_t(resource, node_type::set_ef_search_t)
        , ef_search_(ef_search) {}

    hash_t node_set_ef_search_t::hash_impl() const { return 0; }

    std::string node_set_ef_search_t::to_string_impl() const { return "$set_ef_search"; }

    node_set_ef_search_ptr make_node_set_ef_search(std::pmr::memory_resource* resource, std::size_t ef_search) {
        return {new node_set_ef_search_t{resource, ef_search}};
    }

} // namespace components::logical_plan
