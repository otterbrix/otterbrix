#include "node_recursive_cte.hpp"

namespace components::logical_plan {

    node_recursive_cte_t::node_recursive_cte_t(std::pmr::memory_resource* resource,
                                               std::pmr::string cte_name,
                                               bool all,
                                               node_ptr anchor,
                                               node_ptr recursive)
        : node_t(resource, node_type::recursive_cte_t)
        , cte_name_(std::move(cte_name))
        , all_(all) {
        append_child(std::move(anchor));
        append_child(std::move(recursive));
    }

    hash_t node_recursive_cte_t::hash_impl() const { return 0; }

    std::string node_recursive_cte_t::to_string_impl() const {
        return "recursive_cte(" + std::string(cte_name_) + ", " + (all_ ? "all" : "distinct") + ")";
    }

    node_recursive_cte_ptr make_node_recursive_cte(std::pmr::memory_resource* resource,
                                                   std::pmr::string cte_name,
                                                   bool all,
                                                   node_ptr anchor,
                                                   node_ptr recursive) {
        return {new node_recursive_cte_t(resource, std::move(cte_name), all, std::move(anchor), std::move(recursive))};
    }

} // namespace components::logical_plan