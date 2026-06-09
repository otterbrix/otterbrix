#include "node_cte_scan.hpp"

namespace components::logical_plan {

    node_cte_scan_t::node_cte_scan_t(std::pmr::memory_resource* resource, std::pmr::string cte_name)
        : node_t(resource, node_type::cte_scan_t)
        , cte_name_(std::move(cte_name)) {}

    hash_t node_cte_scan_t::hash_impl() const { return std::hash<std::pmr::string>{}(cte_name_); }

    std::string node_cte_scan_t::to_string_impl() const { return "cte_scan(" + std::string(cte_name_) + ")"; }

    node_cte_scan_ptr make_node_cte_scan(std::pmr::memory_resource* resource, std::pmr::string cte_name) {
        return {new node_cte_scan_t(resource, std::move(cte_name))};
    }

} // namespace components::logical_plan