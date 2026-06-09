#pragma once

#include "node.hpp"

#include <memory_resource>
#include <string>

namespace components::logical_plan {

    class node_cte_scan_t final : public node_t {
    public:
        node_cte_scan_t(std::pmr::memory_resource* resource, std::pmr::string cte_name);

        const std::pmr::string& cte_name() const noexcept { return cte_name_; }

    private:
        std::pmr::string cte_name_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_cte_scan_ptr = boost::intrusive_ptr<node_cte_scan_t>;

    node_cte_scan_ptr make_node_cte_scan(std::pmr::memory_resource* resource, std::pmr::string cte_name);

} // namespace components::logical_plan