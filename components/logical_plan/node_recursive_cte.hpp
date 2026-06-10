#pragma once

#include "node.hpp"

#include <memory_resource>
#include <string>

namespace components::logical_plan {

    class node_recursive_cte_t final : public node_t {
    public:
        node_recursive_cte_t(std::pmr::memory_resource* resource,
                             std::pmr::string cte_name,
                             bool all,
                             node_ptr anchor,
                             node_ptr recursive);

        const std::pmr::string& cte_name() const noexcept { return cte_name_; }
        bool all() const noexcept { return all_; }

    private:
        std::pmr::string cte_name_;
        bool all_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_recursive_cte_ptr = boost::intrusive_ptr<node_recursive_cte_t>;

    node_recursive_cte_ptr make_node_recursive_cte(std::pmr::memory_resource* resource,
                                                   std::pmr::string cte_name,
                                                   bool all,
                                                   node_ptr anchor,
                                                   node_ptr recursive);

} // namespace components::logical_plan