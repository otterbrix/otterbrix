#pragma once

#include "node.hpp"

#include <components/casts/cast_function.hpp>

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

        // One cast per column applied to the recursive term's rows, and the anchor column type each casts to;
        // an empty cast leaves the column as is.
        void set_step_casts(std::pmr::vector<casts::cast_t> casts,
                            std::pmr::vector<types::complex_logical_type> types) {
            step_casts_ = std::move(casts);
            step_types_ = std::move(types);
        }
        const std::pmr::vector<casts::cast_t>& step_casts() const noexcept { return step_casts_; }
        const std::pmr::vector<types::complex_logical_type>& step_types() const noexcept { return step_types_; }

    private:
        std::pmr::string cte_name_;
        bool all_;
        std::pmr::vector<casts::cast_t> step_casts_;
        std::pmr::vector<types::complex_logical_type> step_types_;

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