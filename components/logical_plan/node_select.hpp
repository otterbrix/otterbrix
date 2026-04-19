#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_select_t final : public node_t {
    public:
        explicit node_select_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        // Number of hidden aggregate expressions appended at the tail of expressions_
        // (used for HAVING internal aggregates when there is no GROUP BY).
        // Visible SELECT column count = expressions_.size() - internal_aggregate_count.
        size_t internal_aggregate_count{0};

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_select_ptr = boost::intrusive_ptr<node_select_t>;

    node_select_ptr make_node_select(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

} // namespace components::logical_plan
