#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // Explicit projection node introduced by main's PR #479. Sits as a child
    // of node_aggregate_t and holds the SELECT-clause column expressions; the
    // aggregate parent carries the table identity.
    class node_select_t final : public node_t {
    public:
        explicit node_select_t(std::pmr::memory_resource* resource);

        // Number of hidden aggregate expressions appended at the tail of expressions_
        // (used for HAVING internal aggregates when there is no GROUP BY).
        // Visible SELECT column count = expressions_.size() - internal_aggregate_count.
        size_t internal_aggregate_count{0};

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_select_ptr = boost::intrusive_ptr<node_select_t>;

    node_select_ptr make_node_select(std::pmr::memory_resource* resource);

} // namespace components::logical_plan