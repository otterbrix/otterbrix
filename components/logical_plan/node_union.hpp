#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_union_t final : public node_t {
    public:
        node_union_t(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all);

        bool all() const noexcept { return all_; }

        // A union is binary: the ctor appends [left, right] and nothing else touches
        // children(). Total on a partially built node (a null node_ptr).
        const node_ptr& left() const noexcept { return child_or_null(0); }
        const node_ptr& right() const noexcept { return child_or_null(1); }

    private:
        bool all_;

        std::string to_string_impl() const override;
    };

    using node_union_ptr = boost::intrusive_ptr<node_union_t>;

    node_union_ptr make_node_union(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all);

} // namespace components::logical_plan