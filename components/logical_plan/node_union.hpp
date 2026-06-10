#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_union_t final : public node_t {
    public:
        node_union_t(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all);

        bool all() const noexcept { return all_; }

    private:
        bool all_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_union_ptr = boost::intrusive_ptr<node_union_t>;

    node_union_ptr make_node_union(std::pmr::memory_resource* resource, node_ptr left, node_ptr right, bool all);

} // namespace components::logical_plan