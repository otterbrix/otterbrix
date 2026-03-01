#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_vacuum_t final : public node_t {
    public:
        explicit node_vacuum_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_vacuum_ptr = boost::intrusive_ptr<node_vacuum_t>;
    node_vacuum_ptr make_node_vacuum(std::pmr::memory_resource* resource);

} // namespace components::logical_plan
