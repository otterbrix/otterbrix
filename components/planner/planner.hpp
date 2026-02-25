#pragma once

#include <components/logical_plan/node.hpp>

namespace components::catalog {
    class catalog;
}

namespace components::planner {

    class planner_t {
    public:
        auto create_plan(std::pmr::memory_resource* resource,
                         logical_plan::node_ptr node,
                         const catalog::catalog* catalog = nullptr) -> logical_plan::node_ptr;
    };

} // namespace components::planner
