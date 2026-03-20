#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::catalog {
    class catalog;
}

namespace components::planner {

    // Optimizes logical plan. Called after planner, before physical plan generation.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const catalog::catalog* catalog,
                                    logical_plan::parameter_node_t* parameters);

} // namespace components::planner
