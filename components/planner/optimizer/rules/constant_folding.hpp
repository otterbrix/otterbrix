#pragma once

#include <components/execution_context/graph_execution_context.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner::optimizer {

    void fold_constants(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr& node,
                        logical_plan::parameter_node_t* parameters,
                        const graph_execution_context& ctx);

} // namespace components::planner::optimizer
