#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner::optimizer {

    // Recursively walks the node tree, folding constant expressions
    // (arithmetic on resolved parameters) into new parameter values.
    void fold_constants_recursive(std::pmr::memory_resource* resource,
                                  const logical_plan::node_ptr& node,
                                  logical_plan::parameter_node_t* parameters);

} // namespace components::planner::optimizer
