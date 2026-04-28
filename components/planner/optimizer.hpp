#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::catalog {
    class catalog;
}

namespace components::planner {

    // Optimizes logical plan. Called after planner, BEFORE the schema validator.
    // Safe rules only: those that don't need resolved column paths.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const catalog::catalog* catalog,
                                    logical_plan::parameter_node_t* parameters);

    // Second optimization pass, run AFTER validate_schema has resolved column paths.
    // Rules that need to reason about physical column indices go here (e.g. column pruning).
    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource,
                                                  logical_plan::node_ptr node,
                                                  const catalog::catalog* catalog);

} // namespace components::planner
