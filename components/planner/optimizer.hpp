#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

// V4 catalog facade. Forward-declared so optimizer stays free of dispatcher-layer
// includes; future schema-aware rewrites will pull catalog_view.hpp explicitly.
namespace services::dispatcher {
    class catalog_view_t;
}

namespace components::planner {

    // Optimizes logical plan. Called after planner, before physical plan generation.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const services::dispatcher::catalog_view_t* catalog,
                                    logical_plan::parameter_node_t* parameters);

} // namespace components::planner
