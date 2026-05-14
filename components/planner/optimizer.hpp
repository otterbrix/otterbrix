#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    // Optimizes logical plan. Called after planner, before physical plan generation.
    // `catalog` is an opaque placeholder kept (as `void*`) for ABI compatibility
    // with call sites that pass `nullptr`; future schema-aware rewrites will
    // reintroduce a real catalog-side parameter.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const void* catalog,
                                    logical_plan::parameter_node_t* parameters);

} // namespace components::planner
