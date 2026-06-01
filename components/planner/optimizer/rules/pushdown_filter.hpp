#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    logical_plan::node_ptr pushdown_filter(std::pmr::memory_resource* resource,
                                           logical_plan::node_ptr node);

} // namespace components::planner::optimizer
