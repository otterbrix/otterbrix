#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Copies WHERE predicates that reference both sides of a comma join onto the
    // synthesized cross join. WHERE is left untouched, so this only adds a join filter.
    void push_down_join_predicates(const logical_plan::node_ptr& root);

} // namespace components::planner::optimizer
