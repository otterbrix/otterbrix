#pragma once

#include <components/physical_plan_generator/create_plan.hpp>

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_data(const components::logical_plan::node_ptr& node);
}
