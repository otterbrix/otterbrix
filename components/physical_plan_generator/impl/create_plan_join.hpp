#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_join(const context_storage_t& context,
                                                               const components::logical_plan::node_ptr& node,
                                                               components::logical_plan::limit_t limit);

}