#pragma once

#include <components/physical_plan_generator/create_plan.hpp>

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // The operator gets its own copy of the node's function.
    components::operators::operator_ptr create_plan_register_udf(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
