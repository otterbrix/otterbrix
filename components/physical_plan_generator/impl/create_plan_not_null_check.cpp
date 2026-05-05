#include "create_plan_not_null_check.hpp"

#include <components/logical_plan/node_not_null_check.hpp>
#include <components/physical_plan/operators/operator_not_null_check.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_not_null_check(const context_storage_t& context,
                               const components::compute::function_registry_t& function_registry,
                               const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* params) {
        auto* n = static_cast<components::logical_plan::node_not_null_check_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new components::operators::operator_not_null_check_t(
                context.resource,
                context.log.clone(),
                n->not_null_columns()));
        if (!node->children().empty()) {
            plan->set_children(create_plan(context, function_registry, node->children().front(), {}, params));
        }
        return plan;
    }

} // namespace services::planner::impl
