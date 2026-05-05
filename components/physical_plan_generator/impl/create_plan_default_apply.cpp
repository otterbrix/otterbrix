#include "create_plan_default_apply.hpp"

#include <components/logical_plan/node_default_apply.hpp>
#include <components/physical_plan/operators/operator_default_apply.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_default_apply(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              const components::logical_plan::storage_parameters* params) {
        auto* n = static_cast<components::logical_plan::node_default_apply_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new components::operators::operator_default_apply_t(
                context.resource,
                context.log.clone(),
                n->defaults()));
        if (!node->children().empty()) {
            plan->set_children(create_plan(context, function_registry, node->children().front(), {}, params));
        }
        return plan;
    }

} // namespace services::planner::impl
