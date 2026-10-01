#include "create_plan_fk_check.hpp"

#include <components/logical_plan/node_fk_check.hpp>
#include <components/physical_plan/operators/operator_fk_check.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_fk_check(const context_storage_t& context,
                                       const components::compute::function_registry_t& function_registry,
                                       const components::logical_plan::node_ptr& node,
                                       const components::logical_plan::storage_parameters* params) {
        auto* n = static_cast<components::logical_plan::node_fk_check_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new components::operators::operator_fk_check_t(context.resource, context.log.clone(), n->fk()));
        if (!node->children().empty()) {
            VALUE_OR_RETURN(auto child, create_plan(context, function_registry, node->children().front(), {}, params));
            plan->set_children(std::move(child));
        }
        return plan;
    }

} // namespace services::planner::impl