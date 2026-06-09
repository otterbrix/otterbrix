#include "create_plan_recursive_cte.hpp"

#include <components/logical_plan/node_recursive_cte.hpp>
#include <components/physical_plan/operators/operator_recursive_cte.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_recursive_cte(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              components::logical_plan::limit_t limit,
                              const components::logical_plan::storage_parameters* params) {
        const auto* cte_node = static_cast<const components::logical_plan::node_recursive_cte_t*>(node.get());

        auto op = boost::intrusive_ptr(new components::operators::operator_recursive_cte_t(context.resource,
                                                                                           context.log.clone(),
                                                                                           cte_node->all()));

        // Build anchor using the original context (no cte_working_sets entry needed).
        auto anchor_op = create_plan(context, function_registry, cte_node->children()[0], limit, params);

        // Build the recursive member with the working-set slot injected into context.
        context_storage_t recursive_context = context;
        recursive_context.cte_working_sets[cte_node->cte_name()] = op->working_set_slot();

        auto recursive_op = create_plan(recursive_context, function_registry, cte_node->children()[1], limit, params);

        op->set_children(std::move(anchor_op), std::move(recursive_op));
        return op;
    }

} // namespace services::planner::impl
