#include "create_plan_union.hpp"

#include <components/logical_plan/node_union.hpp>
#include <components/physical_plan/operators/operator_union.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_union(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node,
                      components::logical_plan::limit_t limit,
                      const components::logical_plan::storage_parameters* params) {
        const auto* union_node = static_cast<const components::logical_plan::node_union_t*>(node.get());

        auto left_op = create_plan(context, function_registry, node->children()[0], limit, params);
        auto right_op = create_plan(context, function_registry, node->children()[1], limit, params);

        auto op = boost::intrusive_ptr(
            new components::operators::operator_union_t(context.resource, context.log.clone(), union_node->all()));
        op->set_children(std::move(left_op), std::move(right_op));
        return op;
    }

} // namespace services::planner::impl