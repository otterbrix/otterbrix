#include "create_plan_join.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_join(const context_storage_t& context,
                     const components::compute::function_registry_t& function_registry,
                     const components::logical_plan::node_ptr& node,
                     components::logical_plan::limit_t limit) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        // assign left table as actor for join
        // Try left child context first, fall back to right (one side may be raw data with nullptr context)
        auto left_name = node->children().front()->collection_full_name();
        auto right_name = node->children().back()->collection_full_name();
        bool known = context.has_collection(left_name) || context.has_collection(right_name);
        auto coll_name = context.has_collection(left_name) ? left_name : right_name;
        auto join = known
            ? boost::intrusive_ptr(
                new components::operators::operator_join_t(context.resource, context.log.clone(),
                                                          join_node->type(), node->expressions()[0]))
            : boost::intrusive_ptr(
                new components::operators::operator_join_t(nullptr, log_t{},
                                                          join_node->type(), node->expressions()[0]));
        components::operators::operator_ptr left;
        components::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, function_registry, node->children().front(), limit);
        }
        if (node->children().back()) {
            right = create_plan(context, function_registry, node->children().back(), limit);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl