#include "create_plan_join.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/collection/operators/operator_join.hpp>
#include <components/physical_plan/table/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::collection::planner::impl {

    components::collection::operators::operator_ptr create_plan_join(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node,
                                                                     components::logical_plan::limit_t limit) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        // assign left collection as actor for join
        auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
        auto collection_context = context.at(node->children().front()->collection_full_name());
        auto predicate = components::collection::operators::predicates::create_predicate(*expr);
        auto join = boost::intrusive_ptr(new components::collection::operators::operator_join_t(collection_context,
                                                                                                join_node->type(),
                                                                                                std::move(predicate)));
        components::collection::operators::operator_ptr left;
        components::collection::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, node->children().front(), limit);
        }
        if (node->children().back()) {
            right = create_plan(context, node->children().back(), limit);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    components::base::operators::operator_ptr
    create_plan_join(const context_storage_t& context,
                     const components::compute::function_registry_t& function_registry,
                     const components::logical_plan::node_ptr& node,
                     components::logical_plan::limit_t limit) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        // assign left table as actor for join
        auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
        auto collection_context = context.at(node->children().front()->collection_full_name());
        auto join = boost::intrusive_ptr(
            new components::table::operators::operator_join_t(collection_context, join_node->type(), *expr));
        components::base::operators::operator_ptr left;
        components::base::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, function_registry, node->children().front(), limit);
        }
        if (node->children().back()) {
            right = create_plan(context, function_registry, node->children().back(), limit);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::table::planner::impl
