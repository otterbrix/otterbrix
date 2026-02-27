#include "create_plan_aggregate.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/aggregation.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    components::operators::operator_ptr create_plan_aggregate(const context_storage_t& context,
                                                                const components::compute::function_registry_t& function_registry,
                                                                    const components::logical_plan::node_ptr& node,
                                                                    components::logical_plan::limit_t limit,
                                                                    const components::logical_plan::storage_parameters* params) {
        // First pass: extract limit from limit child (if any)
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::limit_t) {
                const auto* limit_node =
                    static_cast<const components::logical_plan::node_limit_t*>(child.get());
                limit = limit_node->limit();
                break;
            }
        }

        auto coll_name = node->collection_full_name();
        auto op = context.has_collection(coll_name)
            ? boost::intrusive_ptr(new components::operators::aggregation(context.resource, context.log.clone(), coll_name))
            : boost::intrusive_ptr(new components::operators::aggregation(node->resource(), log_t{}, coll_name));
        op->set_limit(limit);
        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t:
                    op->set_match(create_plan(context, function_registry, child, limit, params));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, function_registry, child, limit, params));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, function_registry, child, limit, params));
                    break;
                case node_type::having_t:
                    op->set_having(create_plan(context, function_registry, child, limit, params));
                    break;
                default:
                    op->set_children(create_plan(context, function_registry, child, limit, params));
                    break;
            }
        }
        // Check if DISTINCT flag is set on the aggregate node
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        if (agg_node->is_distinct()) {
            auto distinct_op = context.has_collection(coll_name)
                ? boost::intrusive_ptr(new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            op->set_distinct(std::move(distinct_op));
        }
        return op;
    }

} // namespace services::planner::impl