#include "create_plan_aggregate.hpp"

#include <components/physical_plan/collection/operators/aggregation.hpp>
#include <components/physical_plan/table/operators/aggregation.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::collection::planner::impl {

    using components::logical_plan::node_type;

    components::collection::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit) {
        auto op = boost::intrusive_ptr(
            new components::collection::operators::aggregation(context.at(node->collection_full_name())));
        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    op->set_match(create_plan(context, child, limit));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, child, limit));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, child, limit));
                    break;
                default:
                    op->set_children(create_plan(context, child, limit));
                    break;
            }
        }
        return op;
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    using components::logical_plan::node_type;

    components::base::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::compute::function_registry_t& function_registry,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit) {
        auto op = boost::intrusive_ptr(
            new components::table::operators::aggregation(context.at(node->collection_full_name())));
        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    op->set_match(create_plan(context, function_registry, child, limit));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, function_registry, child, limit));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, function_registry, child, limit));
                    break;
                default:
                    op->set_children(create_plan(context, function_registry, child, limit));
                    break;
            }
        }
        return op;
    }

} // namespace services::table::planner::impl
