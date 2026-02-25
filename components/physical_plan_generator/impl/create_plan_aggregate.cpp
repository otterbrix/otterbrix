#include "create_plan_aggregate.hpp"

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    components::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::compute::function_registry_t& function_registry,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit) {
        // First pass: extract limit from limit child (if any)
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::limit_t) {
                const auto* limit_node = static_cast<const components::logical_plan::node_limit_t*>(child.get());
                limit = limit_node->limit();
                break;
            }
        }

        auto coll_name = node->collection_full_name();
        auto* plan_resource = context.has_collection(coll_name) ? context.resource : node->resource();

        // Build operator chain directly: scan/child → match → group → sort
        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr child_op;

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t:
                    match_op = create_plan(context, function_registry, child, limit);
                    break;
                case node_type::group_t:
                    group_op = create_plan(context, function_registry, child, limit);
                    break;
                case node_type::sort_t:
                    sort_op = create_plan(context, function_registry, child, limit);
                    break;
                default:
                    child_op = create_plan(context, function_registry, child, limit);
                    break;
            }
        }

        // Build chain: base → match → group → sort
        // When sort is present, scan all rows — limit is applied after sort
        auto scan_limit = sort_op ? components::logical_plan::limit_t::unlimit() : limit;

        components::operators::operator_ptr executor;
        if (child_op) {
            executor = std::move(child_op);
            if (match_op) {
                match_op->set_children(std::move(executor));
                executor = std::move(match_op);
            }
        } else {
            executor = match_op
                           ? std::move(match_op)
                           : static_cast<components::operators::operator_ptr>(
                                 boost::intrusive_ptr(new components::operators::transfer_scan(
                                     plan_resource, coll_name, scan_limit)));
        }
        if (group_op) {
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (sort_op) {
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }

        return executor;
    }

} // namespace services::planner::impl
