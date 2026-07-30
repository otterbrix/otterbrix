#include "create_plan_union.hpp"

#include <components/logical_plan/node_union.hpp>
#include <components/physical_plan/operators/operator_union.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_union(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node,
                      const components::logical_plan::storage_parameters* params) {
        const auto* union_node = static_cast<const components::logical_plan::node_union_t*>(node.get());

        auto left_op = create_plan(context,
                                   function_registry,
                                   union_node->left(),
                                   components::logical_plan::limit_t::unlimit(),
                                   params);
        auto right_op = create_plan(context,
                                    function_registry,
                                    union_node->right(),
                                    components::logical_plan::limit_t::unlimit(),
                                    params);

        auto op = boost::intrusive_ptr(
            new components::operators::operator_union_t(context.resource, context.log.clone(), union_node->all()));
        // Forward the validator-stamped, reconciled union schema (validate_schema's
        // union_t case) — the operator types its output from this stamp, not from the
        // row data the branches happen to produce.
        op->set_output_schema(node->output_schema());
        op->set_children(std::move(left_op), std::move(right_op));
        return op;
    }

} // namespace services::planner::impl