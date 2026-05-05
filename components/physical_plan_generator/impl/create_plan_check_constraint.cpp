#include "create_plan_check_constraint.hpp"

#include <components/logical_plan/node_check_constraint.hpp>
#include <components/physical_plan/operators/operator_check_constraint.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_check_constraint(const context_storage_t& context,
                                 const components::compute::function_registry_t& function_registry,
                                 const components::logical_plan::node_ptr& node,
                                 const components::logical_plan::storage_parameters* params) {
        using Op = components::operators::operator_check_constraint_t;
        auto* n = static_cast<components::logical_plan::node_check_constraint_t*>(node.get());

        std::vector<Op::check_entry_t> checks;
        checks.reserve(n->checks().size());
        for (const auto& e : n->checks()) {
            checks.push_back({e.predicate, e.conexpr});
        }

        auto plan = boost::intrusive_ptr(
            new Op(context.resource,
                   context.log.clone(),
                   n->not_null_columns(),
                   std::move(checks)));

        if (!node->children().empty()) {
            plan->set_children(create_plan(context, function_registry, node->children().front(), {}, params));
        }
        return plan;
    }

} // namespace services::planner::impl
