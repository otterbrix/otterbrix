#include "create_plan_check_constraint.hpp"

#include <components/logical_plan/node_check_constraint.hpp>
#include <components/physical_plan/operators/operator_check_constraint.hpp>
#include <components/physical_plan/operators/operator_unique_constraint.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    namespace {
        // if there were no constants used than parameter_map_t was not created
        components::types::parameter_map_t
        enforce_param_map(const components::logical_plan::node_check_constraint_t* n) {
            if (!n->check_params()) {
                return components::types::parameter_map_t{n->resource()};
            }
            return n->check_params()->parameters().parameters;
        }
    } // namespace

    components::operators::operator_ptr
    create_plan_check_constraint(const context_storage_t& context,
                                 const components::compute::function_registry_t& function_registry,
                                 const components::logical_plan::node_ptr& node,
                                 const components::logical_plan::storage_parameters* params) {
        auto* n = static_cast<components::logical_plan::node_check_constraint_t*>(node.get());
        auto plan = boost::intrusive_ptr(new components::operators::operator_check_constraint_t(context.resource,
                                                                                                context.log.clone(),
                                                                                                n->not_null_columns(),
                                                                                                n->check_predicates(),
                                                                                                n->array_size_reqs(),
                                                                                                enforce_param_map(n)));
        // Child sub-plan (the DML sink, possibly under an fk_check chain).
        components::operators::operator_ptr child;
        if (!node->children().empty()) {
            child = create_plan(context, function_registry, node->children().front(), {}, params);
        }

        // When the table carries UNIQUE / PRIMARY KEY constraints, splice an
        // operator_unique_constraint_t BETWEEN the check sink and the DML so both
        // constraint operators validate the SAME written-row snapshot (the unique op
        // reads its child DML's constraint_input(), exactly like an fk_check chain).
        // With no unique groups the check sink adopts the child directly.
        if (!n->unique_groups().empty()) {
            auto unique =
                boost::intrusive_ptr(new components::operators::operator_unique_constraint_t(context.resource,
                                                                                             context.log.clone(),
                                                                                             n->table_oid(),
                                                                                             n->unique_groups()));
            if (child) {
                unique->set_children(child);
            }
            plan->set_children(unique);
        } else if (child) {
            plan->set_children(child);
        }
        return plan;
    }

} // namespace services::planner::impl