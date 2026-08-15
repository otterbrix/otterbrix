#include "create_plan_insert.hpp"

#include "create_plan_select.hpp"
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_insert(const context_storage_t& context,
                       const components::compute::function_registry_t& function_registry,
                       const components::logical_plan::node_ptr& node,
                       const components::logical_plan::storage_parameters* params) {
        const auto* node_insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_insert->returning(), params);
        // Forward the plan-resolved RETURNING output types (stamped on the insert node by
        // validate_schema) onto the projection columns, in projection order, so a
        // CASE/COALESCE/deep-field RETURNING column over the appended rows stays correctly
        // typed instead of being dropped as an untyped (NA) placeholder. evaluate_projection
        // reads col.result_type authoritatively. Mirrors create_plan_aggregate's select path.
        // No RETURNING -> output_types() is empty -> guard skips (no-op). No data-derived
        // fallback (rule 6): a column without a resolved type stays unset.
        if (node->has_output_types()) {
            const auto& out_types = node->output_types();
            for (size_t i = 0; i < returning.size() && i < out_types.size(); ++i) {
                returning[i].result_type = out_types[i];
            }
        }
        auto plan = boost::intrusive_ptr(new components::operators::operator_insert(context.resource,
                                                                                    context.log.clone(),
                                                                                    node->table_oid(),
                                                                                    std::move(returning)));
        // The validator resolved, per incoming column, the target it lands in and the cast
        // that stores it there. The append is name-based, so the operator renames the
        // streamed columns to their targets as it converts them.
        components::logical_plan::insert_column_bindings_t bindings(context.resource);
        bindings.reserve(node_insert->column_bindings().size());
        for (const auto& binding : node_insert->column_bindings()) {
            bindings.emplace_back(components::logical_plan::insert_column_binding_t{
                .target_index = binding.target_index,
                .target_name = std::pmr::string{binding.target_name.c_str(), context.resource},
                .target_type = binding.target_type,
                .cast = binding.cast});
        }
        plan->set_column_bindings(std::move(bindings));
        plan->set_children(create_plan(context,
                                       function_registry,
                                       node->children().front(),
                                       components::logical_plan::limit_t::unlimit(),
                                       params));

        return plan;
    }

} // namespace services::planner::impl
