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
        auto returning = build_returning_columns(context.resource, node_insert->returning());
        auto plan = boost::intrusive_ptr(new components::operators::operator_insert(context.resource,
                                                                                    context.log.clone(),
                                                                                    node->table_oid(),
                                                                                    std::move(returning)));
        plan->set_table_has_indexes(node->table_has_indexes());
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
