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
        // The columns the statement omitted, with the value each must be filled with —
        // resolved once by enrich from pg_attribute. The operator materialises them into
        // the chunk before the append (see operator_insert::push).
        components::logical_plan::insert_fill_list_t fill(context.resource);
        fill.reserve(node_insert->fill_list().size());
        for (const auto& column : node_insert->fill_list()) {
            fill.push_back(components::logical_plan::insert_fill_column_t{
                std::pmr::string{column.name.c_str(), context.resource},
                column.type,
                column.value});
        }
        plan->set_fill_list(std::move(fill));
        plan->set_children(create_plan(context,
                                       function_registry,
                                       node->children().front(),
                                       components::logical_plan::limit_t::unlimit(),
                                       params));

        return plan;
    }

} // namespace services::planner::impl
