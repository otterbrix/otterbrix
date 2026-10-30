#include "create_plan_insert.hpp"

#include "create_plan_select.hpp"
#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/vector/vector_builders.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_insert(const context_storage_t& context,
                                     const components::compute::function_registry_t& function_registry,
                                     const components::logical_plan::node_ptr& node,
                                     const components::logical_plan::storage_parameters* params) {
        const auto* node_insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_insert->returning());
        std::pmr::vector<components::catalog::oid_t> attoids(context.resource);
        if (const auto* table = context.table_metadata_for(node->table_oid());
            table != nullptr && table->relkind != components::catalog::relkind::computed) {
            attoids.reserve(table->columns.size());
            for (const auto& column : table->columns) {
                attoids.push_back(column.attoid);
            }
        }
        VALUE_OR_RETURN(auto address, storage_address_of(context, node->table_oid()));
        auto plan =
            boost::intrusive_ptr(new components::operators::operator_insert(context.resource,
                                                                            context.log.clone(),
                                                                            node->table_oid(),
                                                                            std::move(address),
                                                                            full_name_of(context, node->table_oid()),
                                                                            std::move(attoids),
                                                                            std::move(returning)));
        plan->set_table_has_indexes(node->table_has_indexes());
        if (!node_insert->returning().empty()) {
            VALUE_OR_RETURN(auto table_types, page_types_of(context, node->table_oid(), {}));
            plan->set_table_types(std::move(table_types));
        }
        // The validator resolved, per incoming column, the target it lands in and the cast
        // that stores it there. The operator puts every column at its target position, so the
        // payload arrives in the order the writer sees the table's columns.
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
        // Columns the statement omitted, with their fill values (resolved once by enrich from
        // pg_attribute); materialised into the chunk before the append (operator_insert::push).
        components::logical_plan::insert_fill_list_t fill(context.resource);
        fill.reserve(node_insert->fill_list().size());
        for (const auto& column : node_insert->fill_list()) {
            fill.push_back(components::logical_plan::insert_fill_column_t{
                column.target_index,
                std::pmr::string{column.name.c_str(), context.resource},
                column.type,
                components::vector::make_constant_copy(context.resource, column.value)});
        }
        plan->set_fill_list(std::move(fill));
        VALUE_OR_RETURN(auto child,
                        create_plan(context,
                                    function_registry,
                                    node->children().front(),
                                    components::logical_plan::limit_t::all(),
                                    params));
        plan->set_children(std::move(child));

        return plan;
    }

} // namespace services::planner::impl
