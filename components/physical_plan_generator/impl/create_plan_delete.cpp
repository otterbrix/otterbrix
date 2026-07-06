#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_delete(const context_storage_t& context,
                       const components::compute::function_registry_t& function_registry,
                       const components::logical_plan::node_ptr& node,
                       const components::logical_plan::storage_parameters* params) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());

        // Catalog-delete leaf (DDL pg_catalog row scrub): lower straight to
        // operator_delete's catalog branch — delete by (oid_col_idx, target_oid)
        // via delete_pg_catalog_rows, no predicate scan, no children.
        if (node_delete->oid_col_idx() >= 0 && components::catalog::is_catalog_table(node->table_oid())) {
            return boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                   context.log.clone(),
                                                                                   node->table_oid(),
                                                                                   node_delete->oid_col_idx(),
                                                                                   node_delete->target_oid()));
        }

        auto returning = build_returning_columns(context.resource, node_delete->returning(), params);

        // Forward the plan-resolved RETURNING output types (stamped on the delete node by
        // validate_schema, in RETURNING projection order) onto the projection columns, so a
        // CASE/COALESCE/deep-field RETURNING column over zero affected rows stays correctly
        // typed instead of being dropped as an untyped (NA) placeholder. Mirrors the SELECT
        // path (create_plan_aggregate -> operator_select_t::set_output_types). build_returning_columns
        // emits one select_column_t per scalar RETURNING expression in returning() order, and
        // output_types() is stamped in that same order, so column i maps to output_types()[i].
        // No RETURNING -> output_types() empty -> has_output_types() false -> guard skips (no-op).
        if (node->has_output_types()) {
            const auto& output_types = node->output_types();
            for (size_t i = 0; i < returning.size() && i < output_types.size(); ++i) {
                returning[i].result_type = output_types[i];
            }
        }

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_source = nullptr;
        for (auto child : node_delete->children()) {
            switch (child->type()) {
                case components::logical_plan::node_type::match_t:
                    node_match = child;
                    break;
                case components::logical_plan::node_type::limit_t:
                    node_limit = child;
                    break;
                default:
                    node_source = child;
                    break;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        auto table_oid = node->table_oid();
        if (!node_source) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning)));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        }
        auto expr =
            reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node_match->expressions()[0]);

        auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                    context.log.clone(),
                                                                                    table_oid,
                                                                                    std::move(returning),
                                                                                    *expr));
        plan->set_children(
            boost::intrusive_ptr(
                new components::operators::full_scan(context.resource, context.log.clone(), table_oid, nullptr, limit)),
            create_plan(context, function_registry, node_source, components::logical_plan::limit_t::unlimit(), params));
        return plan;
    }

} // namespace services::planner::impl
