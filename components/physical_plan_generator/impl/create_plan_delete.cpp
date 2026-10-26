#include "create_plan_delete.hpp"

#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    namespace {
        // The rows come from the storage's scan, numbered in row_ids; the matched ones go to its delete sink. A
        // WHERE without USING filters above the scan; with USING the semi-join applies it, as for a local table.
        plan_result_t create_plan_storage_delete(const context_storage_t& context,
                                                 const components::compute::function_registry_t& function_registry,
                                                 const components::logical_plan::node_delete_t& node_delete,
                                                 const components::logical_plan::resolved_table_metadata_t& table,
                                                 const components::logical_plan::node_ptr& node_match,
                                                 const components::logical_plan::node_ptr& node_source,
                                                 components::logical_plan::row_cap_t cap,
                                                 std::pmr::vector<components::operators::projected_column_t> returning,
                                                 const components::logical_plan::storage_parameters* params) {
            VALUE_OR_RETURN(auto sink,
                            storage_operator(context.resource, table.name, table.storage->make_delete(context)));
            VALUE_OR_RETURN(auto scan,
                            storage_operator(context.resource, table.name, table.storage->make_scan(context)));
            std::pmr::vector<components::types::complex_logical_type> columns(context.resource);
            columns.reserve(table.columns.size());
            for (const auto& column : table.columns) {
                columns.push_back(column.type);
            }
            const auto& where = node_match->expressions()[0];
            if (!node_source) {
                auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                            context.log.clone(),
                                                                                            node_delete.table_oid(),
                                                                                            std::move(returning)));
                plan->set_storage_sink(std::move(sink), std::move(columns), table.storage->flush_row_threshold());
                auto filter = boost::intrusive_ptr(
                    new components::operators::operator_match_t(context.resource, context.log.clone(), where, cap));
                filter->set_children(std::move(scan));
                plan->set_children(std::move(filter));
                return plan;
            }
            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        node_delete.table_oid(),
                                                                                        std::move(returning),
                                                                                        where,
                                                                                        cap));
            plan->set_storage_sink(std::move(sink), std::move(columns), table.storage->flush_row_threshold());
            VALUE_OR_RETURN(
                auto source_op,
                create_plan(context, function_registry, node_source, components::logical_plan::limit_t::all(), params));
            plan->set_children(std::move(scan), std::move(source_op));
            return plan;
        }
    } // namespace

    plan_result_t create_plan_delete(const context_storage_t& context,
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

        auto returning = build_returning_columns(context.resource, node_delete->returning());

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
        const auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        // No Limit operator runs over a DML root, so the count is refused here, before any row is touched.
        if (auto invalid = limit.validate(context.resource); invalid.contains_error()) {
            return invalid;
        }
        const auto cap = limit.read_cap();
        if (const auto* table = node->table_metadata(); table != nullptr && table->storage != nullptr) {
            return create_plan_storage_delete(context,
                                              function_registry,
                                              *node_delete,
                                              *table,
                                              node_match,
                                              node_source,
                                              cap,
                                              std::move(returning),
                                              params);
        }
        auto table_oid = node->table_oid();
        // Past the catalog arm the target is always a NAMED table; a target the
        // context cannot vouch for is a table that never resolved. Validation refuses
        // this before plan generation; if that refusal is ever lost again, lowering
        // anyway builds a sink with no table behind it, which the streaming executor
        // admits as a sourceless sink — a DELETE that removes nothing and reports SUCCESS.
        if (!context.has_table_oid(table_oid)) {
            return unresolved_table_refusal(context.resource, node_delete->target());
        }
        if (!node_source) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning)));
            plan->set_table_has_indexes(node->table_has_indexes());
            VALUE_OR_RETURN(auto scan, create_plan_match(context, node_match, cap, node_delete->projected_cols()));
            plan->set_children(std::move(scan));

            return plan;
        }
        auto expr =
            reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node_match->expressions()[0]);

        // Source (DELETE ... USING) path: the semi-join reads ALL left rows (uncapped) and
        // operator_delete stops after exactly the capped count of MATCHED rows — capping the left
        // scan would under-delete (fewer than n of the first n left rows may join a source row).
        auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                    context.log.clone(),
                                                                                    table_oid,
                                                                                    std::move(returning),
                                                                                    *expr,
                                                                                    cap));
        plan->set_table_has_indexes(node->table_has_indexes());
        VALUE_OR_RETURN(
            auto source_op,
            create_plan(context, function_registry, node_source, components::logical_plan::limit_t::all(), params));
        plan->set_children(
            boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                      context.log.clone(),
                                                                      table_oid,
                                                                      nullptr,
                                                                      components::logical_plan::row_cap_t::uncapped(),
                                                                      node_delete->projected_cols())),
            std::move(source_op));
        return plan;
    }

} // namespace services::planner::impl
