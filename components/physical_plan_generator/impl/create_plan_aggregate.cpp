#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include "create_plan_sort.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    components::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::compute::function_registry_t& function_registry,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit,
                          const components::logical_plan::storage_parameters* params) {
        // First pass: extract limit from limit child (if any)
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::limit_t) {
                const auto* limit_node = static_cast<const components::logical_plan::node_limit_t*>(child.get());
                limit = limit_node->limit();
                break;
            }
        }

        auto* plan_resource = context.has_table_oid(node->table_oid()) ? context.resource : node->resource();

        // projected_cols is populated by the column_pruning optimizer rule
        // (components/planner/optimizer/rules/column_pruning.cpp). Empty means
        // "no projection" → read all columns.
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        const auto& projected_cols = agg_node->projected_cols();

        // When ORDER BY is present, scan all rows — limit+offset are applied post-sort.
        bool has_sort = false;
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::sort_t) {
                has_sort = true;
                break;
            }
        }
        auto scan_limit = has_sort ? components::logical_plan::limit_t::unlimit() : limit;

        // Build operator chain: scan/child → match → group → sort → select
        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr select_op;
        components::operators::operator_ptr child_op;

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t:
                    // Call create_plan_match directly so we can pass projected_cols
                    match_op = create_plan_match(context, child, scan_limit, projected_cols);
                    break;
                case node_type::group_t:
                    group_op = create_plan(context, function_registry, child, limit, params);
                    break;
                case node_type::sort_t:
                    sort_op = create_plan_sort(context, child, limit);
                    break;
                case node_type::select_t:
                    select_op = create_plan_select(context, child, params);
                    break;
                default:
                    child_op = create_plan(context, function_registry, child, limit, params);
                    break;
            }
        }

        // Build chain: base → match → group → sort → select
        components::operators::operator_ptr executor;
        if (child_op) {
            executor = std::move(child_op);
            if (match_op) {
                match_op->set_children(std::move(executor));
                executor = std::move(match_op);
            }
        } else {
            // Build projected_cols (storage chunk column indices) for transfer_scan.
            // For relkind='g' we read live columns by their chunk_position (resolved at
            // resolve-table time). For relkind='r' we read column_pruning output from
            // node_aggregate_t::projected_cols(). Empty → pass-through (read all cols).
            std::vector<size_t> projected_cols;
            if (const auto* md = context.table_metadata_for(node->table_oid())) {
                if (md->relkind == components::catalog::relkind::computed) {
                    projected_cols.reserve(md->columns.size());
                    for (const auto& col : md->columns) {
                        if (col.chunk_position >= 0) {
                            projected_cols.push_back(static_cast<size_t>(col.chunk_position));
                        }
                    }
                } else {
                    const auto* agg = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
                    if (!agg->projected_cols().empty()) {
                        projected_cols.assign(agg->projected_cols().begin(), agg->projected_cols().end());
                    }
                }
            }
            executor = match_op ? std::move(match_op)
                                : static_cast<components::operators::operator_ptr>(boost::intrusive_ptr(
                                      new components::operators::transfer_scan(plan_resource,
                                                                               node->table_oid(),
                                                                               scan_limit,
                                                                               std::move(projected_cols))));
        }
        if (group_op) {
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (sort_op) {
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }
        if (select_op) {
            select_op->set_children(std::move(executor));
            executor = std::move(select_op);
        }

        // Check if DISTINCT flag is set on the aggregate node
        if (agg_node->is_distinct()) {
            auto distinct_op =
                context.has_table_oid(node->table_oid())
                    ? boost::intrusive_ptr(
                          new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                    : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }

        return executor;
    }

} // namespace services::planner::impl
