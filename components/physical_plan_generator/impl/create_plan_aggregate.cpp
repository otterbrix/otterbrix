#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"

#include <algorithm>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    namespace {
        using SExpr = components::expressions::scalar_expression_t;
        using AExpr = components::expressions::aggregate_expression_t;
        using KeyT  = components::expressions::key_t;

        // Collect column indices from a compare expression tree (WHERE clause) into a set.
        // Returns false if any wildcard/unresolved reference is found (projection not safe).
        bool collect_cols_from_compare(const components::expressions::compare_expression_ptr& expr,
                                       std::vector<size_t>& cols) {
            using namespace components::expressions;
            if (!expr) return true;

            if (is_union_compare_condition(expr->type())) {
                for (const auto& child : expr->children()) {
                    const auto& ce = reinterpret_cast<const compare_expression_ptr&>(child);
                    if (!collect_cols_from_compare(ce, cols)) return false;
                }
                return true;
            }

            using components::expressions::key_t;
            using components::expressions::param_storage;
            auto extract = [&](const param_storage& side) -> bool {
                if (!std::holds_alternative<key_t>(side)) return true;
                const auto& path = std::get<key_t>(side).path();
                if (path.empty()) return true;
                size_t idx = path[0];
                if (idx == SIZE_MAX) return false; // wildcard
                cols.push_back(idx);
                return true;
            };

            return extract(expr->left()) && extract(expr->right());
        }

        // Collect all column indices referenced by expressions in a node into a set.
        // Returns false if any wildcard/unresolved reference is found.
        bool collect_cols_from_node(const components::logical_plan::node_ptr& node,
                                    std::vector<size_t>& cols) {
            using components::expressions::expression_group;

            auto visit_path = [&](const KeyT& key) -> bool {
                if (key.path().empty()) return true;
                size_t idx = key.path()[0];
                if (idx == SIZE_MAX) return false;
                cols.push_back(idx);
                return true;
            };

            for (const auto& expr : node->expressions()) {
                if (expr->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(expr.get());
                    if (!visit_path(se->key())) return false;
                    for (const auto& p : se->params()) {
                        if (std::holds_alternative<KeyT>(p)) {
                            if (!visit_path(std::get<KeyT>(p))) return false;
                        }
                    }
                } else if (expr->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(expr.get());
                    for (const auto& p : ae->params()) {
                        if (std::holds_alternative<KeyT>(p)) {
                            if (!visit_path(std::get<KeyT>(p))) return false;
                        }
                    }
                }
            }
            return true;
        }
    } // namespace

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

        auto coll_name = node->collection_full_name();
        auto* plan_resource = context.has_collection(coll_name) ? context.resource : node->resource();

        // First pass: detect whether a sort child is present so we can set limits correctly.
        // When ORDER BY is present, LIMIT must only be applied after sorting —
        // match, group, and scan must all use unlimit().
        bool has_sort = false;
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::sort_t) { has_sort = true; break; }
        }
        auto pre_sort_limit = has_sort ? components::logical_plan::limit_t::unlimit() : limit;

        // Compute exact set of column indices needed by GROUP BY + WHERE.
        // Column projection is only safe when a GROUP BY is present — in that case we know
        // exactly which columns are needed in the output (group keys + aggregated fields).
        // For plain SELECT * / find queries (no GROUP BY), we must read all columns so that
        // every field is available to the caller.  Empty projected_cols → no projection.
        std::vector<size_t> projected_cols;
        {
            bool has_group = false;
            for (const auto& child : node->children()) {
                if (child->type() == node_type::group_t) { has_group = true; break; }
            }

            if (has_group) {
                std::vector<size_t> raw_cols;
                bool can_project = true;

                // GROUP BY expressions
                for (const auto& child : node->children()) {
                    if (child->type() == node_type::group_t) {
                        if (!collect_cols_from_node(child, raw_cols)) { can_project = false; break; }
                        break;
                    }
                }

                // WHERE (match) expressions
                if (can_project) {
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::match_t) {
                            for (const auto& expr : child->expressions()) {
                                const auto& ce =
                                    reinterpret_cast<const components::expressions::compare_expression_ptr&>(expr);
                                if (!collect_cols_from_compare(ce, raw_cols)) { can_project = false; break; }
                            }
                            break;
                        }
                    }
                }

                if (can_project && !raw_cols.empty()) {
                    // Deduplicate and sort
                    std::sort(raw_cols.begin(), raw_cols.end());
                    raw_cols.erase(std::unique(raw_cols.begin(), raw_cols.end()), raw_cols.end());
                    projected_cols = std::move(raw_cols);
                }
            }
        }

        // Build operator chain directly: scan/child → match → group → sort
        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr child_op;

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t:
                    match_op = create_plan_match(context, child, pre_sort_limit, projected_cols);
                    break;
                case node_type::group_t:
                    group_op = create_plan(context, function_registry, child, pre_sort_limit, params);
                    break;
                case node_type::sort_t:
                    sort_op = create_plan(context, function_registry, child, limit, params);
                    break;
                default:
                    child_op = create_plan(context, function_registry, child, pre_sort_limit, params);
                    break;
            }
        }

        // Build chain: base → match → group → sort
        // When sort is present, scan all rows — limit is applied by sort
        auto scan_limit = pre_sort_limit;

        components::operators::operator_ptr executor;
        if (child_op) {
            executor = std::move(child_op);
            if (match_op) {
                match_op->set_children(std::move(executor));
                executor = std::move(match_op);
            }
        } else if (match_op) {
            executor = std::move(match_op);
        } else {
            // Pure GROUP BY with no WHERE: use transfer_scan with column projection.
            executor = static_cast<components::operators::operator_ptr>(boost::intrusive_ptr(
                new components::operators::transfer_scan(plan_resource, coll_name, scan_limit, projected_cols)));
        }
        if (group_op) {
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (sort_op) {
            // Pass visible_select_count to sort operator for post-sort column truncation
            for (const auto& child : node->children()) {
                if (child->type() == node_type::group_t) {
                    if (auto* gn = dynamic_cast<const components::logical_plan::node_group_t*>(child.get())) {
                        if (gn->visible_select_count > 0) {
                            static_cast<components::operators::operator_sort_t*>(sort_op.get())
                                ->set_expected_output_count(gn->visible_select_count);
                        }
                    }
                    break;
                }
            }
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }

        // Check if DISTINCT flag is set on the aggregate node
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        if (agg_node->is_distinct()) {
            auto distinct_op =
                context.has_collection(coll_name)
                    ? boost::intrusive_ptr(
                          new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                    : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }

        return executor;
    }

} // namespace services::planner::impl
