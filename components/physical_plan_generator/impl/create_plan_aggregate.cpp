#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"

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

        // Collect the max column index referenced in a compare expression tree (WHERE clause).
        // Returns SIZE_MAX if any wildcard is found or expression is not a simple column reference.
        size_t collect_max_from_compare(const components::expressions::compare_expression_ptr& expr) {
            using namespace components::expressions;
            if (!expr) return SIZE_MAX;

            if (is_union_compare_condition(expr->type())) {
                size_t max_idx = 0;
                bool found = false;
                for (const auto& child : expr->children()) {
                    const auto& ce = reinterpret_cast<const compare_expression_ptr&>(child);
                    size_t m = collect_max_from_compare(ce);
                    if (m == SIZE_MAX) return SIZE_MAX;
                    max_idx = std::max(max_idx, m);
                    found = true;
                }
                return found ? max_idx : SIZE_MAX;
            }

            // Leaf: extract column index from whichever side holds key_t
            using components::expressions::key_t;
            using components::expressions::param_storage;
            auto extract = [](const param_storage& side) -> size_t {
                if (!std::holds_alternative<key_t>(side)) return SIZE_MAX;
                const auto& path = std::get<key_t>(side).path();
                if (path.empty()) return SIZE_MAX;
                size_t idx = path[0];
                return (idx == SIZE_MAX) ? SIZE_MAX : idx;
            };

            size_t left_idx  = extract(expr->left());
            size_t right_idx = extract(expr->right());

            // Return the max of whichever sides are valid column refs
            if (left_idx != SIZE_MAX && right_idx != SIZE_MAX) return std::max(left_idx, right_idx);
            if (left_idx  != SIZE_MAX) return left_idx;
            if (right_idx != SIZE_MAX) return right_idx;
            return SIZE_MAX;
        }

        // Collect the max column index (0-based) referenced by all expressions in a node.
        // Returns SIZE_MAX if no column references or any wildcard reference found.
        size_t collect_max_column_index(const components::logical_plan::node_ptr& node) {
            using components::expressions::expression_group;
            size_t max_idx = 0;
            bool found_any = false;

            auto visit_path = [&](const KeyT& key) -> bool {
                for (size_t idx : key.path()) {
                    if (idx == SIZE_MAX) return false; // wildcard → can't project
                    max_idx = std::max(max_idx, idx);
                    found_any = true;
                }
                return true;
            };

            for (const auto& expr : node->expressions()) {
                if (expr->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(expr.get());
                    if (!visit_path(se->key())) return SIZE_MAX;
                    for (const auto& p : se->params()) {
                        if (std::holds_alternative<KeyT>(p)) {
                            if (!visit_path(std::get<KeyT>(p))) return SIZE_MAX;
                        }
                    }
                } else if (expr->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(expr.get());
                    for (const auto& p : ae->params()) {
                        if (std::holds_alternative<KeyT>(p)) {
                            if (!visit_path(std::get<KeyT>(p))) return SIZE_MAX;
                        }
                    }
                }
            }
            return found_any ? max_idx : SIZE_MAX;
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

        // Compute column projection limit: max column index referenced by GROUP BY + SELECT + WHERE.
        // column_limit = 0 means no projection (read all columns).
        // We use a contiguous prefix [0..column_limit-1] to avoid remapping in downstream operators.
        size_t column_limit = 0;
        {
            size_t max_idx = 0;
            bool can_project = true;

            // GROUP BY expressions
            for (const auto& child : node->children()) {
                if (child->type() == node_type::group_t) {
                    size_t m = collect_max_column_index(child);
                    if (m == SIZE_MAX) { can_project = false; break; }
                    max_idx = std::max(max_idx, m);
                    break;
                }
            }

            // WHERE (match) expressions
            if (can_project) {
                for (const auto& child : node->children()) {
                    if (child->type() == node_type::match_t) {
                        for (const auto& expr : child->expressions()) {
                            const auto& ce = reinterpret_cast<const components::expressions::compare_expression_ptr&>(expr);
                            size_t m = collect_max_from_compare(ce);
                            if (m == SIZE_MAX) { can_project = false; break; }
                            max_idx = std::max(max_idx, m);
                        }
                        break;
                    }
                }
            }

            if (can_project && max_idx > 0) {
                column_limit = max_idx + 1;
            }
            fprintf(stderr, "[create_plan_aggregate] column_limit=%zu can_project=%d\n",
                    column_limit, static_cast<int>(can_project));
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
                    match_op = create_plan_match(context, child, pre_sort_limit, column_limit);
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
                new components::operators::transfer_scan(plan_resource, coll_name, scan_limit, column_limit)));
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
