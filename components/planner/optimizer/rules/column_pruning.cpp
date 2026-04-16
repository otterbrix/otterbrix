#include "column_pruning.hpp"

#include <algorithm>
#include <components/catalog/schema.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <functional>

namespace components::planner::optimizer {

    namespace {

        using SExpr = expressions::scalar_expression_t;
        using AExpr = expressions::aggregate_expression_t;
        using FExpr = expressions::function_expression_t;
        using CExpr = expressions::compare_expression_t;
        using KeyT = expressions::key_t;

        // Forward declarations.
        bool collect_cols_from_param(const expressions::param_storage& p, std::vector<size_t>& cols);
        bool collect_cols_from_compare(const expressions::compare_expression_ptr& expr, std::vector<size_t>& cols);

        bool collect_cols_from_param(const expressions::param_storage& p, std::vector<size_t>& cols) {
            using expressions::expression_group;
            if (std::holds_alternative<KeyT>(p)) {
                const auto& key = std::get<KeyT>(p);
                if (key.path().empty()) return true;
                size_t idx = key.path()[0];
                if (idx == SIZE_MAX) return false; // wildcard — disable projection
                cols.push_back(idx);
                return true;
            }
            if (std::holds_alternative<expressions::expression_ptr>(p)) {
                const auto& sub = std::get<expressions::expression_ptr>(p);
                if (!sub) return true;
                if (sub->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(sub.get());
                    // scalar's own key (if any)
                    if (!se->key().path().empty()) {
                        size_t idx = se->key().path()[0];
                        if (idx == SIZE_MAX) return false;
                        cols.push_back(idx);
                    }
                    for (const auto& sp : se->params()) {
                        if (!collect_cols_from_param(sp, cols)) return false;
                    }
                    return true;
                }
                if (sub->group() == expression_group::compare) {
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(sub);
                    return collect_cols_from_compare(ce, cols);
                }
                if (sub->group() == expression_group::function) {
                    const auto* fe = static_cast<const FExpr*>(sub.get());
                    for (const auto& arg : fe->args()) {
                        if (!collect_cols_from_param(arg, cols)) return false;
                    }
                    return true;
                }
                if (sub->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(sub.get());
                    for (const auto& ap : ae->params()) {
                        if (!collect_cols_from_param(ap, cols)) return false;
                    }
                    return true;
                }
            }
            return true; // parameter_id_t or unknown — no column reference
        }

        bool collect_cols_from_compare(const expressions::compare_expression_ptr& expr, std::vector<size_t>& cols) {
            if (!expr) return true;
            if (expressions::is_union_compare_condition(expr->type())) {
                for (const auto& child : expr->children()) {
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(child);
                    if (!collect_cols_from_compare(ce, cols)) return false;
                }
                return true;
            }
            return collect_cols_from_param(expr->left(), cols) && collect_cols_from_param(expr->right(), cols);
        }

        // Collect all column indices referenced by expressions in a node (group_t, aggregate_t, ...).
        bool collect_cols_from_node(const logical_plan::node_ptr& node, std::vector<size_t>& cols) {
            using expressions::expression_group;
            for (const auto& expr : node->expressions()) {
                if (!expr) continue;
                if (expr->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(expr.get());
                    if (!se->key().path().empty()) {
                        size_t idx = se->key().path()[0];
                        if (idx == SIZE_MAX) return false;
                        cols.push_back(idx);
                    }
                    for (const auto& p : se->params()) {
                        if (!collect_cols_from_param(p, cols)) return false;
                    }
                } else if (expr->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(expr.get());
                    for (const auto& p : ae->params()) {
                        if (!collect_cols_from_param(p, cols)) return false;
                    }
                } else if (expr->group() == expression_group::compare) {
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                    if (!collect_cols_from_compare(ce, cols)) return false;
                } else if (expr->group() == expression_group::function) {
                    const auto* fe = static_cast<const FExpr*>(expr.get());
                    for (const auto& arg : fe->args()) {
                        if (!collect_cols_from_param(arg, cols)) return false;
                    }
                }
            }
            return true;
        }

        // Sort and deduplicate column indices.
        void normalize(std::vector<size_t>& cols) {
            std::sort(cols.begin(), cols.end());
            cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
        }

        // Resolve the output column count for a node's source (table or upstream operator).
        // Returns 0 if unknown (in which case JOIN projection pushdown is disabled for that node).
        size_t resolve_column_count(const logical_plan::node_ptr& node, const catalog::catalog* catalog) {
            if (!node) return 0;
            if (node->type() == logical_plan::node_type::aggregate_t) {
                if (!catalog) return 0;
                catalog::table_id id(node->resource(), node->collection_full_name());
                if (catalog->table_exists(id)) {
                    return catalog->get_table_schema(id).columns().size();
                }
                if (catalog->table_computes(id)) {
                    return catalog->get_computing_table_schema(id).latest_types_struct().child_types().size();
                }
            }
            return 0;
        }

        // Walk an aggregate subtree, computing and setting projected_cols on each
        // node_aggregate_t we encounter. Handles JOIN by splitting per side.
        void process_aggregate(const logical_plan::node_ptr& agg_node, const catalog::catalog* catalog);

        void process_join(const logical_plan::node_ptr& join_node,
                          const std::vector<size_t>& parent_projected,
                          const catalog::catalog* catalog) {
            // A join produces [left_columns..., right_columns...]. Resolve left side's
            // column count so we can split parent_projected correctly.
            if (join_node->children().size() != 2) {
                // malformed / unsupported — just recurse with defaults so inner aggregates
                // still compute their own projection from their own SELECT lists.
                for (const auto& child : join_node->children()) {
                    if (child->type() == logical_plan::node_type::aggregate_t) {
                        process_aggregate(child, catalog);
                    }
                }
                return;
            }

            const auto& left = join_node->children()[0];
            const auto& right = join_node->children()[1];
            size_t left_cols = resolve_column_count(left, catalog);

            std::vector<size_t> left_projected;
            std::vector<size_t> right_projected;
            bool can_split = left_cols > 0 && !parent_projected.empty();

            if (can_split) {
                for (size_t idx : parent_projected) {
                    if (idx < left_cols) {
                        left_projected.push_back(idx);
                    } else {
                        right_projected.push_back(idx - left_cols);
                    }
                }
            }

            // Pull in columns referenced by the JOIN ON condition. Each key_t in the
            // condition carries its own side_t, and its path[0] is an index into THAT
            // side's schema (not the joined schema) — so we dispatch by side here.
            std::function<bool(const expressions::expression_ptr&)> walk;
            walk = [&](const expressions::expression_ptr& expr) -> bool {
                if (!expr) return true;
                // Only compare expressions contribute to JOIN ON conditions we can project
                // safely. Anything else (function, scalar arithmetic) references columns
                // transitively — bail out.
                if (expr->group() != expressions::expression_group::compare) {
                    return false;
                }
                const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                if (expressions::is_union_compare_condition(ce->type())) {
                    for (const auto& child : ce->children()) {
                        if (!walk(child)) return false;
                    }
                    return true;
                }
                auto extract_side = [&](const expressions::param_storage& side) -> bool {
                    if (std::holds_alternative<expressions::expression_ptr>(side)) {
                        // Sub-expression in JOIN leaf — bail out.
                        return false;
                    }
                    if (!std::holds_alternative<KeyT>(side)) return true;
                    const auto& key = std::get<KeyT>(side);
                    if (key.path().empty()) return true;
                    size_t idx = key.path()[0];
                    if (idx == SIZE_MAX) return false;
                    switch (key.side()) {
                        case expressions::side_t::left:  left_projected.push_back(idx);  break;
                        case expressions::side_t::right: right_projected.push_back(idx); break;
                        default: return false;
                    }
                    return true;
                };
                return extract_side(ce->left()) && extract_side(ce->right());
            };

            for (const auto& expr : join_node->expressions()) {
                if (!walk(expr)) { can_split = false; break; }
            }
            if (can_split) {
                normalize(left_projected);
                normalize(right_projected);
            }

            // Descend into each side. Even if we couldn't split, each inner aggregate
            // still computes its OWN projection from its SELECT list — that gives
            // table-level reads of only the columns each side's subquery references.
            if (left->type() == logical_plan::node_type::aggregate_t) {
                process_aggregate(left, catalog);
                if (can_split) {
                    // If the inner aggregate didn't produce its own projection (empty),
                    // or produced one but we want to intersect with what the parent needs,
                    // overwrite only if we have a non-empty split. We DO NOT intersect here;
                    // we only propagate the parent's needs when the inner aggregate itself
                    // didn't constrain anything further. This keeps inner subqueries' own
                    // SELECT list authoritative.
                    auto* agg = static_cast<logical_plan::node_aggregate_t*>(left.get());
                    if (agg->projected_cols().empty() && !left_projected.empty()) {
                        agg->set_projected_cols(std::move(left_projected));
                    }
                }
            }
            if (right->type() == logical_plan::node_type::aggregate_t) {
                process_aggregate(right, catalog);
                if (can_split) {
                    auto* agg = static_cast<logical_plan::node_aggregate_t*>(right.get());
                    if (agg->projected_cols().empty() && !right_projected.empty()) {
                        agg->set_projected_cols(std::move(right_projected));
                    }
                }
            }
        }

        void process_aggregate(const logical_plan::node_ptr& agg_node, const catalog::catalog* catalog) {
            if (!agg_node || agg_node->type() != logical_plan::node_type::aggregate_t) {
                return;
            }

            // Collect columns referenced by this aggregate's own group (SELECT list +
            // aggregate params) and match (WHERE).
            std::vector<size_t> raw_cols;
            bool can_project = true;
            logical_plan::node_ptr group_child, match_child, data_child;

            for (const auto& child : agg_node->children()) {
                switch (child->type()) {
                    case logical_plan::node_type::group_t:
                        group_child = child;
                        break;
                    case logical_plan::node_type::match_t:
                        match_child = child;
                        break;
                    case logical_plan::node_type::limit_t:
                    case logical_plan::node_type::sort_t:
                    case logical_plan::node_type::having_t:
                        break;
                    default:
                        // join_t, aggregate_t (subquery), data_t, etc.
                        data_child = child;
                        break;
                }
            }

            // Projection is safe only when the aggregate has a group_t child that enumerates
            // the output columns (SELECT list + aggregate params). Otherwise the aggregate is
            // a bare raw scan that must return all columns to the caller.
            if (!group_child) {
                can_project = false;
            }

            if (can_project && group_child) {
                if (!collect_cols_from_node(group_child, raw_cols)) can_project = false;
            }
            if (can_project && match_child) {
                for (const auto& expr : match_child->expressions()) {
                    if (!expr) continue;
                    if (expr->group() != expressions::expression_group::compare) {
                        // Function / scalar expressions at the top level of WHERE — we don't
                        // yet enumerate all columns referenced transitively by arbitrary
                        // function bodies (UDFs etc.). Disable projection to be safe.
                        can_project = false;
                        break;
                    }
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                    if (!collect_cols_from_compare(ce, raw_cols)) { can_project = false; break; }
                }
            }

            if (can_project && !raw_cols.empty()) {
                normalize(raw_cols);
                static_cast<logical_plan::node_aggregate_t*>(agg_node.get())->set_projected_cols(std::move(raw_cols));
            }

            // Recurse into non-trivial children (join or nested aggregate).
            if (data_child) {
                if (data_child->type() == logical_plan::node_type::join_t) {
                    const auto& projected = static_cast<const logical_plan::node_aggregate_t*>(agg_node.get())->projected_cols();
                    process_join(data_child, projected, catalog);
                } else if (data_child->type() == logical_plan::node_type::aggregate_t) {
                    // Nested subquery — it independently computes its own projection
                    // from its own SELECT list.
                    process_aggregate(data_child, catalog);
                }
            }
        }

    } // namespace

    void prune_columns(const logical_plan::node_ptr& root, const catalog::catalog* catalog) {
        if (!root) return;
        // BFS over the whole plan, processing every aggregate_t we encounter.
        std::vector<logical_plan::node_ptr> stack{root};
        while (!stack.empty()) {
            auto current = std::move(stack.back());
            stack.pop_back();
            if (current->type() == logical_plan::node_type::aggregate_t) {
                process_aggregate(current, catalog);
                // process_aggregate already recurses into join_t / nested aggregate_t;
                // no need to push those children again.
                continue;
            }
            for (const auto& child : current->children()) {
                stack.push_back(child);
            }
        }
    }

} // namespace components::planner::optimizer
