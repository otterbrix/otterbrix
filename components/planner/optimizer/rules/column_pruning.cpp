#include "column_pruning.hpp"

#include <algorithm>
#include <functional>
#include <unordered_map>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

namespace components::planner::optimizer {

    namespace {

        using SExpr = expressions::scalar_expression_t;
        using AExpr = expressions::aggregate_expression_t;
        using FExpr = expressions::function_expression_t;
        using CExpr = expressions::compare_expression_t;
        using KeyT = expressions::key_t;

        // oid → column_count map built once from the plan tree's
        // catalog_resolve_table_t siblings. Pure index-based projection
        // works because all chunks share the table's canonical schema
        // (verified for both relkind='r' and relkind='g').
        using table_cols_map = std::unordered_map<components::catalog::oid_t, size_t>;

        // Walks the plan once, collecting column counts from every
        // catalog_resolve_table_t::resolved_metadata().
        void collect_table_md(const logical_plan::node_ptr& root, table_cols_map& out) {
            if (!root)
                return;
            std::vector<const logical_plan::node_t*> stack;
            stack.push_back(root.get());
            while (!stack.empty()) {
                const auto* n = stack.back();
                stack.pop_back();
                if (!n)
                    continue;
                if (n->type() == logical_plan::node_type::catalog_resolve_table_t) {
                    const auto* rt = static_cast<const logical_plan::node_catalog_resolve_table_t*>(n);
                    const auto& md_opt = rt->resolved_metadata();
                    if (md_opt && md_opt->table_oid != components::catalog::INVALID_OID) {
                        out[md_opt->table_oid] = md_opt->columns.size();
                    }
                }
                for (const auto& c : n->children()) {
                    stack.push_back(c.get());
                }
            }
        }

        // Forward declarations.
        bool collect_cols_from_param(const expressions::param_storage& p, std::vector<size_t>& cols);
        bool collect_cols_from_compare(const expressions::compare_expression_ptr& expr, std::vector<size_t>& cols);

        bool collect_cols_from_param(const expressions::param_storage& p, std::vector<size_t>& cols) {
            using expressions::expression_group;
            if (std::holds_alternative<KeyT>(p)) {
                const auto& key = std::get<KeyT>(p);
                if (key.path().empty())
                    return true;
                size_t idx = key.path()[0];
                if (idx == SIZE_MAX)
                    return false; // wildcard — disable projection
                cols.push_back(idx);
                return true;
            }
            if (std::holds_alternative<expressions::expression_ptr>(p)) {
                const auto& sub = std::get<expressions::expression_ptr>(p);
                if (!sub)
                    return true;
                if (sub->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(sub.get());
                    // scalar's own key (if any)
                    if (!se->key().path().empty()) {
                        size_t idx = se->key().path()[0];
                        if (idx == SIZE_MAX)
                            return false;
                        cols.push_back(idx);
                    }
                    for (const auto& sp : se->params()) {
                        if (!collect_cols_from_param(sp, cols))
                            return false;
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
                        if (!collect_cols_from_param(arg, cols))
                            return false;
                    }
                    return true;
                }
                if (sub->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(sub.get());
                    for (const auto& ap : ae->params()) {
                        if (!collect_cols_from_param(ap, cols))
                            return false;
                    }
                    return true;
                }
            }
            return true; // parameter_id_t or unknown — no column reference
        }

        bool collect_cols_from_compare(const expressions::compare_expression_ptr& expr, std::vector<size_t>& cols) {
            if (!expr)
                return true;
            if (expressions::is_union_compare_condition(expr->type())) {
                for (const auto& child : expr->children()) {
                    expressions::param_storage p{child};
                    if (!collect_cols_from_param(p, cols))
                        return false;
                }
                return true;
            }
            return collect_cols_from_param(expr->left(), cols) && collect_cols_from_param(expr->right(), cols);
        }

        // Collect all column indices referenced by expressions in a node (group_t, aggregate_t, ...).
        bool collect_cols_from_node(const logical_plan::node_ptr& node, std::vector<size_t>& cols) {
            using expressions::expression_group;
            for (const auto& expr : node->expressions()) {
                if (!expr)
                    continue;
                if (expr->group() == expression_group::scalar) {
                    const auto* se = static_cast<const SExpr*>(expr.get());
                    if (!se->key().path().empty()) {
                        size_t idx = se->key().path()[0];
                        if (idx == SIZE_MAX)
                            return false;
                        cols.push_back(idx);
                    }
                    for (const auto& p : se->params()) {
                        if (!collect_cols_from_param(p, cols))
                            return false;
                    }
                } else if (expr->group() == expression_group::aggregate) {
                    const auto* ae = static_cast<const AExpr*>(expr.get());
                    for (const auto& p : ae->params()) {
                        if (!collect_cols_from_param(p, cols))
                            return false;
                    }
                } else if (expr->group() == expression_group::compare) {
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                    if (!collect_cols_from_compare(ce, cols))
                        return false;
                } else if (expr->group() == expression_group::function) {
                    const auto* fe = static_cast<const FExpr*>(expr.get());
                    for (const auto& arg : fe->args()) {
                        if (!collect_cols_from_param(arg, cols))
                            return false;
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
        size_t resolve_column_count(const logical_plan::node_ptr& node, const table_cols_map& md) {
            if (!node)
                return 0;
            if (node->type() == logical_plan::node_type::aggregate_t) {
                const auto oid = node->table_oid();
                if (oid == components::catalog::INVALID_OID)
                    return 0;
                auto it = md.find(oid);
                return it != md.end() ? it->second : 0;
            }
            return 0;
        }

        // Walk an aggregate subtree, computing and setting projected_cols on each
        // node_aggregate_t we encounter. Handles JOIN by splitting per side.
        void process_aggregate(const logical_plan::node_ptr& agg_node, const table_cols_map& md);

        void process_join(const logical_plan::node_ptr& join_node,
                          const std::vector<size_t>& parent_projected,
                          const table_cols_map& md) {
            // An N-ary join produces [child0_columns..., child1_columns..., ..., child{N-1}_columns...]
            // in left-to-right order. Today the comma-join transformer
            // (transform_select.cpp T_FromExpr) synthesizes only binary JoinExprs, so
            // n == 2 is the steady state. This implementation is forward-compatible
            // with n >= 2 (e.g., a future star-flattening pass that produces N-ary
            // logical joins) and degrades cleanly for n == 0 / n == 1.
            const auto& children = join_node->children();
            const size_t n = children.size();

            // n == 0: defensive — nothing to descend into.
            if (n == 0) {
                return;
            }

            // n == 1: no join split; the single child sees parent_projected directly
            // as if it were the join's output. Treat it the same as the binary path's
            // descent step but without per-side index remapping.
            if (n == 1) {
                const auto& only = children[0];
                if (only && only->type() == logical_plan::node_type::aggregate_t) {
                    process_aggregate(only, md);
                    auto* agg = static_cast<logical_plan::node_aggregate_t*>(only.get());
                    if (agg->projected_cols().empty() && !parent_projected.empty()) {
                        std::vector<size_t> projected = parent_projected;
                        normalize(projected);
                        agg->set_projected_cols(std::move(projected));
                    }
                }
                return;
            }

            // n >= 2: split parent_projected by per-child column counts.
            std::vector<size_t> child_cols(n, 0);
            std::vector<size_t> offsets(n, 0); // cumulative column offset of child[i] in joined schema
            bool all_known = true;
            size_t running = 0;
            for (size_t i = 0; i < n; ++i) {
                child_cols[i] = resolve_column_count(children[i], md);
                offsets[i] = running;
                if (child_cols[i] == 0) {
                    all_known = false;
                }
                running += child_cols[i];
            }

            std::vector<std::vector<size_t>> per_child_projected(n);
            bool can_split = all_known && !parent_projected.empty();

            if (can_split) {
                for (size_t idx : parent_projected) {
                    // Locate which child this joined-schema index falls into.
                    bool placed = false;
                    for (size_t i = 0; i < n; ++i) {
                        const size_t hi = offsets[i] + child_cols[i];
                        if (idx < hi) {
                            per_child_projected[i].push_back(idx - offsets[i]);
                            placed = true;
                            break;
                        }
                    }
                    if (!placed) {
                        // idx out of range for the joined schema — invariant violation;
                        // disable split rather than corrupt projection.
                        can_split = false;
                        for (auto& pc : per_child_projected) pc.clear();
                        break;
                    }
                }
            }

            // Pull in columns referenced by the JOIN ON condition. Each key_t in the
            // condition carries its own side_t (left/right/undefined), and its path[0]
            // is an index into THAT side's schema (not the joined schema).
            //
            // DEGRADATION for n > 2: side_t has only {left, right, undefined}, which
            // cannot distinguish the N-1 non-first children. We attribute side_t::left
            // to child[0] and side_t::right to child[n-1]. Middle children (index in
            // [1, n-2]) are NOT augmented from the ON walker here; they instead rely
            // on their own SELECT-list-driven projection in process_aggregate (which
            // already collects from group_t + match_t children regardless of join
            // context). This is NOT a silent fallback — middle-child projection from
            // SELECT-list is the documented contract of process_aggregate.
            std::function<bool(const expressions::expression_ptr&)> walk;
            walk = [&](const expressions::expression_ptr& expr) -> bool {
                if (!expr)
                    return true;
                // Only compare expressions contribute to JOIN ON conditions we can project
                // safely. Anything else (function, scalar arithmetic) references columns
                // transitively — bail out.
                if (expr->group() != expressions::expression_group::compare) {
                    return false;
                }
                const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                if (expressions::is_union_compare_condition(ce->type())) {
                    for (const auto& child : ce->children()) {
                        if (!walk(child))
                            return false;
                    }
                    return true;
                }
                auto extract_side = [&](const expressions::param_storage& side) -> bool {
                    if (std::holds_alternative<expressions::expression_ptr>(side)) {
                        // Sub-expression in JOIN leaf — bail out.
                        return false;
                    }
                    if (!std::holds_alternative<KeyT>(side))
                        return true;
                    const auto& key = std::get<KeyT>(side);
                    if (key.path().empty())
                        return true;
                    size_t idx = key.path()[0];
                    if (idx == SIZE_MAX)
                        return false;
                    switch (key.side()) {
                        case expressions::side_t::left:
                            per_child_projected[0].push_back(idx);
                            break;
                        case expressions::side_t::right:
                            per_child_projected[n - 1].push_back(idx);
                            break;
                        default:
                            return false;
                    }
                    return true;
                };
                return extract_side(ce->left()) && extract_side(ce->right());
            };

            for (const auto& expr : join_node->expressions()) {
                if (!walk(expr)) {
                    can_split = false;
                    break;
                }
            }
            if (can_split) {
                for (auto& pc : per_child_projected) {
                    normalize(pc);
                }
            }

            // Descend into each child. Even if we couldn't split, each inner aggregate
            // still computes its OWN projection from its SELECT list — that gives
            // table-level reads of only the columns each side's subquery references.
            for (size_t i = 0; i < n; ++i) {
                const auto& child = children[i];
                if (!child || child->type() != logical_plan::node_type::aggregate_t) {
                    continue;
                }
                process_aggregate(child, md);
                if (can_split) {
                    auto* agg = static_cast<logical_plan::node_aggregate_t*>(child.get());
                    if (agg->projected_cols().empty() && !per_child_projected[i].empty()) {
                        agg->set_projected_cols(std::move(per_child_projected[i]));
                    }
                }
            }
        }

        void process_aggregate(const logical_plan::node_ptr& agg_node, const table_cols_map& md) {
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
                if (!collect_cols_from_node(group_child, raw_cols))
                    can_project = false;
            }
            if (can_project && match_child) {
                for (const auto& expr : match_child->expressions()) {
                    if (!expr)
                        continue;
                    if (expr->group() != expressions::expression_group::compare) {
                        can_project = false;
                        break;
                    }
                    const auto& ce = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
                    if (!collect_cols_from_compare(ce, raw_cols)) {
                        can_project = false;
                        break;
                    }
                }
            }

            if (can_project && !raw_cols.empty()) {
                normalize(raw_cols);
                static_cast<logical_plan::node_aggregate_t*>(agg_node.get())->set_projected_cols(std::move(raw_cols));
            }

            // Recurse into non-trivial children (join or nested aggregate).
            if (data_child) {
                if (data_child->type() == logical_plan::node_type::join_t) {
                    const auto& projected =
                        static_cast<const logical_plan::node_aggregate_t*>(agg_node.get())->projected_cols();
                    process_join(data_child, projected, md);
                } else if (data_child->type() == logical_plan::node_type::aggregate_t) {
                    process_aggregate(data_child, md);
                }
            }
        }

    } // namespace

    void prune_columns(const logical_plan::node_ptr& root) {
        if (!root)
            return;

        // Build oid → column_count map from sibling catalog_resolve_table_t
        // nodes that enrich already populated with resolved_metadata().
        table_cols_map md;
        collect_table_md(root, md);

        // BFS over the whole plan, processing every aggregate_t we encounter.
        std::vector<logical_plan::node_ptr> stack{root};
        while (!stack.empty()) {
            auto current = std::move(stack.back());
            stack.pop_back();
            if (current->type() == logical_plan::node_type::aggregate_t) {
                process_aggregate(current, md);
                // process_aggregate already recurses into join_t / nested aggregate_t.
                continue;
            }
            for (const auto& child : current->children()) {
                stack.push_back(child);
            }
        }
    }

} // namespace components::planner::optimizer