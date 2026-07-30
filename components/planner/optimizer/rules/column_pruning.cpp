#include "column_pruning.hpp"

#include <algorithm>
#include <functional>
#include <unordered_map>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
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
                if (n->type() == logical_plan::node_type::catalog_resolve_t) {
                    const auto* rt = static_cast<const logical_plan::node_catalog_resolve_t*>(n);
                    if (rt->kind() == logical_plan::resolve_kind::table) {
                        const auto& md_opt = rt->resolved_metadata();
                        if (md_opt && md_opt->table_oid != components::catalog::INVALID_OID) {
                            out[md_opt->table_oid] = md_opt->columns.size();
                        }
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
            if (expressions::is_key(p)) {
                const auto& key = expressions::as_key(p);
                if (key.path().empty())
                    return true;
                size_t idx = key.path()[0];
                if (idx == SIZE_MAX)
                    return false; // wildcard — disable projection
                cols.push_back(idx);
                return true;
            }
            if (expressions::is_expr(p)) {
                const auto& sub = expressions::as_expr(p);
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

        // Collect the scan columns a plain-SELECT $select projection reads. A plain SELECT
        // (no GROUP BY) carries a select_t — NOT a group_t — so process_aggregate must read
        // it to prune. Returns false to DISABLE projection (read ALL columns) whenever any
        // visible column is not a plain get_field/constant we can statically map to one
        // storage column: a star_expand (SELECT * / t.*), an arithmetic / CASE / COALESCE /
        // function projection, an aggregate, or an unresolved path. Conservative by design —
        // a MISSED column would under-read and corrupt results, so anything not fully
        // enumerable falls back to reading every column (always correct).
        bool collect_cols_from_select(const logical_plan::node_ptr& select_node, std::vector<size_t>& cols) {
            for (const auto& expr : select_node->expressions()) {
                if (!expr)
                    continue;
                if (expr->group() != expressions::expression_group::scalar) {
                    return false; // aggregate / compare / function projection — not enumerable here
                }
                const auto* se = static_cast<const SExpr*>(expr.get());
                switch (se->type()) {
                    case expressions::scalar_type::constant:
                        continue; // a literal reads no scan column
                    case expressions::scalar_type::get_field:
                        break;
                    default:
                        return false; // star_expand / arithmetic / case_when / coalesce / ...
                }
                // get_field input field: params.front() when aliased (SELECT a AS x),
                // else the key itself carries the storage path (mirrors build_pushed_spec).
                const KeyT* field = nullptr;
                if (!se->params().empty()) {
                    if (!expressions::is_key(se->params().front())) {
                        return false; // computed input — not a plain column
                    }
                    field = &expressions::as_key(se->params().front());
                } else {
                    field = &se->key();
                }
                if (field->path().empty()) {
                    return false; // unresolved projection — read all
                }
                size_t idx = field->path()[0];
                if (idx == SIZE_MAX) {
                    return false;
                }
                cols.push_back(idx);
            }
            return true;
        }

        // Collect the scan columns an ORDER BY reads. For a PLAIN select the sort runs over
        // the scan, so its keys are scan columns that must be present. Returns false to
        // DISABLE projection on any key we cannot resolve to a single storage column
        // (positional / output-alias / expression sort keys) — reading all is always safe.
        bool collect_cols_from_sort(const logical_plan::node_ptr& sort_node, std::vector<size_t>& cols) {
            for (const auto& expr : sort_node->expressions()) {
                if (!expr)
                    continue;
                if (expr->group() != expressions::expression_group::sort) {
                    return false;
                }
                const auto* se = static_cast<const expressions::sort_expression_t*>(expr.get());
                const auto& p = se->key().path();
                if (p.empty() || p[0] == SIZE_MAX) {
                    return false;
                }
                cols.push_back(p[0]);
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
                    if (expressions::is_expr(side)) {
                        // Sub-expression in JOIN leaf — bail out.
                        return false;
                    }
                    if (!expressions::is_key(side))
                        return true;
                    const auto& key = expressions::as_key(side);
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

            // Collect the scan columns this aggregate's own pipeline stages read: a GROUP BY
            // ($group) or plain projection ($select), plus WHERE ($match), ORDER BY ($sort)
            // and DISTINCT ON. Exactly one of $group / $select is the output enumerator.
            std::vector<size_t> raw_cols;
            bool can_project = true;
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(agg_node.get());
            // This rule's own reading of the roles: LIMIT and HAVING never contribute
            // scan columns, so they are simply not consulted; `source` is the nested
            // subtree (join_t / aggregate_t subquery / data_t / union_t, ...) that gets
            // its own recursive pass below.
            const auto roles = agg->pipeline();
            const logical_plan::node_ptr& group_child = roles.group;
            const logical_plan::node_ptr& select_child = roles.select;
            const logical_plan::node_ptr& match_child = roles.match;
            const logical_plan::node_ptr& sort_child = roles.sort;
            const logical_plan::node_ptr& data_child = roles.source;

            // Projection needs an enumerator of the output columns:
            //   * GROUP BY  ($group) enumerates every scan column the aggregation touches
            //     (GROUP BY keys + aggregate args). $select / $sort / $having ABOVE a GROUP BY
            //     address the grouped OUTPUT, not scan columns, so they are NOT collected.
            //   * A plain projection ($select, no $group) enumerates the projected scan
            //     columns; ORDER BY and DISTINCT ON then ALSO read scan columns, so they are
            //     collected too.
            // A bare scan (neither $group nor $select — e.g. SELECT *) must return ALL columns.
            if (group_child) {
                if (!collect_cols_from_node(group_child, raw_cols))
                    can_project = false;
            } else if (select_child) {
                if (!collect_cols_from_select(select_child, raw_cols))
                    can_project = false;
                if (can_project && sort_child && !collect_cols_from_sort(sort_child, raw_cols))
                    can_project = false;
                if (can_project) {
                    for (const auto& k : agg->distinct_on_keys()) {
                        const auto& p = k.path();
                        if (p.empty() || p[0] == SIZE_MAX) {
                            can_project = false;
                            break;
                        }
                        raw_cols.push_back(p[0]);
                    }
                }
            } else {
                can_project = false; // SELECT * / bare scan — read all columns
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