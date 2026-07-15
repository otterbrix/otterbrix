#include "pushdown_aggregate.hpp"

#include <cctype>
#include <string>
#include <string_view>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;

        // Children whose presence means the aggregate does NOT sit over a single
        // owned base table (join = multi-table; nested aggregate = a sub-aggregate
        // reduce; data = client-injected raw chunk with no owning agent; cte_scan /
        // union / intersect / recursive_cte = multi-source). Any of these => skip (a).
        bool is_shape_breaking_child(lp::node_type t) noexcept {
            switch (t) {
                case lp::node_type::join_t:
                case lp::node_type::aggregate_t:
                case lp::node_type::data_t:
                case lp::node_type::cte_scan_t:
                case lp::node_type::union_t:
                case lp::node_type::intersect_t:
                case lp::node_type::recursive_cte_t:
                    return true;
                default:
                    return false;
            }
        }

        // The node_group_t child carries the aggregate exprs (expression_group::
        // aggregate) interleaved with the scalar group-key exprs. Skip (b) fires
        // if any aggregate is distinct or non-mergeable. Mergeability is a resolved
        // capability (aggregate_expression::is_mergeable(), stamped at validate from
        // the function's function::is_mergeable()) — only SUM/COUNT/MIN/MAX/AVG (or
        // a future UDA with a fragment-merge kernel) carry it. Everything else must
        // stay coordinator-side.
        bool has_unmergeable_aggregate(const lp::node_group_t* group) noexcept {
            for (const auto& expr : group->expressions()) {
                if (expr->group() != ce::expression_group::aggregate) {
                    continue;
                }
                const auto* agg = static_cast<const ce::aggregate_expression_t*>(expr.get());
                if (agg->is_distinct() || !agg->is_mergeable()) {
                    return true;
                }
            }
            return false;
        }

        // A user-defined function is any resolved function_uid at or beyond the
        // builtin set. register_default_functions registers EXACTLY the
        // DEFAULT_FUNCTIONS entries (uids [0, N)); the owning agent rebuilds its
        // registry with the SAME register_default_functions and NOTHING else (it has
        // no access to the coordinator's UDF registrations). So a uid >= N is a UDF
        // the agent cannot resolve — building the pushed fragment there would look up
        // a null function pointer and deref it. invalid_function_uid (unresolved) is
        // NOT treated as a UDF: it is left to the coordinator's normal resolution.
        bool is_udf_uid(components::compute::function_uid uid) noexcept {
            return uid != components::compute::invalid_function_uid &&
                   uid >= components::compute::DEFAULT_FUNCTIONS.size();
        }

        bool expr_references_udf(const ce::expression_ptr& expr);

        // param_storage is variant<parameter_id_t, key_t, expression_ptr>; only the
        // nested-expression alternative can carry a further function reference.
        bool param_references_udf(const ce::param_storage& param) {
            if (std::holds_alternative<ce::expression_ptr>(param)) {
                return expr_references_udf(std::get<ce::expression_ptr>(param));
            }
            return false;
        }

        // True iff any function_expression / aggregate_expression in the tree is a
        // UDF (or one of their nested argument expressions is). Recurses through
        // function/aggregate/scalar params and compare children/operands. R14: tag
        // via group() + static_cast (NO dynamic_cast).
        bool expr_references_udf(const ce::expression_ptr& expr) {
            if (!expr) {
                return false;
            }
            switch (expr->group()) {
                case ce::expression_group::function: {
                    const auto* f = static_cast<const ce::function_expression_t*>(expr.get());
                    if (is_udf_uid(f->function_uid())) {
                        return true;
                    }
                    for (const auto& a : f->args()) {
                        if (param_references_udf(a)) {
                            return true;
                        }
                    }
                    return false;
                }
                case ce::expression_group::aggregate: {
                    const auto* a = static_cast<const ce::aggregate_expression_t*>(expr.get());
                    if (is_udf_uid(a->function_uid())) {
                        return true;
                    }
                    for (const auto& p : a->params()) {
                        if (param_references_udf(p)) {
                            return true;
                        }
                    }
                    return false;
                }
                case ce::expression_group::scalar: {
                    const auto* s = static_cast<const ce::scalar_expression_t*>(expr.get());
                    for (const auto& p : s->params()) {
                        if (param_references_udf(p)) {
                            return true;
                        }
                    }
                    return false;
                }
                case ce::expression_group::compare: {
                    const auto* c = static_cast<const ce::compare_expression_t*>(expr.get());
                    if (param_references_udf(c->left()) || param_references_udf(c->right())) {
                        return true;
                    }
                    for (const auto& child : c->children()) {
                        if (expr_references_udf(child)) {
                            return true;
                        }
                    }
                    return false;
                }
                default:
                    return false;
            }
        }

        // Walk the whole aggregate fragment sub-tree (WHERE filter, group/aggregate
        // exprs, projections, group keys — every node carries its exprs in
        // expressions()) checking for any UDF reference.
        bool subtree_references_udf(const lp::node_ptr& node) {
            if (!node) {
                return false;
            }
            for (const auto& expr : node->expressions()) {
                if (expr_references_udf(expr)) {
                    return true;
                }
            }
            for (const auto& child : node->children()) {
                if (subtree_references_udf(child)) {
                    return true;
                }
            }
            return false;
        }

        // Find the single node_group_t child of the aggregate node (the flat
        // sibling that holds the reduce). Returns nullptr if none.
        lp::node_group_t* find_group_child(const lp::node_ptr& aggregate) noexcept {
            for (const auto& child : aggregate->children()) {
                if (child && child->type() == lp::node_type::group_t) {
                    return static_cast<lp::node_group_t*>(child.get());
                }
            }
            return nullptr;
        }

        // If `node` is a pushable single-owned-table aggregate, stamp its group
        // child; otherwise leave everything untouched. Total (no-op on non-match).
        void try_stamp_aggregate(const lp::node_ptr& node) {
            if (node->type() != lp::node_type::aggregate_t) {
                return;
            }
            // Must target ONE resolved owned table. enrich stamps table_oid()
            // before optimize() runs; INVALID_OID => not a single owned table.
            if (node->table_oid() == components::catalog::INVALID_OID) {
                return;
            }
            // Skip (a): any shape-breaking child means it is not one owned table.
            for (const auto& child : node->children()) {
                if (child && is_shape_breaking_child(child->type())) {
                    return;
                }
            }
            auto* group = find_group_child(node);
            if (group == nullptr) {
                return;
            }
            // Skip (c): HAVING is a hard correctness gate — a coordinator kernel-
            // merge reduce would never evaluate it. HAVING is a having_t child of
            // the aggregate (not shape-breaking, so find_group_child still finds the
            // group); be conservative and skip on ANY having_t child.
            for (const auto& child : node->children()) {
                if (child && child->type() == lp::node_type::having_t) {
                    return;
                }
            }
            // Skip (d): DISTINCT ON dedups on the ON-key subset BELOW the projection on the
            // coordinator path; the pushdown reduce / group_merge layout can't resolve those ON
            // indices. Force it coordinator-side. Plain DISTINCT (empty ON list) still pushes down.
            if (!static_cast<const lp::node_aggregate_t*>(node.get())->distinct_on_keys().empty()) {
                return;
            }
            // Skip (b): a distinct or non-mergeable aggregate stays coordinator-side.
            if (has_unmergeable_aggregate(group)) {
                return;
            }
            // Skip (e): a UDF anywhere in the fragment (WHERE filter, aggregate
            // argument, projection, group key) is NOT pushable — the owning agent
            // rebuilds its function registry with register_default_functions ONLY, so
            // a user-defined function_uid resolves to null there and the
            // predicate/kernel would deref it. A "computed" shape in the R6 sense:
            // the coordinator (which HOLDS the UDF) must run it.
            if (subtree_references_udf(node)) {
                return;
            }
            // Scalar (0 group keys -> empty-keys single group) and grouped (>0 keys)
            // shapes are BOTH pushable; the key count does not gate the stamp —
            // physgen branches on it when lowering.
            group->set_pushdown(true);
        }

        void walk(const lp::node_ptr& node) {
            if (!node) {
                return;
            }
            for (auto& child : node->children()) {
                walk(child);
            }
            try_stamp_aggregate(node);
        }
    } // namespace

    logical_plan::node_ptr pushdown_aggregate(std::pmr::memory_resource* /*resource*/, logical_plan::node_ptr root) {
        walk(root);
        return root;
    }

} // namespace components::planner::optimizer
