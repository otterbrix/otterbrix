#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include "create_plan_sort.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/compute/function.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan/operators/operator_group.hpp>
#include <components/physical_plan/operators/operator_group_merge.hpp>
#include <components/physical_plan/operators/operator_limit.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/pushed_reduce_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;
        namespace ops = components::operators;
    } // namespace

    // Build the POD reduce spec from the group node + aggregate node. Returns
    // false — fall back to the coordinator aggregate (R6 capability select, NOT a
    // redundant fallback) — whenever any shape is NOT faithfully representable as a POD:
    // a coalesce / case_when / arithmetic group key, a HAVING, a distinct / multi-arg /
    // expression / unresolved / UDF aggregate argument. Whatever it cannot encode stays
    // coordinator-side, so the pushed result is byte-identical to the coordinator one.
    bool build_pushed_spec(const lp::node_group_t* group,
                           const lp::node_ptr& agg_node,
                           std::pmr::memory_resource* resource,
                           ops::pushed_aggregate_spec_t& out) {
        {
            // A HAVING (a first-class having_t child of the aggregate) or an internal HAVING
            // aggregate is not POD-representable — fall back to the coordinator group so the
            // operator_having filter above it still runs. (The optimizer's pushdown_aggregate rule
            // already skips any aggregate with a having_t child, so this is defense-in-depth.)
            bool has_having_child = false;
            for (const auto& child : agg_node->children()) {
                if (child && child->type() == node_type::having_t) {
                    has_having_child = true;
                    break;
                }
            }
            if (has_having_child || group->internal_aggregate_count != 0) {
                return false;
            }
            for (const auto& expr : group->expressions()) {
                if (expr->group() == ce::expression_group::scalar) {
                    const auto* s = static_cast<const ce::scalar_expression_t*>(expr.get());
                    const ce::key_t* field = nullptr;
                    switch (s->type()) {
                        case ce::scalar_type::group_field:
                            field = &s->key();
                            break;
                        case ce::scalar_type::get_field:
                            // Guard the variant access (R2): a non-key_t first param
                            // (parameter_id / nested expression) is not POD-representable.
                            if (!s->params().empty() && !ce::is_key(s->params().front())) {
                                return false;
                            }
                            field = s->params().empty() ? &s->key() : &ce::as_key(s->params().front());
                            break;
                        default:
                            return false; // coalesce / case_when / arithmetic — not POD-representable
                    }
                    if (s->key().storage().empty()) {
                        return false;
                    }
                    ops::pushed_group_key_t gk{resource};
                    const auto& name = s->key().storage().back();
                    gk.name.assign(name.data(), name.size());
                    gk.path.assign(field->path().begin(), field->path().end());
                    out.group_keys.push_back(std::move(gk));
                } else if (expr->group() == ce::expression_group::aggregate) {
                    const auto* a = static_cast<const ce::aggregate_expression_t*>(expr.get());
                    // The owning agent rebuilds its registry with register_default_functions ONLY,
                    // so only a RESOLVED builtin uid (< DEFAULT_FUNCTIONS.size()) resolves there.
                    // This uid >= DEFAULT_FUNCTIONS.size() test is the agent RESOLVABILITY gate (a
                    // UDF the agent cannot look up), a DIFFERENT concern from mergeability — the
                    // fragment-merge capability is enforced upstream by the optimizer's pushdown
                    // stamp (aggregate_expression::is_mergeable()); here we only re-check
                    // resolvability + distinct before emitting the pushed spec.
                    if (a->is_distinct() || a->function_uid() == components::compute::invalid_function_uid ||
                        a->function_uid() >= components::compute::DEFAULT_FUNCTIONS.size()) {
                        return false;
                    }
                    ops::pushed_aggregate_t pa{resource};
                    pa.function_name.assign(a->function_name().data(), a->function_name().size());
                    pa.func_uid = a->function_uid();
                    pa.distinct = false;
                    const auto alias = a->key().as_pmr_string();
                    pa.alias.assign(alias.data(), alias.size());
                    // Argument: empty params => COUNT(*); exactly ONE key_t column otherwise. A
                    // multi-arg / expression / parameter argument (SUM(a+b)) is not representable.
                    if (a->params().empty()) {
                        // count-star: arg_col_path stays empty
                    } else if (a->params().size() == 1 && ce::is_key(a->params().front())) {
                        const auto& kp = ce::as_key(a->params().front()).path();
                        pa.arg_col_path.assign(kp.begin(), kp.end());
                    } else {
                        return false;
                    }
                    out.aggregates.push_back(std::move(pa));
                } else {
                    return false; // unexpected expression group in a group node
                }
            }
            if (!out.active()) {
                return false; // neither keys nor aggregates — not a pushable aggregate
            }
            // FINAL output types (keys first, then aggregate values) forwarded from the
            // aggregate node, so the reduce types an empty-slice scalar result instead of NA.
            // Types only — the spec names these same columns itself (pushed_aggregate_spec_t
            // ::output_schema), in the same keys-then-aggregates order.
            out.output_types.reserve(agg_node->output_schema().size());
            for (const auto& column : agg_node->output_schema()) {
                out.output_types.push_back(column.type);
            }
            return true;
        }
    }

    namespace {
        // Storage-chunk column indices for the base scan of `node`, honoring relkind: a computed
        // (relkind='g') relation reads its LIVE columns by chunk_position; any other relation uses
        // the column_pruning output passed as `base_projected_cols`. Empty ⇒ read all columns.
        // Factored so the pushed reduce scan (build_pushdown_scan) and the coordinator
        // transfer_scan (create_plan_aggregate) derive the SAME projection from one rule.
        std::vector<size_t> relkind_projected_cols(const context_storage_t& context,
                                                   const lp::node_ptr& node,
                                                   const std::vector<size_t>& base_projected_cols) {
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
                    projected_cols = base_projected_cols;
                }
            }
            return projected_cols;
        }

        // Lower a pushdown-stamped aggregate to a pushed_reduce_scan (the source shipping the
        // POD spec on the DEDICATED storage_reduce protocol leg) under an operator_group_merge
        // (the coordinator-side aggregate terminal: identity passthrough today, owner of the
        // empty-input scalar row, and the socket a sharded future turns into a real kernel
        // merge). Returns nullptr to fall back to the coordinator aggregate. The WHERE (a match
        // child) is validated via create_plan_match: only a plain full_scan shape (pure compare,
        // no index) is pushable — its expression + projection are LIFTED onto the reduce scan
        // and the probe operator is discarded. With no WHERE the SAME relkind-aware
        // projected_cols the coordinator base scan would use are derived directly (so a
        // relkind='g' computed relation reduces correctly).
        //
        // SINGLE-OWNER INVARIANT: the agent returns FINAL aggregated rows, which is only
        // correct while ONE agent owns the whole table (pool_idx_for_oid routing). Sharding
        // table slices across agents requires per-slice PARTIAL states merged in
        // operator_group_merge — do NOT extend this lowering past that assumption.
        ops::operator_ptr build_pushdown_scan(const context_storage_t& context,
                                              const lp::node_ptr& node,
                                              const lp::node_group_t* group,
                                              const std::vector<size_t>& base_projected_cols) {
            const bool known = context.has_table_oid(node->table_oid());
            auto* resource = known ? context.resource : node->resource();

            // A pushed reduce names its group keys and aggregate inputs by the LOGICAL ordinal
            // the validator resolved, and evaluates them on the agent against a chunk in STORAGE
            // layout. Those agree until a column is dropped. Fall back to the coordinator
            // aggregate over an identity-projected scan for as long as they do not.
            if (ops::scan_identity_projection_t::displaced(context.table_metadata_for(node->table_oid()))) {
                return nullptr;
            }

            ops::pushed_aggregate_spec_t spec{resource};
            if (!build_pushed_spec(group, node, resource, spec)) {
                return nullptr;
            }

            const lp::node_ptr* match_child = nullptr;
            for (const lp::node_ptr& child : node->children()) {
                if (child->type() == node_type::match_t) {
                    match_child = &child;
                    break;
                }
            }

            ce::compare_expression_ptr where_expr; // null == no WHERE
            std::vector<size_t> projected_cols;
            if (match_child != nullptr) {
                auto m = create_plan_match(context, *match_child, lp::limit_t::unlimit(), base_projected_cols);
                if (!m || m->type() != ops::operator_type::full_scan) {
                    return nullptr; // index_scan / operator_match / transfer_scan — not pushable
                }
                const auto* fs = static_cast<const ops::full_scan*>(m.get());
                where_expr = fs->expression();
                projected_cols = fs->projected_cols();
            } else {
                projected_cols = relkind_projected_cols(context, node, base_projected_cols);
            }

            // Merge ctor inputs, taken from the spec BEFORE it moves onto the scan.
            const bool scalar = spec.group_keys.empty();
            std::pmr::vector<components::types::complex_logical_type> merge_types{spec.output_types.begin(),
                                                                                  spec.output_types.end(),
                                                                                  resource};
            std::vector<std::pair<std::string, std::string>> merge_aggs;
            merge_aggs.reserve(spec.aggregates.size());
            for (const auto& a : spec.aggregates) {
                merge_aggs.emplace_back(std::string(a.alias.begin(), a.alias.end()),
                                        std::string(a.function_name.begin(), a.function_name.end()));
            }

            auto scan = boost::intrusive_ptr(new ops::pushed_reduce_scan(resource,
                                                                         known ? context.log.clone() : log_t{},
                                                                         node->table_oid(),
                                                                         where_expr,
                                                                         std::move(projected_cols),
                                                                         std::move(spec)));
            auto merge = boost::intrusive_ptr(new ops::operator_group_merge_t(resource,
                                                                              known ? context.log.clone() : log_t{},
                                                                              scalar,
                                                                              std::move(merge_types),
                                                                              std::move(merge_aggs)));
            merge->set_children(std::move(scan));
            return merge;
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

        auto* plan_resource = context.has_table_oid(node->table_oid()) ? context.resource : node->resource();

        // projected_cols is populated by the column_pruning optimizer rule
        // (components/planner/optimizer/rules/column_pruning.cpp). Empty means
        // "no projection" → read all columns.
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        const auto& projected_cols = agg_node->projected_cols();

        // Role-classified children, read once for all three lowering shapes below.
        // This lowering's own policy: a LIMIT child is never lowered as an operator
        // (wrap_limit owns the single authoritative window), and `source` is the
        // explicit sub-plan that replaces the otherwise-implicit transfer_scan.
        const auto roles = agg_node->pipeline();

        // operator_limit is the single authoritative limiter: inserted as the OUTERMOST
        // node (above DISTINCT — SQL applies LIMIT after DISTINCT) when the LIMIT/OFFSET is
        // effective, applying the real [offset, offset+limit) window. Every source below gets
        // only an advisory read-cap (offset 0) that the pushdown_limit rule stamped on the
        // eligible node; an unstamped node reads unlimit(). OFFSET is thus applied in exactly
        // one place, so double-OFFSET is structurally impossible.
        const bool limit_effective =
            limit.limit() != components::logical_plan::limit_t::unlimit().limit() || limit.offset() != 0;

        // Wrap `op` in the canonical operator_limit as the OUTERMOST node when the
        // LIMIT/OFFSET is effective; else pass it through unchanged. Shared by the
        // pushdown and normal return paths.
        auto wrap_limit = [&](components::operators::operator_ptr op) -> components::operators::operator_ptr {
            if (!limit_effective) {
                return op;
            }
            auto limit_op =
                context.has_table_oid(node->table_oid())
                    ? boost::intrusive_ptr(
                          new components::operators::operator_limit_t(context.resource, context.log.clone(), limit))
                    : boost::intrusive_ptr(
                          new components::operators::operator_limit_t(node->resource(), log_t{}, limit));
            limit_op->set_children(std::move(op));
            return limit_op;
        };

        // --- Aggregate-pushdown lowering ---
        // When the optimizer stamped the group child pushdown() AND the whole aggregate is
        // faithfully representable as a POD spec + a WHERE that lowers to a plain full_scan,
        // lower to a group_merge over a pushed_reduce_scan carrying the reduce spec: the owning
        // agent reduces its OWN slice (the EXISTING operator_group rebuilt from the POD) and
        // streams back the FINAL aggregated rows, which pass through unchanged. The coordinator
        // group/aggregate are DROPPED (identity passthrough); only the coordinator
        // sort/select/distinct layer on top, exactly as the normal chain below would. A
        // non-representable shape returns nullptr and falls through to the normal coordinator
        // aggregate (R6: capability select).
        const components::logical_plan::node_group_t* pushdown_group = nullptr;
        if (roles.group) {
            const auto* g = static_cast<const components::logical_plan::node_group_t*>(roles.group.get());
            if (g->pushdown()) {
                pushdown_group = g;
            }
        }
        if (pushdown_group != nullptr) {
            if (auto pushdown_scan = build_pushdown_scan(context, node, pushdown_group, agg_node->projected_cols())) {
                components::operators::operator_ptr executor = std::move(pushdown_scan);
                components::operators::operator_ptr push_sort_op;
                components::operators::operator_ptr push_select_op;
                if (roles.sort) {
                    push_sort_op = create_plan_sort(
                        context,
                        roles.sort,
                        static_cast<const components::logical_plan::node_sort_t*>(roles.sort.get())->read_cap());
                    // A refused child invalidates the whole plan: the `if` guards below only
                    // splice, so a nullptr here would silently drop the ORDER BY / projection
                    // and return the raw pushed-reduce output as SUCCESS.
                    if (!push_sort_op) {
                        return nullptr;
                    }
                }
                if (roles.select) {
                    push_select_op = create_plan_select(context, roles.select, params);
                    if (!push_select_op) {
                        return nullptr;
                    }
                }
                if (push_sort_op) {
                    push_sort_op->set_children(std::move(executor));
                    executor = std::move(push_sort_op);
                }
                if (push_select_op) {
                    push_select_op->set_output_schema(node->output_schema());
                    push_select_op->set_children(std::move(executor));
                    executor = std::move(push_select_op);
                }
                if (agg_node->is_distinct()) {
                    auto distinct_op =
                        context.has_table_oid(node->table_oid())
                            ? boost::intrusive_ptr(
                                  new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                            : boost::intrusive_ptr(
                                  new components::operators::operator_distinct_t(node->resource(), log_t{}));
                    distinct_op->set_children(std::move(executor));
                    executor = std::move(distinct_op);
                }
                return wrap_limit(std::move(executor));
            }
        }

        // Build operator chain: scan/child → match → group → sort → select
        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        components::operators::operator_ptr having_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr select_op;
        components::operators::operator_ptr child_op;

        // A non-scan source (UNION / recursive-CTE / join): always unlimited —
        // operator_limit applies the merged window on top. Forwarding the outer
        // limit into each arm would apply limit/offset twice (wrong OFFSET).
        //
        // EVERY non-role child is lowered, in child order, not just the one the role
        // classifier kept (its `source` slot is last-wins): a child that fails to lower
        // (e.g. a host-extension node with no injected create_plan rule) must surface as
        // an invalid plan even when a lowerable sibling follows it — NOT fall through to
        // the transfer_scan branch below (which is only for an aggregate with no explicit
        // source child), which would silently mis-execute over the wrong source. The last
        // successful lowering is the source, matching pipeline()'s slot.
        for (const auto& child : node->children()) {
            if (!child) {
                continue;
            }
            switch (child->type()) {
                case node_type::match_t:
                case node_type::group_t:
                case node_type::having_t:
                case node_type::sort_t:
                case node_type::select_t:
                case node_type::limit_t:
                    break;
                default: {
                    child_op = create_plan(context,
                                           function_registry,
                                           child,
                                           components::logical_plan::limit_t::unlimit(),
                                           params);
                    if (!child_op) {
                        return nullptr;
                    }
                    break;
                }
            }
        }
        if (roles.match) {
            // Call create_plan_match directly so we can pass projected_cols. The
            // read-cap is the pushdown_limit stamp on this match node (unlimit when
            // the rule left it unstamped — e.g. under a sort / group / distinct).
            match_op = create_plan_match(
                context,
                roles.match,
                static_cast<const components::logical_plan::node_match_t*>(roles.match.get())->read_cap(),
                projected_cols);
        }
        // A refused role child invalidates the whole plan. The splice guards below are
        // presence checks, not error checks: letting a nullptr through them silently drops
        // the grouping / HAVING / ORDER BY / projection and runs the query without it.
        // (create_plan_match never refuses — the no-FROM sentinel scan is its own arm —
        // so match_op stays uncheck'd: a null match_op only means "no WHERE".)
        if (roles.group) {
            // A GROUP BY is never cardinality-preserving from its scan and has no
            // output-cap hook — operator_limit windows the full grouped output.
            group_op = create_plan(context,
                                   function_registry,
                                   roles.group,
                                   components::logical_plan::limit_t::unlimit(),
                                   params);
            if (!group_op) {
                return nullptr;
            }
        }
        if (roles.having) {
            // HAVING → dedicated operator_having filter, spliced ABOVE the group (below),
            // between the group and the sort. It has no window (operator_limit is the sole
            // window), so create_plan_having takes no limit.
            having_op = create_plan_having(context, roles.having);
            if (!having_op) {
                return nullptr;
            }
        }
        if (roles.sort) {
            // The full sort truncates its OUTPUT to the read-cap the pushdown_limit
            // rule stamped (unlimit when a DISTINCT sits above); operator_limit
            // applies the real window on top.
            sort_op = create_plan_sort(
                context,
                roles.sort,
                static_cast<const components::logical_plan::node_sort_t*>(roles.sort.get())->read_cap());
            if (!sort_op) {
                return nullptr;
            }
        }
        if (roles.select) {
            select_op = create_plan_select(context, roles.select, params);
            if (!select_op) {
                return nullptr;
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
            std::vector<size_t> projected_cols = relkind_projected_cols(context, node, agg_node->projected_cols());
            executor = match_op
                           ? std::move(match_op)
                           : static_cast<components::operators::operator_ptr>(boost::intrusive_ptr(
                                 new components::operators::transfer_scan(plan_resource,
                                                                          node->table_oid(),
                                                                          agg_node->read_cap(),
                                                                          std::move(projected_cols),
                                                                          context.table_metadata_for(
                                                                              node->table_oid()))));
        }
        if (group_op) {
            // Forward the plan-time resolved output schema (stamped on the aggregate node
            // by validate_schema) into the group operator, so it builds correctly-typed and
            // correctly-named results over zero input rows (PostgreSQL TupleDesc model)
            // instead of NA. set_output_schema is a base virtual (no-op by default) -> no downcast.
            group_op->set_output_schema(node->output_schema());
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        // HAVING filters the aggregated output — AFTER the group, BEFORE ORDER BY / projection.
        if (having_op) {
            having_op->set_children(std::move(executor));
            executor = std::move(having_op);
        }
        if (sort_op) {
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }
        // DISTINCT ON dedups on the ON-key subset BELOW the projection, so ON columns that do not
        // survive projection are still present. Keep-first over the sorted input gives "first row per
        // ON key in ORDER BY order". Plain DISTINCT (empty ON list) stays ABOVE the projection (below).
        if (agg_node->is_distinct() && !agg_node->distinct_on_keys().empty()) {
            auto distinct_op =
                context.has_table_oid(node->table_oid())
                    ? boost::intrusive_ptr(
                          new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                    : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            std::pmr::vector<size_t> on_cols(node->resource());
            on_cols.reserve(agg_node->distinct_on_keys().size());
            for (const auto& key : agg_node->distinct_on_keys()) {
                on_cols.push_back(key.path().front()); // resolved to a scan/group-output column by validation
            }
            distinct_op->set_on_keys(std::move(on_cols));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }
        if (select_op) {
            // Forward the plan-resolved output types onto the projection columns so a
            // CASE/COALESCE/deep-field column over zero rows stays correctly typed instead
            // of being dropped as an untyped placeholder. Base virtual -> no downcast.
            select_op->set_output_schema(node->output_schema());
            select_op->set_children(std::move(executor));
            executor = std::move(select_op);
        }

        // Plain DISTINCT (whole-row dedup) sits ABOVE the projection. DISTINCT ON was already
        // spliced below the projection above, so guard on an empty ON list here.
        if (agg_node->is_distinct() && agg_node->distinct_on_keys().empty()) {
            auto distinct_op =
                context.has_table_oid(node->table_oid())
                    ? boost::intrusive_ptr(
                          new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                    : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }

        return wrap_limit(std::move(executor));
    }

} // namespace services::planner::impl
