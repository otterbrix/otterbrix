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
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan/operators/operator_group.hpp>
#include <components/physical_plan/operators/operator_group_merge.hpp>
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

    // Build the POD reduce spec (SEAM B) from the group node + aggregate node. Returns
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
            if (group->having() != nullptr || group->internal_aggregate_count != 0) {
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
                            if (!s->params().empty() && !std::holds_alternative<ce::key_t>(s->params().front())) {
                                return false;
                            }
                            field = s->params().empty() ? &s->key()
                                                        : &std::get<ce::key_t>(s->params().front());
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
                    } else if (a->params().size() == 1 &&
                               std::holds_alternative<ce::key_t>(a->params().front())) {
                        const auto& kp = std::get<ce::key_t>(a->params().front()).path();
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
            if (agg_node->has_output_types()) {
                out.output_types.assign(agg_node->output_types().begin(), agg_node->output_types().end());
            }
            return true;
        }
    }

    namespace {
        // Storage-chunk column indices for the base scan of `node`, honoring relkind: a computed
        // (relkind='g') relation reads its LIVE columns by chunk_position; any other relation uses
        // the column_pruning output passed as `base_projected_cols`. Empty ⇒ read all columns.
        // Factored so the pushed full_scan (build_pushdown_scan) and the coordinator transfer_scan
        // (create_plan_aggregate) derive the SAME projection from one rule.
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

        // When ORDER BY is present, scan all rows — limit+offset are applied post-sort.
        bool has_sort = false;
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::sort_t) {
                has_sort = true;
                break;
            }
        }
        auto scan_limit = has_sort ? components::logical_plan::limit_t::unlimit() : limit;

        // --- Aggregate-pushdown lowering (SEAM B) ---
        // When the optimizer stamped the group child pushdown() AND the whole aggregate is
        // faithfully representable as a POD spec + a full_scan-carryable WHERE, lower to a
        // full_scan that CARRIES the reduce spec: the owning agent reduces its OWN slice (the
        // EXISTING operator_group rebuilt from the POD) and streams back the FINAL aggregated
        // rows, which pass through unchanged. The coordinator group/aggregate are DROPPED
        // (identity passthrough); only the coordinator sort/select/distinct layer on top,
        // exactly as the normal chain below would. A non-representable shape returns nullptr and
        // falls through to the normal coordinator aggregate (R6: capability select).
        const components::logical_plan::node_group_t* pushdown_group = nullptr;
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::group_t) {
                const auto* g = static_cast<const components::logical_plan::node_group_t*>(child.get());
                if (g->pushdown()) {
                    pushdown_group = g;
                }
                break;
            }
        }
        if (pushdown_group != nullptr) {
            if (auto pushdown_scan = build_pushdown_scan(context, node, pushdown_group, agg_node->projected_cols())) {
                components::operators::operator_ptr executor = std::move(pushdown_scan);
                components::operators::operator_ptr push_sort_op;
                components::operators::operator_ptr push_select_op;
                for (const components::logical_plan::node_ptr& child : node->children()) {
                    if (child->type() == node_type::sort_t) {
                        push_sort_op = create_plan_sort(context, child, limit);
                    } else if (child->type() == node_type::select_t) {
                        push_select_op = create_plan_select(context, child, params);
                    }
                }
                if (push_sort_op) {
                    push_sort_op->set_children(std::move(executor));
                    executor = std::move(push_sort_op);
                }
                if (push_select_op) {
                    if (node->has_output_types()) {
                        push_select_op->set_output_types(node->output_types());
                    }
                    push_select_op->set_children(std::move(executor));
                    executor = std::move(push_select_op);
                }
                if (agg_node->is_distinct()) {
                    auto distinct_op =
                        context.has_table_oid(node->table_oid())
                            ? boost::intrusive_ptr(new components::operators::operator_distinct_t(context.resource,
                                                                                                  context.log.clone()))
                            : boost::intrusive_ptr(
                                  new components::operators::operator_distinct_t(node->resource(), log_t{}));
                    distinct_op->set_children(std::move(executor));
                    executor = std::move(distinct_op);
                }
                return executor;
            }
        }

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
            std::vector<size_t> projected_cols = relkind_projected_cols(context, node, agg_node->projected_cols());
            executor = match_op ? std::move(match_op)
                                : static_cast<components::operators::operator_ptr>(boost::intrusive_ptr(
                                      new components::operators::transfer_scan(plan_resource,
                                                                               node->table_oid(),
                                                                               scan_limit,
                                                                               std::move(projected_cols))));
        }
        if (group_op) {
            // Forward the plan-time resolved output types (stamped on the aggregate node
            // by validate_schema) into the group operator, so it builds correctly-typed
            // results over zero input rows (PostgreSQL TupleDesc model) instead of NA.
            // set_output_types is a base virtual (no-op by default) -> no downcast.
            if (node->has_output_types()) {
                group_op->set_output_types(node->output_types());
            }
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (sort_op) {
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }
        if (select_op) {
            // Forward the plan-resolved output types onto the projection columns so a
            // CASE/COALESCE/deep-field column over zero rows stays correctly typed instead
            // of being dropped as an untyped placeholder. Base virtual -> no downcast.
            if (node->has_output_types()) {
                select_op->set_output_types(node->output_types());
            }
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
