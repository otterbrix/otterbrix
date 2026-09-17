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
#include <components/physical_plan/operators/operator_group_merge.hpp>
#include <components/physical_plan/operators/operator_hash_group.hpp>
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

    // Falls back to the coordinator aggregate (byte-identical result) whenever the shape is not POD-
    // representable: a computed/arithmetic group key, a HAVING, or a distinct/multi-arg/UDF aggregate arg.
    bool build_pushed_spec(const lp::node_group_t* group,
                           const lp::node_ptr& agg_node,
                           std::pmr::memory_resource* resource,
                           ops::pushed_aggregate_spec_t& out) {
        {
            // Defense-in-depth: the optimizer already skips any aggregate with a having_t child.
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
            // Pass 1: the GROUP BY keys; output_key_of[i] is SIZE_MAX for a key the target list never names.
            std::pmr::vector<size_t> output_key_of(group->expressions().size(), SIZE_MAX, resource);
            for (size_t i = 0; i < group->expressions().size(); i++) {
                const auto& expr = group->expressions()[i];
                if (expr->group() != ce::expression_group::scalar) {
                    continue;
                }
                const auto* s = static_cast<const ce::scalar_expression_t*>(expr.get());
                const ce::key_t* field = nullptr;
                bool emits_output = true;
                switch (s->type()) {
                    case ce::scalar_type::group_field:
                        // A pushed_group_key_t is a name plus a resolved column-index path, and the
                        // agent rebuilds operator_group by that path. A key that computes something
                        // has no path so we can not push it
                        if (!s->params().empty()) {
                            return false;
                        }
                        field = &s->key();
                        emits_output = false;
                        break;
                    case ce::scalar_type::get_field:
                        if (!s->params().empty() && !std::holds_alternative<ce::key_t>(s->params().front())) {
                            return false;
                        }
                        field = s->params().empty() ? &s->key() : &std::get<ce::key_t>(s->params().front());
                        break;
                    default:
                        return false;
                }
                if (s->key().storage().empty()) {
                    return false;
                }
                const auto& path = field->path();
                bool reused = false;
                for (size_t k = 0; emits_output && !path.empty() && k < out.group_keys.size(); k++) {
                    const auto& existing = out.group_keys[k].path;
                    if (existing.size() == path.size() && std::equal(existing.begin(), existing.end(), path.begin())) {
                        output_key_of[i] = k;
                        reused = true;
                        break;
                    }
                }
                if (reused) {
                    continue;
                }
                if (emits_output) {
                    output_key_of[i] = out.group_keys.size();
                }
                ops::pushed_group_key_t gk{resource};
                const auto& name = s->key().storage().back();
                gk.name.assign(name.data(), name.size());
                gk.path.assign(path.begin(), path.end());
                out.group_keys.push_back(std::move(gk));
            }

            // Pass 2: the aggregates sit at group_keys.size() + their index, which pass 1 has now settled.
            for (size_t i = 0; i < group->expressions().size(); i++) {
                const auto& expr = group->expressions()[i];
                if (expr->group() == ce::expression_group::scalar) {
                    if (output_key_of[i] != SIZE_MAX) {
                        out.outputs.push_back(expr); // the target list naming this key
                    }
                    continue; // the key itself was added in pass 1
                }
                if (expr->group() == ce::expression_group::aggregate) {
                    const auto* a = static_cast<const ce::aggregate_expression_t*>(expr.get());
                    // uid >= DEFAULT_FUNCTIONS.size() is the agent's own RESOLVABILITY gate, a DIFFERENT
                    // concern from mergeability, which the optimizer's pushdown stamp already enforced.
                    if (a->is_distinct() || a->function_uid() == components::compute::invalid_function_uid ||
                        a->function_uid() >= components::compute::DEFAULT_FUNCTIONS.size()) {
                        return false;
                    }
                    ops::pushed_aggregate_t pa{resource};
                    pa.function_name.assign(a->function_name().data(), a->function_name().size());
                    pa.func_uid = a->function_uid();
                    pa.distinct = false;
                    pa.result_type = a->result_type();
                    const auto alias = a->key().as_pmr_string();
                    pa.alias.assign(alias.data(), alias.size());
                    // A multi-arg/expression argument (SUM(a+b)) is not representable.
                    if (a->params().empty()) {
                    } else if (a->params().size() == 1 && std::holds_alternative<ce::key_t>(a->params().front())) {
                        const auto& kp = std::get<ce::key_t>(a->params().front()).path();
                        pa.arg_col_path.assign(kp.begin(), kp.end());
                    } else {
                        return false;
                    }
                    out.outputs.push_back(expr);
                    out.aggregates.push_back(std::move(pa));
                } else {
                    return false;
                }
            }
            if (!out.active()) {
                return false;
            }
            if (agg_node->has_output_types()) {
                out.output_types.assign(agg_node->output_types().begin(), agg_node->output_types().end());
            }
            out.input_types.assign(group->input_types().begin(), group->input_types().end());
            return true;
        }
    }

    namespace {
        // Factored so pushed_reduce_scan and the coordinator transfer_scan derive the same projection.
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

        // Returns nullptr unless the WHERE lowers to a plain full_scan via create_plan_match.
        // SINGLE-OWNER INVARIANT: correct only while ONE agent owns the whole table (pool_idx_for_oid
        // routing) — do not extend this lowering past that assumption.
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
        for (const components::logical_plan::node_ptr& child : node->children()) {
            if (child->type() == node_type::limit_t) {
                const auto* limit_node = static_cast<const components::logical_plan::node_limit_t*>(child.get());
                limit = limit_node->limit();
                break;
            }
        }

        auto* plan_resource = context.has_table_oid(node->table_oid()) ? context.resource : node->resource();

        // Populated by the column_pruning optimizer rule; empty means read all columns.
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        const auto& projected_cols = agg_node->projected_cols();

        // operator_limit is the single authoritative limiter; sources below only get an advisory read-cap.
        const bool limit_effective =
            limit.limit() != components::logical_plan::limit_t::unlimit().limit() || limit.offset() != 0;

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

        // Aggregate-pushdown: coordinator group/aggregate are dropped for a group_merge over a pushed scan.
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
                        push_sort_op = create_plan_sort(
                            context,
                            child,
                            static_cast<const components::logical_plan::node_sort_t*>(child.get())->read_cap());
                    } else if (child->type() == node_type::select_t) {
                        push_select_op = create_plan_select(context, child);
                    }
                }
                if (push_sort_op) {
                    push_sort_op->set_children(std::move(executor));
                    executor = std::move(push_sort_op);
                }
                if (push_select_op) {
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

        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        components::operators::operator_ptr having_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr select_op;
        components::operators::operator_ptr child_op;

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t:
                    match_op = create_plan_match(
                        context,
                        child,
                        static_cast<const components::logical_plan::node_match_t*>(child.get())->read_cap(),
                        projected_cols);
                    // Must refuse the aggregate: falling through would swap it for the no-table sentinel
                    // transfer_scan below, which FABRICATES a synthetic row for a table that does not exist.
                    if (!match_op) {
                        return nullptr;
                    }
                    break;
                case node_type::group_t:
                    group_op = create_plan(context,
                                           function_registry,
                                           child,
                                           components::logical_plan::limit_t::unlimit(),
                                           params);
                    break;
                case node_type::sort_t:
                    sort_op = create_plan_sort(
                        context,
                        child,
                        static_cast<const components::logical_plan::node_sort_t*>(child.get())->read_cap());
                    break;
                case node_type::select_t:
                    select_op = create_plan_select(context, child);
                    break;
                case node_type::having_t:
                    // Spliced between the group and the sort; operator_limit alone provides the window.
                    having_op = create_plan_having(context, child);
                    break;
                default:
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

        components::operators::operator_ptr executor;
        if (child_op) {
            executor = std::move(child_op);
            if (match_op) {
                match_op->set_children(std::move(executor));
                executor = std::move(match_op);
            }
        } else {
            // The base scan comes from the declaration, not the oid: INVALID_OID means both no-FROM and unresolved.
            if (!match_op) {
                switch (agg_node->source()) {
                    case components::logical_plan::match_source::none:
                        break;
                    case components::logical_plan::match_source::table:
                        if (!context.has_table_oid(node->table_oid())) {
                            return nullptr;
                        }
                        break;
                }
            }
            std::vector<size_t> projected_cols = relkind_projected_cols(context, node, agg_node->projected_cols());
            executor = match_op ? std::move(match_op)
                                : static_cast<components::operators::operator_ptr>(boost::intrusive_ptr(
                                      new components::operators::transfer_scan(plan_resource,
                                                                               node->table_oid(),
                                                                               agg_node->read_cap(),
                                                                               std::move(projected_cols))));
        }
        if (group_op) {
            // Forwarded so zero input rows still build correctly-typed results (PostgreSQL TupleDesc model).
            if (node->has_output_types()) {
                group_op->set_output_types(node->output_types());
            }
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (having_op) {
            having_op->set_children(std::move(executor));
            executor = std::move(having_op);
        }
        if (sort_op) {
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }
        // DISTINCT ON dedups on the ON-key subset BELOW the projection, so ON columns that don't survive it
        // are still present; keep-first over sorted input gives "first row per ON key in ORDER BY order".
        if (agg_node->is_distinct() && !agg_node->distinct_on_keys().empty()) {
            auto distinct_op =
                context.has_table_oid(node->table_oid())
                    ? boost::intrusive_ptr(
                          new components::operators::operator_distinct_t(context.resource, context.log.clone()))
                    : boost::intrusive_ptr(new components::operators::operator_distinct_t(node->resource(), log_t{}));
            std::pmr::vector<size_t> on_cols(node->resource());
            on_cols.reserve(agg_node->distinct_on_keys().size());
            for (const auto& key : agg_node->distinct_on_keys()) {
                on_cols.push_back(key.path().front());
            }
            distinct_op->set_on_keys(std::move(on_cols));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }
        if (select_op) {
            select_op->set_children(std::move(executor));
            executor = std::move(select_op);
        }

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
