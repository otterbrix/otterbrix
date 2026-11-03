#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include "create_plan_sort.hpp"
#include <components/expressions/clone_expression.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/compute/function.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/physical_plan/operators/operator_distinct.hpp>
#include <components/physical_plan/operators/operator_hash_group.hpp>
#include <components/physical_plan/operators/operator_limit.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    using components::logical_plan::node_type;

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;
        namespace ops = components::operators;
    } // namespace

    namespace {
        // A computed table is read whole: its live columns, whatever the projection.
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
    } // namespace

    plan_result_t create_plan_aggregate(const context_storage_t& context,
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

        auto* plan_resource = context.resource;

        // Populated by the column_pruning optimizer rule; empty means read all columns.
        const auto* agg_node = static_cast<const components::logical_plan::node_aggregate_t*>(node.get());
        const auto& projected_cols = agg_node->projected_cols();

        // operator_limit is the single authoritative limiter; sources below only get an advisory read-cap.

        auto wrap_limit = [&](components::operators::operator_ptr op) -> components::operators::operator_ptr {
            if (!limit.is_effective()) {
                return op;
            }
            auto limit_op = boost::intrusive_ptr(
                new components::operators::operator_limit_t(context.resource, context.log.clone(), limit));
            limit_op->set_children(std::move(op));
            return limit_op;
        };

        components::operators::operator_ptr match_op;
        components::operators::operator_ptr group_op;
        const components::logical_plan::node_t* group_node = nullptr;
        components::operators::operator_ptr having_op;
        components::operators::operator_ptr sort_op;
        components::operators::operator_ptr select_op;
        components::operators::operator_ptr child_op;

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::limit_t:
                    break; // already handled above
                case node_type::match_t: {
                    // A refused WHERE refuses the aggregate: the no-table sentinel transfer_scan below would
                    // FABRICATE a synthetic row for a table that does not exist.
                    VALUE_OR_RETURN(
                        match_op,
                        create_plan_match(
                            context,
                            child,
                            static_cast<const components::logical_plan::node_match_t*>(child.get())->read_cap(),
                            projected_cols));
                    break;
                }
                case node_type::group_t: {
                    group_node = child.get();
                    VALUE_OR_RETURN(group_op,
                                    create_plan(context,
                                                function_registry,
                                                child,
                                                components::logical_plan::limit_t::all(),
                                                params));
                    break;
                }
                case node_type::sort_t: {
                    VALUE_OR_RETURN(
                        sort_op,
                        create_plan_sort(
                            context,
                            child,
                            static_cast<const components::logical_plan::node_sort_t*>(child.get())->read_cap()));
                    break;
                }
                case node_type::select_t: {
                    select_op = create_plan_select(context, child);
                    break;
                }
                case node_type::having_t: {
                    // Spliced between the group and the sort; operator_limit alone provides the window.
                    VALUE_OR_RETURN(having_op, create_plan_having(context, child));
                    break;
                }
                default: {
                    VALUE_OR_RETURN(child_op,
                                    create_plan(context,
                                                function_registry,
                                                child,
                                                components::logical_plan::limit_t::all(),
                                                params));
                    break;
                }
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
                            return unresolved_table_refusal(context.resource, agg_node->target());
                        }
                        break;
                }
            }
            std::vector<size_t> projected_cols = relkind_projected_cols(context, node, agg_node->projected_cols());
            if (match_op) {
                executor = std::move(match_op);
            } else {
                VALUE_OR_RETURN(auto types, page_types_of(context, node->table_oid(), projected_cols));
                VALUE_OR_RETURN(auto address, storage_address_of(context, node->table_oid()));
                auto scan = boost::intrusive_ptr(
                    new components::operators::transfer_scan(plan_resource,
                                                             node->table_oid(),
                                                             std::move(address),
                                                             full_name_of(context, node->table_oid()),
                                                             agg_node->read_cap(),
                                                             std::move(projected_cols)));
                scan->set_page_types(std::move(types));
                executor = std::move(scan);
            }
        }
        if (group_op) {
            // Forwarded so zero input rows still build correctly-typed results (PostgreSQL TupleDesc model).
            const auto* typed = group_node->has_output_types() ? group_node : node.get();
            if (typed->has_output_types()) {
                group_op->set_output_types(typed->output_types());
            }
            group_op->set_children(std::move(executor));
            executor = std::move(group_op);
        }
        if (having_op) {
            having_op->set_children(std::move(executor));
            executor = std::move(having_op);
        }
        const bool distinct_on = agg_node->is_distinct() && !agg_node->distinct_on_keys().empty();
        const size_t output_width = select_op ? 0 : agg_node->output_width();
        if (sort_op) {
            if (output_width > 0 && !distinct_on) {
                static_cast<components::operators::operator_sort_t*>(sort_op.get())
                    ->set_expected_output_count(output_width);
            }
            sort_op->set_children(std::move(executor));
            executor = std::move(sort_op);
        }
        // DISTINCT ON dedups on the ON-key subset BELOW the projection, so ON columns that don't survive it
        // are still present; keep-first over sorted input gives "first row per ON key in ORDER BY order".
        if (distinct_on) {
            auto distinct_op = boost::intrusive_ptr(
                new components::operators::operator_distinct_t(context.resource, context.log.clone()));
            std::pmr::vector<size_t> on_cols(node->resource());
            on_cols.reserve(agg_node->distinct_on_keys().size());
            for (const auto& key : agg_node->distinct_on_keys()) {
                on_cols.push_back(key.path().front());
            }
            distinct_op->set_on_keys(std::move(on_cols));
            distinct_op->set_expected_output_count(output_width);
            distinct_op->set_cap(agg_node->distinct_on_read_cap());
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }
        if (select_op) {
            select_op->set_children(std::move(executor));
            executor = std::move(select_op);
        }

        if (agg_node->is_distinct() && agg_node->distinct_on_keys().empty()) {
            auto distinct_op = boost::intrusive_ptr(
                new components::operators::operator_distinct_t(context.resource, context.log.clone()));
            distinct_op->set_children(std::move(executor));
            executor = std::move(distinct_op);
        }

        return wrap_limit(std::move(executor));
    }

} // namespace services::planner::impl
