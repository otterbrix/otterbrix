#include "create_plan_join.hpp"

#include <cassert>
#include <cstdlib>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/effective_table_oid.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_hash_join.hpp>
#include <components/physical_plan/operators/operator_join.hpp>
#include <components/physical_plan/operators/operator_lateral_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <utility>

namespace services::planner::impl {

    namespace {
        // has_join marks an unbounded join sub-tree (never tie-broken); has_filter is evidence for the tie-break below.
        struct subplan_shape_t {
            bool has_join{false};
            bool has_filter{false};
        };

        void probe_subplan_shape(const components::logical_plan::node_ptr& n, subplan_shape_t& out) {
            if (!n) {
                return;
            }
            using components::logical_plan::node_type;
            if (n->type() == node_type::join_t) {
                out.has_join = true;
            } else if (n->type() == node_type::match_t) {
                out.has_filter = true;
            }
            for (const auto& child : n->children()) {
                probe_subplan_shape(child, out);
            }
        }

        components::expressions::expression_ptr key_expression(std::pmr::memory_resource* resource,
                                                               const components::expressions::param_storage& operand) {
            using namespace components::expressions;
            if (is_key(operand)) {
                return make_scalar_expression(resource, scalar_type::get_field, as_key(operand));
            }
            return as_expr(operand);
        }

    } // namespace

    plan_result_t create_plan_join(const context_storage_t& context,
                                   const components::compute::function_registry_t& function_registry,
                                   const components::logical_plan::node_ptr& node,
                                   const components::logical_plan::storage_parameters* params) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        auto* resource = context.resource;
        auto log = context.log.clone();

        using join_type = components::logical_plan::join_type;
        using join_algo = components::logical_plan::node_join_t::join_algo;

        auto lower_input = [&](const components::logical_plan::node_ptr& input) -> plan_result_t {
            assert(input && "a join has both inputs");
            return create_plan(context, function_registry, input, components::logical_plan::limit_t::all(), params);
        };

        if (join_node->is_lateral()) {
            std::pmr::vector<components::logical_plan::node_join_t::correlation_t> correlations(resource);
            correlations.reserve(join_node->correlations().size());
            for (const auto& correlation : join_node->correlations()) {
                correlations.emplace_back(correlation.first,
                                          components::expressions::key_t{correlation.second, resource});
            }
            components::expressions::expression_ptr on_expression =
                node->expressions().empty() ? nullptr : node->expressions()[0];
            std::pmr::vector<components::types::complex_logical_type> outer_schema(node->resource());
            std::pmr::vector<components::types::complex_logical_type> inner_schema(node->resource());
            outer_schema.assign(node->children().front()->output_types().begin(),
                                node->children().front()->output_types().end());
            inner_schema.assign(node->children().back()->output_types().begin(),
                                node->children().back()->output_types().end());
            auto lateral =
                boost::intrusive_ptr(new components::operators::operator_lateral_join_t(resource,
                                                                                        log.clone(),
                                                                                        join_node->type(),
                                                                                        std::move(correlations),
                                                                                        std::move(on_expression),
                                                                                        std::move(outer_schema),
                                                                                        std::move(inner_schema)));
            VALUE_OR_RETURN(auto outer, lower_input(node->children().front()));
            VALUE_OR_RETURN(auto inner, lower_input(node->children().back()));
            lateral->set_lateral_terms(std::move(outer), std::move(inner));
            return lateral;
        }

        // rewrite_hash_joins already stamped algo()==hash for the detected equi-key (sole source of truth).
        if (join_node->algo() == join_algo::hash) {
            // Moves the smaller table onto the build side for INNER joins with known live counts on
            // both effective (filter-unwrapped) sides; outer joins never swap, and a wrong estimate
            // only ever picks a slower, still-correct plan.
            bool swap_build_side = false;
            if (join_node->type() == join_type::inner) {
                const auto left_eff = components::logical_plan::effective_table_oid(node->children().front());
                const auto right_eff = components::logical_plan::effective_table_oid(node->children().back());
                if (left_eff != components::catalog::INVALID_OID && right_eff != components::catalog::INVALID_OID &&
                    left_eff != right_eff) {
                    const auto left_it = context.row_counts.find(left_eff);
                    const auto right_it = context.row_counts.find(right_eff);
                    if (left_it != context.row_counts.end() && right_it != context.row_counts.end()) {
                        if (left_it->second < right_it->second) {
                            swap_build_side = true;
                        } else if (left_it->second == right_it->second) {
                            // A filtered side is provably <= an unfiltered other at an exact tie; break onto it.
                            subplan_shape_t left_shape;
                            subplan_shape_t right_shape;
                            probe_subplan_shape(node->children().front(), left_shape);
                            probe_subplan_shape(node->children().back(), right_shape);
                            if (!left_shape.has_join && !right_shape.has_join && left_shape.has_filter &&
                                !right_shape.has_filter) {
                                swap_build_side = true;
                            }
                        }
                    }
                }
            }

            // rewrite_hash_joins put the left side's operand first; after a swap the probe is the
            // logical-right table, so the right operand is the probe key.
            const auto* on =
                static_cast<const components::expressions::compare_expression_t*>(node->expressions().front().get());
            std::pmr::vector<components::expressions::expression_ptr> probe_keys(resource);
            std::pmr::vector<components::expressions::expression_ptr> build_keys(resource);
            probe_keys.push_back(key_expression(resource, swap_build_side ? on->right() : on->left()));
            build_keys.push_back(key_expression(resource, swap_build_side ? on->left() : on->right()));
            components::operators::operator_ptr hash_join =
                boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                     log.clone(),
                                                                                     join_node->type(),
                                                                                     std::move(probe_keys),
                                                                                     std::move(build_keys),
                                                                                     swap_build_side,
                                                                                     join_node->single_match()));
            // Cross never carries an equi-key.
            switch (join_node->type()) {
                case join_type::left:
                case join_type::right:
                case join_type::inner:
                case join_type::full:
                case join_type::semi:
                case join_type::anti:
                    break;
                case join_type::cross:
                case join_type::invalid:
                    assert(false && "a hash join is never cross or invalid");
                    std::abort();
            }
            const auto& probe_child = swap_build_side ? node->children().back() : node->children().front();
            const auto& build_child = swap_build_side ? node->children().front() : node->children().back();
            VALUE_OR_RETURN(auto hash_left, lower_input(probe_child));
            VALUE_OR_RETURN(auto hash_right, lower_input(build_child));
            hash_join->set_children(std::move(hash_left), std::move(hash_right));
            return hash_join;
        }

        const auto& expression = node->expressions()[0];

        // Equi-join selection happens in rewrite_hash_joins; anything left as plain join_t lands here.
        components::operators::operator_ptr join =
            boost::intrusive_ptr(new components::operators::operator_join_t(resource,
                                                                            std::move(log),
                                                                            join_node->type(),
                                                                            expression,
                                                                            join_node->single_match()));

        switch (join_node->type()) {
            case join_type::left:
            case join_type::right:
            case join_type::cross:
            case join_type::inner:
            case join_type::full:
            case join_type::semi:
            case join_type::anti:
                break;
            case join_type::invalid:
                assert(false && "an invalid join never reaches plan generation");
                std::abort();
        }
        VALUE_OR_RETURN(auto left, lower_input(node->children().front()));
        VALUE_OR_RETURN(auto right, lower_input(node->children().back()));
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl
