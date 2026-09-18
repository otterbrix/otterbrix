#include "create_plan_join.hpp"

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

    } // namespace

    components::operators::operator_ptr
    create_plan_join(const context_storage_t& context,
                     const components::compute::function_registry_t& function_registry,
                     const components::logical_plan::node_ptr& node,
                     const components::logical_plan::storage_parameters* params) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        // Try left child context first, fall back to right (one side may be raw data with nullptr context)
        auto left_oid = node->children().front()->table_oid();
        auto right_oid = node->children().back()->table_oid();
        bool known = context.has_table_oid(left_oid) || context.has_table_oid(right_oid);
        // Neither side may be a known table; fall back to the logical node's own resource so the
        // operator still has a valid allocator for its working state (mirrors create_plan_aggregate).
        auto* resource = known ? context.resource : node->resource();
        auto log = known ? context.log.clone() : log_t{};

        using join_type = components::logical_plan::join_type;
        using join_algo = components::logical_plan::node_join_t::join_algo;

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
            components::operators::operator_ptr outer;
            components::operators::operator_ptr inner;
            if (node->children().front()) {
                outer = create_plan(context,
                                    function_registry,
                                    node->children().front(),
                                    components::logical_plan::limit_t::unlimit(),
                                    params);
            }
            if (node->children().back()) {
                inner = create_plan(context,
                                    function_registry,
                                    node->children().back(),
                                    components::logical_plan::limit_t::unlimit(),
                                    params);
            }
            // A null child means a term failed to lower; return nullptr rather than execute on it.
            if (!outer || !inner) {
                return nullptr;
            }
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

            // After a swap the probe is the logical-right table and the build is the logical-left
            // table, so the ctor key columns swap too (see operator_hash_join_t's ctor contract).
            const std::size_t probe_key_col = swap_build_side ? join_node->right_col() : join_node->left_col();
            const std::size_t build_key_col = swap_build_side ? join_node->left_col() : join_node->right_col();
            components::operators::operator_ptr hash_join =
                boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                     log.clone(),
                                                                                     join_node->type(),
                                                                                     probe_key_col,
                                                                                     build_key_col,
                                                                                     swap_build_side));
            // The hash path covers inner/left/right/full only; cross never carries an equi-key.
            switch (join_node->type()) {
                case join_type::left:
                case join_type::right:
                case join_type::inner:
                case join_type::full:
                    break;
                case join_type::cross:
                case join_type::invalid:
                case join_type::semi:
                case join_type::anti:
                    return nullptr;
            }
            const auto& probe_child = swap_build_side ? node->children().back() : node->children().front();
            const auto& build_child = swap_build_side ? node->children().front() : node->children().back();
            components::operators::operator_ptr hash_left;
            components::operators::operator_ptr hash_right;
            if (probe_child) {
                hash_left = create_plan(context,
                                        function_registry,
                                        probe_child,
                                        components::logical_plan::limit_t::unlimit(),
                                        params);
            }
            if (build_child) {
                hash_right = create_plan(context,
                                         function_registry,
                                         build_child,
                                         components::logical_plan::limit_t::unlimit(),
                                         params);
            }
            if (!hash_left || !hash_right) {
                return nullptr;
            }
            hash_join->set_children(std::move(hash_left), std::move(hash_right));
            return hash_join;
        }

        const auto& expression = node->expressions()[0];

        // Equi-join selection happens in rewrite_hash_joins; anything left as plain join_t lands here.
        components::operators::operator_ptr join = boost::intrusive_ptr(
            new components::operators::operator_join_t(resource, std::move(log), join_node->type(), expression));

        switch (join_node->type()) {
            case join_type::left:
            case join_type::right:
            case join_type::cross:
            case join_type::inner:
            case join_type::full:
                break;
            case join_type::invalid:
            case join_type::semi:
            case join_type::anti:
                // invalid never fires (validation); semi/anti appear only as LATERAL joins (handled above).
                return nullptr;
        }
        components::operators::operator_ptr left;
        components::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context,
                               function_registry,
                               node->children().front(),
                               components::logical_plan::limit_t::unlimit(),
                               params);
        }
        if (node->children().back()) {
            right = create_plan(context,
                                function_registry,
                                node->children().back(),
                                components::logical_plan::limit_t::unlimit(),
                                params);
        }
        if (!left || !right) {
            return nullptr;
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl
