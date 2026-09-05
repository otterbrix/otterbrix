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
        // Structural probe of a hash-join input sub-plan for the exact-count
        // TIE-break below. `has_join` marks a multi-relation intermediate (a join
        // result — its output size is not bounded by either input count, so it is
        // never tie-broken); `has_filter` marks a pushed-down single-relation WHERE
        // filter (a `match` below the join) — at an exact pre-filter count tie the
        // evidence that this side's actual size is <= the other's. Read-only, purely
        // from the logical plan shape — no catalog / row-count access.
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
        // assign left table as actor for join
        // Try left child context first, fall back to right (one side may be raw data with nullptr context)
        auto left_oid = node->children().front()->table_oid();
        auto right_oid = node->children().back()->table_oid();
        bool known = context.has_table_oid(left_oid) || context.has_table_oid(right_oid);
        // When neither side is a known table (both raw data), there is no table-actor
        // context, but the operator still needs a VALID resource to allocate its working
        // state (the typed hash index, the key-column lists built in the ctor, the
        // output). Fall back to the logical node's own resource -- mirrors
        // create_plan_aggregate. The pre-STEP-3 join stored plain key indices and never
        // allocated here, so a nullptr was tolerated; the typed hash+verify hash-join
        // allocates in its constructor, so nullptr now segfaults.
        auto* resource = known ? context.resource : node->resource();
        auto log = known ? context.log.clone() : log_t{};

        using join_type = components::logical_plan::join_type;
        using join_algo = components::logical_plan::node_join_t::join_algo;

        if (join_node->is_lateral()) {
            std::pmr::vector<components::logical_plan::node_join_t::correlation_t> correlations(node->resource());
            for (const auto& correlation : join_node->correlations()) {
                correlations.emplace_back(correlation);
            }
            // Filters each inner row against the outer row inside the operator.
            components::expressions::expression_ptr on_expression =
                node->expressions().empty() ? nullptr : node->expressions()[0];
            // Output types already were determined by the validator
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
            // A null child means a term failed to lower (e.g. a host-extension node
            // with no injected create_plan rule). Surface it as an invalid plan
            // rather than driving a null child at execution (segfault).
            if (!outer || !inner) {
                return nullptr;
            }
            lateral->set_lateral_terms(std::move(outer), std::move(inner));
            return lateral;
        }

        // Equi-join fast path: the optimizer rule rewrite_hash_joins detected a single
        // eq(left.key, right.key) ON condition and stamped algo()==hash plus the matched
        // equi-key column indices. Lower straight to operator_hash_join_t (O(L+R)). No
        // detection here — the annotation is the single source of truth.
        if (join_node->algo() == join_algo::hash) {
            // Build-side selection: operator_hash_join_t materializes its
            // physical RIGHT child as the hash build side, so the default build is the
            // LOGICAL-right table. Move the SMALLER table onto the build side IFF this
            // is an INNER join, both children are (possibly filter-wrapped) distinct
            // base tables whose live row counts are known (fetched into
            // context.row_counts by execute_plan_full), and the current build (right)
            // is the LARGER side. Each side is resolved to its EFFECTIVE base relation
            // (effective_table_oid descends through pushdown_filter's oid-less match
            // wrapper), so a filtered base table still exposes the table the count was
            // fetched for. When swapping, the smaller logical-left child moves into the
            // physical build slot and swapped_=true tells compute_join_layout to
            // re-assemble the output in logical [left, right] order, so results are
            // byte-for-byte identical. Outer joins are never swapped (their NULL-pad
            // side is orientation-fixed); a self-join (same effective oid), a missing
            // count, or an INVALID_OID side keeps the default child order. A wrong
            // estimate only picks a slower-but-correct plan.
            //
            // Counts decide only when BOTH effective counts exist; with incomplete
            // evidence the default order stands. No shape-only heuristics: a
            // "filtered side is smaller" guess can swap a weakly-filtered huge left
            // onto the build (memory blow-up). The fetch side
            // (collect_inner_hash_join_oids in services/collection/executor.cpp)
            // resolves each join input through the same effective-oid descent, so
            // every side with a backing relation gets a live count.
            bool swap_build_side = false;
            if (join_node->type() == join_type::inner) {
                const auto left_eff = components::logical_plan::effective_table_oid(node->children().front());
                const auto right_eff = components::logical_plan::effective_table_oid(node->children().back());
                if (left_eff != components::catalog::INVALID_OID && right_eff != components::catalog::INVALID_OID &&
                    left_eff != right_eff) {
                    const auto left_it = context.row_counts.find(left_eff);
                    const auto right_it = context.row_counts.find(right_eff);
                    if (left_it != context.row_counts.end() && right_it != context.row_counts.end()) {
                        // Both live counts known -> the statistics decide.
                        if (left_it->second < right_it->second) {
                            swap_build_side = true;
                        } else if (left_it->second == right_it->second) {
                            // Exact PRE-filter count tie: a pushed-down local filter only
                            // removes rows, so a filtered-left / unfiltered-right pair makes
                            // the left certainly <= the right — evidence-backed tie-break
                            // onto the build. Join sub-trees are never tie-broken (their
                            // output size is not bounded by either input count).
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

            // After a swap the probe (physical left_) is the logical-right table and
            // the build (physical right_) is the logical-left table, so the ctor key
            // columns are swapped too (left_col == probe key col, right_col == build
            // key col — see operator_hash_join_t's ctor contract).
            const std::size_t probe_key_col = swap_build_side ? join_node->right_col() : join_node->left_col();
            const std::size_t build_key_col = swap_build_side ? join_node->left_col() : join_node->right_col();
            components::operators::operator_ptr hash_join =
                boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                     log.clone(),
                                                                                     join_node->type(),
                                                                                     probe_key_col,
                                                                                     build_key_col,
                                                                                     swap_build_side));
            // The hash path covers inner/left/right/full only (cross never carries an
            // equi-key). Defensive guard against a cross/invalid slipping through.
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
                    // Defensive guard: hash is never stamped on cross/invalid, and semi/anti
                    // are only ever produced as LATERAL joins (handled above). Return nullptr
                    // -> executor surfaces the error (no throw here).
                    return nullptr;
            }
            // Physical roles: probe = left_, build = right_. When swapped the
            // logical-right child becomes the probe and the logical-left child the build.
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
            // A null child means a side failed to lower (e.g. a host-extension node
            // with no injected create_plan rule) → invalid plan, not a null-child
            // deref at execution.
            if (!hash_left || !hash_right) {
                return nullptr;
            }
            hash_join->set_children(std::move(hash_left), std::move(hash_right));
            return hash_join;
        }

        const auto& expression = node->expressions()[0];

        // Nested-loop join. Equi-join selection (the eq(left.key, right.key) fast
        // path) happens in the optimizer (rewrite_hash_joins), which stamps the hash
        // annotation handled above; anything left as a plain join_t lands here.
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
                // Defensive guard: validation guarantees invalid never fires, and semi/anti
                // are only ever produced as LATERAL joins (handled above). Return nullptr ->
                // executor surfaces the error (no throw on the operator-build path).
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
        // A null child means a side failed to lower (e.g. a host-extension node with
        // no injected create_plan rule) → invalid plan, not a null-child deref.
        if (!left || !right) {
            return nullptr;
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl
