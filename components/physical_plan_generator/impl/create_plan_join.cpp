#include "create_plan_join.hpp"

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_hash_join.hpp>
#include <components/physical_plan/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <utility>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_join(const context_storage_t& context,
                     const components::compute::function_registry_t& function_registry,
                     const components::logical_plan::node_ptr& node,
                     components::logical_plan::limit_t limit,
                     const components::logical_plan::storage_parameters* params) {
        const auto* join_node = static_cast<const components::logical_plan::node_join_t*>(node.get());
        // assign left table as actor for join
        // Try left child context first, fall back to right (one side may be raw data with nullptr context)
        auto left_oid = node->children().front()->table_oid();
        auto right_oid = node->children().back()->table_oid();
        bool known = context.has_table_oid(left_oid) || context.has_table_oid(right_oid);
        auto* resource = known ? context.resource : nullptr;
        auto log = known ? context.log.clone() : log_t{};

        using join_type = components::logical_plan::join_type;
        using join_algo = components::logical_plan::node_join_t::join_algo;

        // Equi-join fast path: the optimizer rule rewrite_hash_joins detected a single
        // eq(left.key, right.key) ON condition and stamped algo()==hash plus the matched
        // equi-key column indices. Lower straight to operator_hash_join_t (O(L+R)). No
        // detection here — the annotation is the single source of truth.
        if (join_node->algo() == join_algo::hash) {
            components::operators::operator_ptr hash_join =
                boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                    log.clone(),
                                                                                    join_node->type(),
                                                                                    join_node->left_col(),
                                                                                    join_node->right_col()));
            // Push the LIMIT down to whichever side an outer join preserves. The hash
            // path covers inner/left/right/full only (cross never carries an equi-key).
            auto hash_limit_left = components::logical_plan::limit_t::unlimit();
            auto hash_limit_right = components::logical_plan::limit_t::unlimit();
            switch (join_node->type()) {
                case join_type::left:
                    hash_limit_left = limit;
                    break;
                case join_type::right:
                    hash_limit_right = limit;
                    break;
                case join_type::inner:
                case join_type::full:
                    break;
                case join_type::cross:
                case join_type::invalid:
                    // Defensive guard (the optimizer never stamps hash on these): return
                    // nullptr -> executor surfaces the error (rule 9: no throw here).
                    return nullptr;
            }
            components::operators::operator_ptr hash_left;
            components::operators::operator_ptr hash_right;
            if (node->children().front()) {
                hash_left = create_plan(context, function_registry, node->children().front(), hash_limit_left, params);
            }
            if (node->children().back()) {
                hash_right = create_plan(context, function_registry, node->children().back(), hash_limit_right, params);
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

        auto limit_left = components::logical_plan::limit_t::unlimit();
        auto limit_right = components::logical_plan::limit_t::unlimit();
        switch (join_node->type()) {
            case join_type::left:
                limit_left = limit;
                break;
            case join_type::right:
                limit_right = limit;
                break;
            case join_type::cross:
                limit_left = limit;
                limit_right = limit;
                break;
            case join_type::inner:
            case join_type::full:
                break;
            case join_type::invalid:
                // Defensive guard (validation guarantees this never fires): return nullptr ->
                // executor surfaces the error (rule 9: no throw on the operator-build path).
                return nullptr;
        }
        components::operators::operator_ptr left;
        components::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, function_registry, node->children().front(), limit_left, params);
        }
        if (node->children().back()) {
            right = create_plan(context, function_registry, node->children().back(), limit_right, params);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl
