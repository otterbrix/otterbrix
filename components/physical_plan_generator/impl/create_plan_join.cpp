#include "create_plan_join.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_hash_join.hpp>
#include <components/physical_plan/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <optional>
#include <utility>
#include <variant>

namespace services::planner::impl {

    namespace {
        // Detect a single equi-comparison `eq(left.key, right.key)` and return the
        // (left_col, right_col) column indices into each side's input chunk. The
        // validator stamps key.side()/key.path() during JOIN validation, so we rely
        // on those. Returns nullopt for anything else — non-eq comparisons, AND/union
        // conditions (group() == compare but type() != eq), const operands, or two
        // keys on the same side — and the caller keeps the nested-loop join.
        std::optional<std::pair<size_t, size_t>>
        detect_equi_columns(const components::expressions::expression_ptr& expr) {
            namespace ce = components::expressions;
            if (!expr || expr->group() != ce::expression_group::compare) {
                return std::nullopt;
            }
            const auto* cmp = static_cast<const ce::compare_expression_t*>(expr.get());
            if (cmp->type() != ce::compare_type::eq) {
                return std::nullopt;
            }
            if (!std::holds_alternative<ce::key_t>(cmp->left()) ||
                !std::holds_alternative<ce::key_t>(cmp->right())) {
                return std::nullopt;
            }
            const auto& lk = std::get<ce::key_t>(cmp->left());
            const auto& rk = std::get<ce::key_t>(cmp->right());
            // Only a single top-level column maps to a hash-table probe. A multi-element
            // path is a nested-struct/UDT field access (e.g. `(custom_type).f1`); path()[0]
            // would address the whole struct column, not the scalar being compared, so we
            // leave those to the nested-loop join, which evaluates the full path correctly.
            if (lk.path().size() != 1 || rk.path().size() != 1) {
                return std::nullopt;
            }
            if (lk.side() == ce::side_t::left && rk.side() == ce::side_t::right) {
                return std::make_pair(lk.path()[0], rk.path()[0]);
            }
            if (lk.side() == ce::side_t::right && rk.side() == ce::side_t::left) {
                return std::make_pair(rk.path()[0], lk.path()[0]);
            }
            return std::nullopt;
        }

        // CROSS has no equi-condition; INVALID is rejected below. The hash path
        // implements inner / left / right / full only.
        bool is_equi_joinable(components::logical_plan::join_type t) {
            using jt = components::logical_plan::join_type;
            return t == jt::inner || t == jt::left || t == jt::right || t == jt::full;
        }
    } // namespace

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

        const auto& expression = node->expressions()[0];

        // Optimizer rewrite: a single eq(left.key, right.key) condition lets us
        // replace the O(L·R) nested-loop join with an O(L+R) hash join. Any other
        // condition keeps operator_join_t.
        std::optional<std::pair<size_t, size_t>> equi;
        if (is_equi_joinable(join_node->type())) {
            equi = detect_equi_columns(expression);
        }

        components::operators::operator_ptr join;
        if (equi) {
            join = boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                        std::move(log),
                                                                                        join_node->type(),
                                                                                        equi->first,
                                                                                        equi->second));
        } else {
            join = boost::intrusive_ptr(
                new components::operators::operator_join_t(resource, std::move(log), join_node->type(), expression));
        }

        using join_type = components::logical_plan::join_type;
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
                throw std::logic_error("create_plan_join: INVALID join type");
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
