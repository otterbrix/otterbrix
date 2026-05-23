#include "create_plan_join.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_index_join.hpp>
#include <components/physical_plan/operators/operator_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <optional>

namespace services::planner::impl {
    namespace {
        bool is_simple_inner_eq_join(const components::expressions::expression_ptr& expr) {
            using namespace components::expressions;
            if (!expr || expr->group() != expression_group::compare) {
                return false;
            }
            auto comp = static_cast<const compare_expression_t*>(expr.get());
            if (comp->type() != compare_type::eq || !comp->children().empty()) {
                return false;
            }
            return std::holds_alternative<components::expressions::key_t>(comp->left()) &&
                   std::holds_alternative<components::expressions::key_t>(comp->right());
        }

        std::optional<components::expressions::key_t>
        extract_probe_key_for_side(const components::expressions::expression_ptr& expr, components::expressions::side_t side) {
            using namespace components::expressions;
            if (!is_simple_inner_eq_join(expr)) {
                return std::nullopt;
            }
            auto comp = static_cast<const compare_expression_t*>(expr.get());
            const auto& lhs = std::get<components::expressions::key_t>(comp->left());
            const auto& rhs = std::get<components::expressions::key_t>(comp->right());
            if (lhs.side() == side) {
                return lhs;
            }
            if (rhs.side() == side) {
                return rhs;
            }
            return std::nullopt;
        }

        bool has_index_on_key(const context_storage_t& context,
                                                components::catalog::oid_t table_oid,
                                                const components::expressions::expression_ptr& expr,
                                                components::expressions::side_t side) {
            auto probe_key = extract_probe_key_for_side(expr, side);
            if (!probe_key.has_value()) {
                return false;
            }
            return context.has_index_on(table_oid, *probe_key);
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
        const bool right_candidate =
            join_node->type() == components::logical_plan::join_type::inner &&
            !node->expressions().empty() &&
            is_simple_inner_eq_join(node->expressions()[0]) &&
            has_index_on_key(context, right_oid, node->expressions()[0], components::expressions::side_t::right) &&
            right_oid != components::catalog::INVALID_OID;
        const bool left_candidate =
            join_node->type() == components::logical_plan::join_type::inner &&
            !node->expressions().empty() &&
            is_simple_inner_eq_join(node->expressions()[0]) &&
            has_index_on_key(context, left_oid, node->expressions()[0], components::expressions::side_t::left) &&
            left_oid != components::catalog::INVALID_OID;
        const bool index_join_candidate = right_candidate || left_candidate;
        const auto probe_oid = right_candidate ? right_oid : left_oid;
        const auto probe_side = right_candidate ? components::operators::operator_index_join_t::probe_side_t::right
                                                : components::operators::operator_index_join_t::probe_side_t::left;
        auto* op_resource = known ? context.resource : nullptr;
        auto op_log = known ? context.log.clone() : log_t{};
        components::operators::operator_ptr join =
            index_join_candidate
                ? components::operators::operator_ptr(new components::operators::operator_index_join_t(op_resource,
                                                                                                        std::move(op_log),
                                                                                                        join_node->type(),
                                                                                                        node->expressions()[0],
                                                                                                        probe_oid,
                                                                                                        probe_side))
                : components::operators::operator_ptr(new components::operators::operator_join_t(op_resource,
                                                                                                  std::move(op_log),
                                                                                                  join_node->type(),
                                                                                                  node->expressions()[0]));
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
