#include "constant_folding.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/vector/arithmetic.hpp>
#include <components/vector/vector.hpp>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;
        using namespace components::vector;
        using namespace components::types;

        // Map scalar_type to arithmetic_op. Returns false if not an arithmetic op.
        bool to_arithmetic_op(scalar_type st, arithmetic_op& out) {
            switch (st) {
                case scalar_type::add:
                    out = arithmetic_op::add;
                    return true;
                case scalar_type::subtract:
                    out = arithmetic_op::subtract;
                    return true;
                case scalar_type::multiply:
                    out = arithmetic_op::multiply;
                    return true;
                case scalar_type::divide:
                    out = arithmetic_op::divide;
                    return true;
                case scalar_type::mod:
                    out = arithmetic_op::mod;
                    return true;
                default:
                    return false;
            }
        }

        // Check if all params of a scalar expression are parameter_id_t
        bool all_params_are_constants(const scalar_expression_t& expr) {
            if (expr.params().size() != 2) {
                return false;
            }
            return std::holds_alternative<core::parameter_id_t>(expr.params()[0]) &&
                   std::holds_alternative<core::parameter_id_t>(expr.params()[1]);
        }

        // Try to fold a scalar arithmetic expression with constant params.
        // On success, replaces the expression's params with a single parameter_id_t
        // that holds the computed result (reusing left_id slot).
        bool try_fold_scalar(std::pmr::memory_resource* resource,
                             scalar_expression_t& expr,
                             parameter_node_t* parameters) {
            arithmetic_op op;
            if (!to_arithmetic_op(expr.type(), op)) {
                return false;
            }
            if (!all_params_are_constants(expr)) {
                return false;
            }

            auto left_id = std::get<core::parameter_id_t>(expr.params()[0]);
            auto right_id = std::get<core::parameter_id_t>(expr.params()[1]);

            const auto& left_val = parameters->parameter(left_id);
            const auto& right_val = parameters->parameter(right_id);

            // Skip if either is NULL
            if (left_val.is_null() || right_val.is_null()) {
                return false;
            }

            // Create single-element vectors from the values
            vector_t left_vec(resource, left_val, 1);
            vector_t right_vec(resource, right_val, 1);

            auto result_vec = compute_binary_arithmetic(resource, op, left_vec, right_vec, 1);
            auto result_val = result_vec.value(0);

            // Overwrite left_id's value with the computed result (reuse existing ID
            // to avoid issues with new IDs not surviving actor message copy chain)
            parameters->set_parameter(left_id, std::move(result_val));

            // Replace params: single param = left_id
            expr.params().clear();
            expr.append_param(left_id);
            return true;
        }

        // Evaluate a constant comparison. Returns {true, result} on success.
        std::pair<bool, bool> eval_compare(compare_type ct,
                                           const expr_value_t& left_val,
                                           const expr_value_t& right_val) {
            if (left_val.is_null() || right_val.is_null()) {
                return {false, false};
            }

            auto cmp = left_val.compare(right_val);
            switch (ct) {
                case compare_type::eq:
                    return {true, cmp == compare_t::equals};
                case compare_type::ne:
                    return {true, cmp != compare_t::equals};
                case compare_type::gt:
                    return {true, cmp == compare_t::more};
                case compare_type::lt:
                    return {true, cmp == compare_t::less};
                case compare_type::gte:
                    return {true, cmp == compare_t::more || cmp == compare_t::equals};
                case compare_type::lte:
                    return {true, cmp == compare_t::less || cmp == compare_t::equals};
                default:
                    return {false, false};
            }
        }

        // Try to fold a compare expression where both sides are constant parameters
        void try_fold_compare(compare_expression_t& expr, parameter_node_t* parameters) {
            // Only fold leaf comparisons (not union_and/or/not)
            if (is_union_compare_condition(expr.type())) {
                return;
            }
            if (expr.type() == compare_type::all_true || expr.type() == compare_type::all_false) {
                return;
            }

            // Both sides must be parameter_id_t
            if (!std::holds_alternative<core::parameter_id_t>(expr.left()) ||
                !std::holds_alternative<core::parameter_id_t>(expr.right())) {
                return;
            }

            auto left_id = std::get<core::parameter_id_t>(expr.left());
            auto right_id = std::get<core::parameter_id_t>(expr.right());

            const auto& left_val = parameters->parameter(left_id);
            const auto& right_val = parameters->parameter(right_id);

            auto [ok, result] = eval_compare(expr.type(), left_val, right_val);
            if (ok) {
                expr.set_type(result ? compare_type::all_true : compare_type::all_false);
            }
        }

        // Check if a union expression's children are all folded to a specific type
        void simplify_union(compare_expression_t* comp) {
            if (comp->type() != compare_type::union_and && comp->type() != compare_type::union_or) {
                return;
            }
            if (comp->children().empty()) {
                return;
            }

            bool is_and = (comp->type() == compare_type::union_and);
            // AND: any_false → all_false, all_true → all_true
            // OR:  any_true → all_true, all_false → all_false
            auto dominating = is_and ? compare_type::all_false : compare_type::all_true;
            auto neutral = is_and ? compare_type::all_true : compare_type::all_false;

            bool all_neutral = true;
            for (const auto& child : comp->children()) {
                if (child->group() != expression_group::compare) {
                    all_neutral = false;
                    continue;
                }
                auto ct = static_cast<const compare_expression_t*>(child.get())->type();
                if (ct == dominating) {
                    comp->set_type(dominating);
                    return;
                }
                if (ct != neutral) {
                    all_neutral = false;
                }
            }
            if (all_neutral) {
                comp->set_type(neutral);
            }
        }

        // Promote a folded scalar expression_ptr to parameter_id_t.
        // IMPORTANT: extract the id by value BEFORE assigning to slot,
        // because the assignment destroys the expression_ptr which may
        // free the scalar expression (use-after-free if we hold a reference).
        void try_promote_scalar(param_storage& slot) {
            if (!std::holds_alternative<expression_ptr>(slot)) {
                return;
            }
            auto& nested = std::get<expression_ptr>(slot);
            if (!nested || nested->group() != expression_group::scalar) {
                return;
            }
            auto* ns = static_cast<scalar_expression_t*>(nested.get());
            if (ns->params().size() == 1 &&
                std::holds_alternative<core::parameter_id_t>(ns->params()[0])) {
                auto id = std::get<core::parameter_id_t>(ns->params()[0]);
                slot = id;
            }
        }

        // Recursively fold expressions in a single expression tree
        void fold_expression(std::pmr::memory_resource* resource,
                             const expression_ptr& expr,
                             parameter_node_t* parameters) {
            if (!expr) {
                return;
            }

            if (expr->group() == expression_group::scalar) {
                auto* scalar = static_cast<scalar_expression_t*>(expr.get());

                // First recurse into nested expression params
                for (auto& param : scalar->params()) {
                    if (std::holds_alternative<expression_ptr>(param)) {
                        fold_expression(resource, std::get<expression_ptr>(param), parameters);
                        try_promote_scalar(param);
                    }
                }

                // Then try to fold this expression itself
                try_fold_scalar(resource, *scalar, parameters);

            } else if (expr->group() == expression_group::compare) {
                auto* comp = static_cast<compare_expression_t*>(expr.get());

                // Recurse into children (union_and/or/not have children)
                for (const auto& child : comp->children()) {
                    fold_expression(resource, child, parameters);
                }

                // Recurse into left/right if they are expressions
                if (std::holds_alternative<expression_ptr>(comp->left())) {
                    fold_expression(resource, std::get<expression_ptr>(comp->left()), parameters);
                    try_promote_scalar(comp->left());
                }
                if (std::holds_alternative<expression_ptr>(comp->right())) {
                    fold_expression(resource, std::get<expression_ptr>(comp->right()), parameters);
                    try_promote_scalar(comp->right());
                }

                // Try to fold this compare
                try_fold_compare(*comp, parameters);

                // Simplify union expressions after children are folded
                simplify_union(comp);
            }
        }

    } // namespace

    void fold_constants_recursive(std::pmr::memory_resource* resource,
                                  const logical_plan::node_ptr& node,
                                  logical_plan::parameter_node_t* parameters) {
        if (!node) {
            return;
        }

        // Recurse into children first (bottom-up)
        for (const auto& child : node->children()) {
            fold_constants_recursive(resource, child, parameters);
        }

        // Only fold expressions in match nodes (WHERE clauses).
        // Group/sort/aggregate expressions are used for computation and
        // must keep their structure for the physical plan evaluator.
        if (node->type() == logical_plan::node_type::match_t) {
            for (const auto& expr : node->expressions()) {
                fold_expression(resource, expr, parameters);
            }
        }
    }

} // namespace components::planner::optimizer
