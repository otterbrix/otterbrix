#include "constant_folding.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/forward.hpp>
#include <core/arithmetic_op.hpp>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;
        using namespace components::types;
        using components::vector::arithmetic_op;

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

        // Try to fold a scalar arithmetic expression with constant params. On success, replaces the
        // expression's params with a single parameter_id_t that holds the computed result (reusing left_id).
        //
        // Channel: `true` = folded, `false` = the shape is not foldable (non-arithmetic op, non-constant
        // params, NULL operand), error = both sides ARE constant but the arithmetic refused them (unsupported
        // operand types). The refusal is the logical_value_t entry points' own, which is why the operands are
        // not boxed into 1-element vectors for compute_binary_arithmetic: that has no error channel and
        // silently answers a typed NA for e.g. 'a' + 1, so the fold would rewrite the expression into a
        // constant NULL instead of declining.
        core::result_wrapper_t<bool>
        try_fold_scalar(std::pmr::memory_resource* resource, scalar_expression_t& expr, parameter_node_t* parameters) {
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

            // Fold only operand shapes the scalar evaluator dispatches soundly: both numeric (it promotes
            // them to a common type first) or the same type (one switch arm reads both). A MIXED pair outside
            // that — 'a' + 1, DATE + INTERVAL — is declined: logical_value_t's arithmetic dispatches those on
            // the LEFT type alone and reads the right operand through the left arm's getter, which throws
            // value<T>() on the mismatch (a components/types defect this rule must not trip at plan time).
            // Declining loses nothing but the fold: the runtime evaluator answers the expression.
            const auto left_type = left_val.type().type();
            const auto right_type = right_val.type().type();
            if (left_type != right_type && !(is_numeric(left_type) && is_numeric(right_type))) {
                return false;
            }

            auto result = [&]() -> core::result_wrapper_t<expr_value_t> {
                switch (op) {
                    case arithmetic_op::add:
                        return expr_value_t::sum(left_val, right_val);
                    case arithmetic_op::subtract:
                        return expr_value_t::subtract(left_val, right_val);
                    case arithmetic_op::multiply:
                        return expr_value_t::mult(left_val, right_val);
                    case arithmetic_op::divide:
                        return expr_value_t::divide(left_val, right_val);
                    case arithmetic_op::mod:
                        return expr_value_t::modulus(left_val, right_val);
                }
                // Unreachable: to_arithmetic_op maps exactly these five. Kept as a
                // refusal (not a silent NA) so an enum extension cannot fold garbage.
                return core::error_t{core::error_code_t::arithmetics_failure,
                                     std::pmr::string{"constant folding: unmapped arithmetic op", resource}};
            }();
            if (result.has_error()) {
                return result.error();
            }

            // Overwrite left_id's value with the computed result (reuse existing ID
            // to avoid issues with new IDs not surviving actor message copy chain)
            parameters->set_parameter(left_id, std::move(result.value()));

            // Replace params: single param = left_id
            expr.params().clear();
            expr.append_param(left_id);
            return true;
        }

        // Evaluate a constant comparison. Returns {true, result} on success.
        std::pair<bool, bool>
        eval_compare(compare_type ct, const expr_value_t& left_val, const expr_value_t& right_val) {
            if (left_val.is_null() || right_val.is_null()) {
                return {true, false};
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
            if (expr.type() == compare_type::all_true || expr.type() == compare_type::all_false || expr.do_not_fold()) {
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
            // !ok: this comparison KIND has no fold (regex / ANY / ALL / IS [NOT]
            // NULL over constants). That is ordinary transformer output, not a
            // broken invariant — the runtime evaluator answers it; folding is only
            // an optimization, so this is a skip and not an assert.
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
            if (ns->params().size() == 1 && std::holds_alternative<core::parameter_id_t>(ns->params()[0])) {
                auto id = std::get<core::parameter_id_t>(ns->params()[0]);
                slot = id;
            }
        }

        void fold_expression(std::pmr::memory_resource* resource, expression_ptr& expr, parameter_node_t* parameters);

        void
        fold_scalar(std::pmr::memory_resource* resource, scalar_expression_t* scalar, parameter_node_t* parameters) {
            for (auto& param : scalar->params()) {
                if (!std::holds_alternative<expression_ptr>(param)) {
                    continue;
                }
                fold_expression(resource, std::get<expression_ptr>(param), parameters);
                try_promote_scalar(param);
            }
            auto folded = try_fold_scalar(resource, *scalar, parameters);
            if (folded.has_error()) {
                // Both sides are constants and the arithmetic refused them (unsupported operand types).
                // Declining the fold is the whole answer HERE: this rule is an optimization pass with no
                // path to the user (optimize() returns a plan, not a result), so the expression is left
                // standing and the runtime evaluator — the canonical evaluation point — answers it.
                return;
            }
        }

        void
        fold_compare(std::pmr::memory_resource* resource, compare_expression_t* comp, parameter_node_t* parameters) {
            for (auto& child : comp->children()) {
                fold_expression(resource, child, parameters);
            }
            if (std::holds_alternative<expression_ptr>(comp->left())) {
                fold_expression(resource, std::get<expression_ptr>(comp->left()), parameters);
                try_promote_scalar(comp->left());
            }
            if (std::holds_alternative<expression_ptr>(comp->right())) {
                fold_expression(resource, std::get<expression_ptr>(comp->right()), parameters);
                try_promote_scalar(comp->right());
            }
            try_fold_compare(*comp, parameters);
            simplify_union(comp);
            // NOT over a fully folded single child folds to the complementary constant: NOT(all_false)
            // scans everything, NOT(all_true) is the short-circuited empty scan. Only the single-child
            // form folds — multi-child union_not means NOT(child1 AND child2 ...) and keeps its children.
            // Without this, `WHERE NOT (1=2)` survived folding into filter construction, whose all_false /
            // key-shape guards were Release-erased asserts.
            if (comp->type() == compare_type::union_not && comp->children().size() == 1 &&
                comp->children().front()->group() == expression_group::compare) {
                const auto child_type =
                    static_cast<const compare_expression_t*>(comp->children().front().get())->type();
                if (child_type == compare_type::all_false) {
                    comp->set_type(compare_type::all_true);
                    comp->children().clear();
                } else if (child_type == compare_type::all_true) {
                    comp->set_type(compare_type::all_false);
                    comp->children().clear();
                }
            }
            if (comp->type() == compare_type::union_and || comp->type() == compare_type::union_or) {
                const auto neutral =
                    (comp->type() == compare_type::union_and) ? compare_type::all_true : compare_type::all_false;
                auto& ch = comp->children();
                ch.erase(std::remove_if(ch.begin(),
                                        ch.end(),
                                        [neutral](const expression_ptr& child) {
                                            if (child->group() != expression_group::compare) {
                                                return false;
                                            }
                                            return static_cast<const compare_expression_t*>(child.get())->type() ==
                                                   neutral;
                                        }),
                         ch.end());
            }
        }

        void fold_expression(std::pmr::memory_resource* resource, expression_ptr& expr, parameter_node_t* parameters) {
            if (!expr) {
                return;
            }
            if (expr->group() == expression_group::scalar) {
                fold_scalar(resource, static_cast<scalar_expression_t*>(expr.get()), parameters);
            } else if (expr->group() == expression_group::compare) {
                fold_compare(resource, static_cast<compare_expression_t*>(expr.get()), parameters);
                auto* comp = static_cast<compare_expression_t*>(expr.get());
                if ((comp->type() == compare_type::union_and || comp->type() == compare_type::union_or) &&
                    comp->children().size() == 1) {
                    expr = comp->children().front();
                }
            }
        }

    } // namespace

    void fold_constants(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr& node,
                        logical_plan::parameter_node_t* parameters) {
        if (!node) {
            return;
        }

        // BFS collect all nodes, then process in reverse (bottom-up)
        std::vector<logical_plan::node_ptr> stack{node};
        std::vector<logical_plan::node_ptr> order;
        while (!stack.empty()) {
            auto current = std::move(stack.back());
            stack.pop_back();
            for (const auto& child : current->children()) {
                stack.push_back(child);
            }
            order.push_back(std::move(current));
        }

        for (auto it = order.rbegin(); it != order.rend(); ++it) {
            if ((*it)->type() != logical_plan::node_type::match_t) {
                continue;
            }
            for (auto& expr : (*it)->expressions()) {
                fold_expression(resource, expr, parameters);
            }
        }
    }

} // namespace components::planner::optimizer
