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
            return is_parameter(expr.params()[0]) &&
                   is_parameter(expr.params()[1]);
        }

        // Try to fold a scalar arithmetic expression with constant params.
        // On success, replaces the expression's params with a single parameter_id_t
        // that holds the computed result (reusing left_id slot).
        bool
        try_fold_scalar(std::pmr::memory_resource* resource, scalar_expression_t& expr, parameter_node_t* parameters) {
            arithmetic_op op;
            if (!to_arithmetic_op(expr.type(), op)) {
                return false;
            }
            if (!all_params_are_constants(expr)) {
                return false;
            }

            auto left_id = as_parameter(expr.params()[0]);
            auto right_id = as_parameter(expr.params()[1]);

            const auto* left_val = parameters->parameter(left_id);
            const auto* right_val = parameters->parameter(right_id);

            // Skip if either operand is unbound (nothing to compute with) or NULL.
            if (!left_val || !right_val || left_val->is_null() || right_val->is_null()) {
                return false;
            }

            // Boxed into one-element vectors so the fold runs through the SAME producer that
            // would execute the expression un-folded. logical_value_t::arithmetic is the cheaper
            // scalar path and no longer throws, but it is a DIFFERENT producer: its result type
            // is whatever C++ promotion yields, so folding `tinyint + tinyint` through it would
            // answer INTEGER where the plan (and the kernel) say TINYINT, and a folded constant
            // is the expression's type. Folding must not change the answer's type, so the kernel
            // stays until the two producers agree on the narrow-integer and BOOLEAN arms.
            vector_t left_vec(resource, *left_val, 1);
            vector_t right_vec(resource, *right_val, 1);

            auto result_vec = compute_binary_arithmetic(resource, op, left_vec, right_vec, 1);
            if (result_vec.has_error()) {
                // Not foldable (a non-numeric operand pair reached the kernel): leave the
                // expression for the executor to reject with a real error.
                return false;
            }
            auto result_val = result_vec.value().value(0);

            // The folded result gets its OWN slot. `$n` placeholders are de-duplicated
            // statement-wide (one slot per distinct `$n`, see transformer::add_param_value),
            // so the left operand's slot is generally shared with other expressions:
            // writing the result there rebound `$n` itself and every sibling predicate
            // reading it silently saw the folded value. A fresh id is safe — the plan
            // carries parameter_node_t by intrusive_ptr and this rule runs inside the
            // executor, after the mailbox crossing, so the runtime reads the same object.
            auto result_id = parameters->add_parameter(std::move(result_val));

            // Replace params: single param = the folded constant
            expr.params().clear();
            expr.append_param(result_id);
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
            if (!is_parameter(expr.left()) ||
                !is_parameter(expr.right())) {
                return;
            }

            auto left_id = as_parameter(expr.left());
            auto right_id = as_parameter(expr.right());

            const auto* left_val = parameters->parameter(left_id);
            const auto* right_val = parameters->parameter(right_id);
            // An unbound operand is not a constant, so this comparison is not foldable. Leave it
            // for the runtime, which reads the binding that exists by then.
            if (!left_val || !right_val) {
                return;
            }

            auto [ok, result] = eval_compare(expr.type(), *left_val, *right_val);
            if (ok) {
                expr.set_type(result ? compare_type::all_true : compare_type::all_false);
            } else {
                assert(false);
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
            if (!is_expr(slot)) {
                return;
            }
            auto& nested = as_expr(slot);
            if (!nested || nested->group() != expression_group::scalar) {
                return;
            }
            auto* ns = static_cast<scalar_expression_t*>(nested.get());
            if (ns->params().size() == 1 && is_parameter(ns->params()[0])) {
                auto id = as_parameter(ns->params()[0]);
                slot = id;
            }
        }

        void fold_expression(std::pmr::memory_resource* resource, expression_ptr& expr, parameter_node_t* parameters);

        void
        fold_scalar(std::pmr::memory_resource* resource, scalar_expression_t* scalar, parameter_node_t* parameters) {
            for (auto& param : scalar->params()) {
                if (!is_expr(param)) {
                    continue;
                }
                fold_expression(resource, as_expr(param), parameters);
                try_promote_scalar(param);
            }
            try_fold_scalar(resource, *scalar, parameters);
        }

        void
        fold_compare(std::pmr::memory_resource* resource, compare_expression_t* comp, parameter_node_t* parameters) {
            for (auto& child : comp->children()) {
                fold_expression(resource, child, parameters);
            }
            if (is_expr(comp->left())) {
                fold_expression(resource, as_expr(comp->left()), parameters);
                try_promote_scalar(comp->left());
            }
            if (is_expr(comp->right())) {
                fold_expression(resource, as_expr(comp->right()), parameters);
                try_promote_scalar(comp->right());
            }
            try_fold_compare(*comp, parameters);
            simplify_union(comp);
            // NOT over a fully folded single child folds to the complementary
            // constant: NOT(all_false) scans everything, NOT(all_true) is the
            // short-circuited empty scan. Only the single-child form folds —
            // multi-child union_not means NOT(child1 AND child2 ...) and keeps
            // its children. Without this, `WHERE NOT (1=2)` survived folding
            // into filter construction, whose all_false / key-shape guards
            // were Release-erased asserts.
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
