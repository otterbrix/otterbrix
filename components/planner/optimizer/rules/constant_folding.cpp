#include "constant_folding.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/types/tri_bool.hpp>
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

        using deferred_parameters_t = std::pmr::set<core::parameter_id_t>;

        // A sub-query's result slot holds an NA placeholder until that sub-query runs. Nothing that
        // reads it is foldable: its value is not the NULL it currently looks like.
        bool is_deferred(const param_storage& slot, const deferred_parameters_t* deferred) {
            if (deferred == nullptr || !std::holds_alternative<core::parameter_id_t>(slot)) {
                return false;
            }
            return deferred->count(std::get<core::parameter_id_t>(slot)) != 0;
        }

        // Check if all params of a scalar expression are parameter_id_t
        bool all_params_are_constants(const scalar_expression_t& expr, const deferred_parameters_t* deferred) {
            if (expr.params().size() != 2) {
                return false;
            }
            if (is_deferred(expr.params()[0], deferred) || is_deferred(expr.params()[1], deferred)) {
                return false;
            }
            return std::holds_alternative<core::parameter_id_t>(expr.params()[0]) &&
                   std::holds_alternative<core::parameter_id_t>(expr.params()[1]);
        }

        // Try to fold a scalar arithmetic expression with constant params. On success, replaces the
        // expression's params with a single parameter_id_t that holds the computed result (reusing left_id).
        //
        // Channel: `true` = folded, `false` = not foldable (non-arithmetic op, non-constant params, NULL
        // operand), error = both sides constant but the arithmetic refused them (unsupported operand types).
        core::result_wrapper_t<bool> try_fold_scalar(std::pmr::memory_resource* resource,
                                                     scalar_expression_t& expr,
                                                     parameter_node_t* parameters,
                                                     const deferred_parameters_t* deferred) {
            arithmetic_op op;
            if (!to_arithmetic_op(expr.type(), op)) {
                return false;
            }
            if (!all_params_are_constants(expr, deferred)) {
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

            // TODO: return an error
            // For now it will results in an error during processing
            if (op == arithmetic_op::divide || op == arithmetic_op::mod) {
                if (right_val == logical_value_t{resource, right_val.type()}) {
                    return false;
                }
            }

            // No type guard here any more: logical_value_t's arithmetic used to dispatch on the LEFT
            // type alone and read the right operand through the wrong getter, so a mixed pair had to
            // be declined at plan time. It refuses mismatches itself now and answers DATE + INTERVAL
            // properly; a refusal travels back through result_wrapper_t below, which leaves the
            // expression to the runtime evaluator -- exactly what declining did.

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
                // Unreachable (to_arithmetic_op maps exactly these five); refusal, not a silent NA.
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

        bool is_folded_constant(compare_type ct) noexcept {
            return ct == compare_type::all_true || ct == compare_type::all_false || ct == compare_type::all_unknown;
        }

        types::tri_bool_t folded_value(compare_type ct) noexcept {
            switch (ct) {
                case compare_type::all_true:
                    return types::tri_bool_t::yes;
                case compare_type::all_false:
                    return types::tri_bool_t::no;
                default:
                    return types::tri_bool_t::unknown;
            }
        }

        compare_type folded_type(types::tri_bool_t value) noexcept {
            switch (value) {
                case types::tri_bool_t::yes:
                    return compare_type::all_true;
                case types::tri_bool_t::no:
                    return compare_type::all_false;
                default:
                    return compare_type::all_unknown;
            }
        }

        bool is_value_comparison(compare_type ct) noexcept {
            switch (ct) {
                case compare_type::eq:
                case compare_type::ne:
                case compare_type::gt:
                case compare_type::lt:
                case compare_type::gte:
                case compare_type::lte:
                    return true;
                default:
                    return false;
            }
        }

        // Evaluate a constant comparison. Returns {true, result} on success.
        std::pair<bool, types::tri_bool_t>
        eval_compare(compare_type ct, const expr_value_t& left_val, const expr_value_t& right_val) {
            if (!is_value_comparison(ct)) {
                return {false, types::tri_bool_t::unknown};
            }
            if (left_val.is_null() || right_val.is_null()) {
                return {true, types::tri_bool_t::unknown};
            }

            auto cmp = left_val.compare(right_val);
            switch (ct) {
                case compare_type::eq:
                    return {true, types::tri_of(cmp == compare_t::equals)};
                case compare_type::ne:
                    return {true, types::tri_of(cmp != compare_t::equals)};
                case compare_type::gt:
                    return {true, types::tri_of(cmp == compare_t::more)};
                case compare_type::lt:
                    return {true, types::tri_of(cmp == compare_t::less)};
                case compare_type::gte:
                    return {true, types::tri_of(cmp == compare_t::more || cmp == compare_t::equals)};
                case compare_type::lte:
                    return {true, types::tri_of(cmp == compare_t::less || cmp == compare_t::equals)};
                default:
                    return {false, types::tri_bool_t::unknown};
            }
        }

        // Try to fold a compare expression where both sides are constant parameters
        void try_fold_compare(compare_expression_t& expr,
                              parameter_node_t* parameters,
                              const deferred_parameters_t* deferred) {
            // Only fold leaf comparisons (not union_and/or/not)
            if (is_union_compare_condition(expr.type())) {
                return;
            }
            if (is_folded_constant(expr.type()) || expr.do_not_fold()) {
                return;
            }
            if (is_deferred(expr.left(), deferred) || is_deferred(expr.right(), deferred)) {
                return;
            }

            bool left_is_param = std::holds_alternative<core::parameter_id_t>(expr.left());
            bool right_is_param = std::holds_alternative<core::parameter_id_t>(expr.right());

            if (left_is_param != right_is_param && is_value_comparison(expr.type())) {
                const auto constant_id = std::get<core::parameter_id_t>(left_is_param ? expr.left() : expr.right());
                if (parameters->parameter(constant_id).is_null()) {
                    expr.set_type(compare_type::all_unknown);
                }
                return;
            }

            // Both sides must be parameter_id_t
            if (!left_is_param || !right_is_param) {
                return;
            }

            auto left_id = std::get<core::parameter_id_t>(expr.left());
            auto right_id = std::get<core::parameter_id_t>(expr.right());

            const auto& left_val = parameters->parameter(left_id);
            const auto& right_val = parameters->parameter(right_id);

            auto [ok, result] = eval_compare(expr.type(), left_val, right_val);
            if (ok) {
                expr.set_type(folded_type(result));
            }
            // !ok: this comparison kind has no fold (regex / ANY / ALL / IS [NOT] NULL) — a skip, not an
            // assert, since folding is only an optimization and the runtime evaluator answers it anyway.
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
            auto dominating = is_and ? types::tri_bool_t::no : types::tri_bool_t::yes;

            auto folded = is_and ? types::tri_bool_t::yes : types::tri_bool_t::no;
            bool every_child_folded = true;
            for (const auto& child : comp->children()) {
                if (child->group() != expression_group::compare) {
                    every_child_folded = false;
                    continue;
                }
                auto ct = static_cast<const compare_expression_t*>(child.get())->type();
                if (!is_folded_constant(ct)) {
                    every_child_folded = false;
                    continue;
                }
                auto value = folded_value(ct);
                if (value == dominating) {
                    comp->set_type(folded_type(dominating));
                    return;
                }
                folded = is_and ? types::tri_and(folded, value) : types::tri_or(folded, value);
            }
            if (every_child_folded) {
                comp->set_type(folded_type(folded));
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

        void fold_expression(std::pmr::memory_resource* resource,
                             expression_ptr& expr,
                             parameter_node_t* parameters,
                             const deferred_parameters_t* deferred);

        void fold_scalar(std::pmr::memory_resource* resource,
                         scalar_expression_t* scalar,
                         parameter_node_t* parameters,
                         const deferred_parameters_t* deferred) {
            for (auto& param : scalar->params()) {
                if (!std::holds_alternative<expression_ptr>(param)) {
                    continue;
                }
                fold_expression(resource, std::get<expression_ptr>(param), parameters, deferred);
                try_promote_scalar(param);
            }
            auto folded = try_fold_scalar(resource, *scalar, parameters, deferred);
            if (folded.has_error()) {
                // Arithmetic refused the operands; this pass has no path to the user (optimize()
                // returns a plan, not a result), so leave the expression for the runtime evaluator.
                return;
            }
        }

        void fold_compare(std::pmr::memory_resource* resource,
                          compare_expression_t* comp,
                          parameter_node_t* parameters,
                          const deferred_parameters_t* deferred) {
            for (auto& child : comp->children()) {
                fold_expression(resource, child, parameters, deferred);
            }
            if (std::holds_alternative<expression_ptr>(comp->left())) {
                fold_expression(resource, std::get<expression_ptr>(comp->left()), parameters, deferred);
                try_promote_scalar(comp->left());
            }
            if (std::holds_alternative<expression_ptr>(comp->right())) {
                fold_expression(resource, std::get<expression_ptr>(comp->right()), parameters, deferred);
                try_promote_scalar(comp->right());
            }
            try_fold_compare(*comp, parameters, deferred);
            simplify_union(comp);
            // NOT over a fully folded single child folds to the complementary constant (multi-child
            // union_not means NOT(child1 AND child2 ...) and keeps its children). Without this,
            // `WHERE NOT (1=2)` survived into filter construction, whose guards were Release-erased asserts.
            if (comp->type() == compare_type::union_not && comp->children().size() == 1 &&
                comp->children().front()->group() == expression_group::compare) {
                const auto child_type =
                    static_cast<const compare_expression_t*>(comp->children().front().get())->type();
                if (is_folded_constant(child_type)) {
                    comp->set_type(folded_type(types::tri_not(folded_value(child_type))));
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

        void fold_expression(std::pmr::memory_resource* resource,
                             expression_ptr& expr,
                             parameter_node_t* parameters,
                             const deferred_parameters_t* deferred) {
            if (!expr) {
                return;
            }
            if (expr->group() == expression_group::scalar) {
                fold_scalar(resource, static_cast<scalar_expression_t*>(expr.get()), parameters, deferred);
            } else if (expr->group() == expression_group::compare) {
                fold_compare(resource, static_cast<compare_expression_t*>(expr.get()), parameters, deferred);
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
                        logical_plan::parameter_node_t* parameters,
                        const std::pmr::set<core::parameter_id_t>* deferred_parameters) {
        if (!node) {
            return;
        }

        // A LATERAL join's correlated slots are rebound per outer row and start as NA placeholders,
        // exactly like a sub-query's result slot
        std::pmr::set<core::parameter_id_t> deferred{resource};
        if (deferred_parameters != nullptr) {
            deferred.insert(deferred_parameters->begin(), deferred_parameters->end());
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
            if (current->type() == logical_plan::node_type::join_t) {
                for (const auto& correlation :
                     static_cast<const logical_plan::node_join_t*>(current.get())->correlations()) {
                    deferred.insert(correlation.first);
                }
            }
            order.push_back(std::move(current));
        }

        for (auto it = order.rbegin(); it != order.rend(); ++it) {
            if ((*it)->type() != logical_plan::node_type::match_t) {
                continue;
            }
            for (auto& expr : (*it)->expressions()) {
                fold_expression(resource, expr, parameters, &deferred);
            }
        }
    }

} // namespace components::planner::optimizer
