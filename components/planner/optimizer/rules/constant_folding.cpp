#include "constant_folding.hpp"

#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/types/tri_bool.hpp>
#include <components/vector/operations/apply_operator.hpp>
#include <components/vector/operations/apply_quantified.hpp>

#include <limits>
#include <optional>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;
        using namespace components::types;

        using components::operators::operator_code;

        // The operators scalar folding covers.
        operator_code folding_operator(scalar_type st) noexcept {
            switch (st) {
                case scalar_type::add:
                case scalar_type::subtract:
                case scalar_type::multiply:
                case scalar_type::divide:
                case scalar_type::mod:
                case scalar_type::unary_minus:
                    return to_operator_code(st);
                default:
                    return operator_code::invalid;
            }
        }

        // A new slot needs a free id; at the limit the fold is skipped.
        bool has_free_id(const storage_parameters& parameters) noexcept {
            return parameters.size() <= std::numeric_limits<uint32_t>::max();
        }

        // Only a literal or a bound $N is known before the statement runs; the rest is written while it runs.
        bool is_foldable(const storage_parameters& parameters, core::parameter_id_t id) {
            return parameters.kind(id) != parameter_kind::execution;
        }

        // A sub-query result or a LATERAL outer cell is written while the statement runs, so nothing that
        // reads it is foldable.
        bool is_deferred(const param_storage& slot, const storage_parameters& parameters) {
            if (!std::holds_alternative<core::parameter_id_t>(slot)) {
                return false;
            }
            return !is_foldable(parameters, std::get<core::parameter_id_t>(slot));
        }

        // Every operand of a scalar expression is a foldable parameter id.
        bool
        all_params_are_constants(const scalar_expression_t& expr, size_t arity, const storage_parameters& parameters) {
            if (expr.params().size() != arity) {
                return false;
            }
            for (const auto& param : expr.params()) {
                if (is_deferred(param, parameters) || !std::holds_alternative<core::parameter_id_t>(param)) {
                    return false;
                }
            }
            return true;
        }

        // Try to fold a scalar arithmetic expression with constant params. On success, replaces the
        // expression's params with a single parameter_id_t of a NEW slot that holds the computed result
        // (a slot keeps its type, and the result may have another).
        bool
        try_fold_scalar(scalar_expression_t& expr, parameter_node_t* parameters, const graph_execution_context& ctx) {
            const auto op = folding_operator(expr.type());
            if (op == operator_code::invalid) {
                return false;
            }
            const size_t arity = operators::arity_of(op) == operators::operator_arity::binary ? 2 : 1;
            const auto& storage = parameters->parameters();
            if (!all_params_are_constants(expr, arity, storage) || !has_free_id(storage) ||
                expr.result_type().type() == types::logical_type::INVALID) {
                return false;
            }

            const auto left_id = std::get<core::parameter_id_t>(expr.params()[0]);
            if (storage.is_null(left_id)) {
                return false;
            }
            const auto right_id = arity == 2 ? std::get<core::parameter_id_t>(expr.params()[1]) : left_id;
            if (storage.is_null(right_id)) {
                return false;
            }

            // Whatever the kernel refuses (a zero divisor, an overflow, operands of different types) is left
            // for the runtime to report.
            vector::vector_t out(storage.resource(), expr.result_type(), 1);
            const auto error =
                arity == 2
                    ? vector::operations::apply_binary(op, storage.at(left_id), storage.at(right_id), &out, ctx, 1)
                    : vector::operations::apply_unary(op, storage.at(left_id), &out, ctx, 1);
            if (error.contains_error()) {
                return false;
            }
            out.set_vector_type(vector::vector_type::CONSTANT);

            const auto folded_id = parameters->add_parameter(std::move(out), parameter_kind::literal);
            expr.params().clear();
            expr.append_param(folded_id);
            return true;
        }

        // Run the registered cast over a foldable constant with the real context; the result goes to a NEW
        // slot. A cast the kernel refuses is left for the runtime, which raises it only for a row that
        // reaches it.
        std::optional<core::parameter_id_t>
        try_fold_cast(const cast_expression_t& expr, parameter_node_t* parameters, const graph_execution_context& ctx) {
            if (!std::holds_alternative<core::parameter_id_t>(expr.child()) || !expr.cast() ||
                expr.result_type().type() == types::logical_type::INVALID) {
                return std::nullopt;
            }
            const auto& storage = parameters->parameters();
            const auto child_id = std::get<core::parameter_id_t>(expr.child());
            if (!is_foldable(storage, child_id) || !has_free_id(storage)) {
                return std::nullopt;
            }
            vector::vector_t out(storage.resource(), expr.result_type(), 1);
            if (expr.cast()(expr.kind(), storage.at(child_id), &out, ctx, 1).contains_error()) {
                return std::nullopt;
            }
            // A NULL result (a NULL operand, or a TRY_CAST that refused) stays a cast: folded, it would become an
            // all_unknown leaf, which the graph builder refuses under OR/AND/NOT (#747).
            if (out.is_null(0)) {
                return std::nullopt;
            }
            out.set_vector_type(vector::vector_type::CONSTANT);
            return parameters->add_parameter(std::move(out), parameter_kind::literal);
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

        // Evaluate a constant comparison. Returns {true, result} on success; a refusal of the kernel is a skip.
        std::pair<bool, types::tri_bool_t> eval_compare(std::pmr::memory_resource* resource,
                                                        compare_type ct,
                                                        const vector::vector_t& left,
                                                        const vector::vector_t& right,
                                                        const graph_execution_context& ctx) {
            if (!is_value_comparison(ct)) {
                return {false, types::tri_bool_t::unknown};
            }
            if (left.is_null(0) || right.is_null(0)) {
                return {true, types::tri_bool_t::unknown};
            }
            vector::vector_t out(resource, types::complex_logical_type{types::logical_type::BOOLEAN}, 1);
            if (vector::operations::apply_binary(to_operator_code(ct), left, right, &out, ctx, 1).contains_error()) {
                return {false, types::tri_bool_t::unknown};
            }
            return {true, out.is_null(0) ? types::tri_bool_t::unknown : types::tri_of(out.get_value<bool>(0))};
        }

        std::pair<bool, types::tri_bool_t> eval_quantified(const vector::storage_parameters& storage,
                                                           const compare_expression_t& expr,
                                                           const vector::vector_t& probe,
                                                           const vector::vector_t& set) {
            vector::operations::element_test_t test{to_operator_code(expr.inner_op())};
            if (expr.inner_op() == compare_type::regex) {
                auto flags = expr.regex_flags_param();
                if (!flags.has_value()) {
                    return {false, types::tri_bool_t::unknown};
                }
                test = vector::operations::element_test_t{
                    vector::operations::match_flags_of(storage.at(*flags).get_value<std::string_view>(0))};
            } else if (!vector::operations::is_quantifiable(test.code())) {
                return {false, types::tri_bool_t::unknown};
            }
            vector::vector_t out(storage.resource(), types::complex_logical_type{types::logical_type::BOOLEAN}, 1);
            auto kind = expr.type() == compare_type::any ? vector::operations::quantifier::any
                                                         : vector::operations::quantifier::all;
            if (vector::operations::apply_quantified(test, kind, probe, set, &out, 1).contains_error()) {
                return {false, types::tri_bool_t::unknown};
            }
            return {true, out.is_null(0) ? types::tri_bool_t::unknown : types::tri_of(out.get_value<bool>(0))};
        }

        // Try to fold a compare expression where both sides are constant parameters
        void
        try_fold_compare(compare_expression_t& expr, parameter_node_t* parameters, const graph_execution_context& ctx) {
            // Only fold leaf comparisons (not union_and/or/not)
            if (is_union_compare_condition(expr.type())) {
                return;
            }
            if (is_folded_constant(expr.type()) || expr.do_not_fold()) {
                return;
            }
            if (is_deferred(expr.left(), parameters->parameters()) ||
                is_deferred(expr.right(), parameters->parameters())) {
                return;
            }

            bool left_is_param = std::holds_alternative<core::parameter_id_t>(expr.left());
            bool right_is_param = std::holds_alternative<core::parameter_id_t>(expr.right());

            if (left_is_param != right_is_param && is_value_comparison(expr.type())) {
                const auto constant_id = std::get<core::parameter_id_t>(left_is_param ? expr.left() : expr.right());
                if (parameters->parameters().is_null(constant_id)) {
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

            const auto& storage = parameters->parameters();
            bool quantified = expr.type() == compare_type::any || expr.type() == compare_type::all;
            auto [ok, result] =
                quantified
                    ? eval_quantified(storage, expr, storage.at(left_id), storage.at(right_id))
                    : eval_compare(storage.resource(), expr.type(), storage.at(left_id), storage.at(right_id), ctx);
            if (ok) {
                expr.set_type(folded_type(result));
            }
            // otherwise this comparison kind has no fold
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

        // True when a scalar expression was folded down to the single id of a new slot.
        bool fold_expression(std::pmr::memory_resource* resource,
                             expression_ptr& expr,
                             parameter_node_t* parameters,
                             const graph_execution_context& ctx);

        // Promote a folded scalar or cast expression_ptr to parameter_id_t.
        // IMPORTANT: extract the id by value BEFORE assigning to slot,
        // because the assignment destroys the expression_ptr which may
        // free the expression (use-after-free if we hold a reference).
        void promote_slot(std::pmr::memory_resource* resource,
                          param_storage& slot,
                          parameter_node_t* parameters,
                          const graph_execution_context& ctx) {
            if (!std::holds_alternative<expression_ptr>(slot)) {
                return;
            }
            auto& nested = std::get<expression_ptr>(slot);
            if (!nested) {
                return;
            }
            if (nested->group() == expression_group::scalar) {
                // Only an expression that really folded stands for its single operand; a one-operand
                // operator the fold refused still applies it.
                if (fold_expression(resource, nested, parameters, ctx)) {
                    auto* ns = static_cast<scalar_expression_t*>(nested.get());
                    auto id = std::get<core::parameter_id_t>(ns->params()[0]);
                    slot = id;
                }
            } else if (nested->group() == expression_group::cast) {
                auto* cast = static_cast<cast_expression_t*>(nested.get());
                if (std::holds_alternative<expression_ptr>(cast->child())) {
                    promote_slot(resource, cast->child(), parameters, ctx);
                }
                if (const auto id = try_fold_cast(*cast, parameters, ctx)) {
                    slot = *id;
                }
            }
        }

        bool fold_scalar(std::pmr::memory_resource* resource,
                         scalar_expression_t* scalar,
                         parameter_node_t* parameters,
                         const graph_execution_context& ctx) {
            for (auto& param : scalar->params()) {
                if (!std::holds_alternative<expression_ptr>(param)) {
                    continue;
                }
                promote_slot(resource, param, parameters, ctx);
            }
            return try_fold_scalar(*scalar, parameters, ctx);
        }

        void fold_compare(std::pmr::memory_resource* resource,
                          compare_expression_t* comp,
                          parameter_node_t* parameters,
                          const graph_execution_context& ctx) {
            for (auto& child : comp->children()) {
                fold_expression(resource, child, parameters, ctx);
            }
            if (std::holds_alternative<expression_ptr>(comp->left())) {
                promote_slot(resource, comp->left(), parameters, ctx);
            }
            if (std::holds_alternative<expression_ptr>(comp->right())) {
                promote_slot(resource, comp->right(), parameters, ctx);
            }
            try_fold_compare(*comp, parameters, ctx);
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

        bool fold_expression(std::pmr::memory_resource* resource,
                             expression_ptr& expr,
                             parameter_node_t* parameters,
                             const graph_execution_context& ctx) {
            if (!expr) {
                return false;
            }
            if (expr->group() == expression_group::scalar) {
                return fold_scalar(resource, static_cast<scalar_expression_t*>(expr.get()), parameters, ctx);
            } else if (expr->group() == expression_group::compare) {
                fold_compare(resource, static_cast<compare_expression_t*>(expr.get()), parameters, ctx);
                auto* comp = static_cast<compare_expression_t*>(expr.get());
                if ((comp->type() == compare_type::union_and || comp->type() == compare_type::union_or) &&
                    comp->children().size() == 1) {
                    expr = comp->children().front();
                }
            }
            return false;
        }

    } // namespace

    void fold_constants(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr& node,
                        logical_plan::parameter_node_t* parameters,
                        const graph_execution_context& ctx) {
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
                fold_expression(resource, expr, parameters, ctx);
            }
        }
    }

} // namespace components::planner::optimizer
