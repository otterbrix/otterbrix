#include "arithmetic_eval.hpp"

namespace components::operators {

    namespace detail {

        vector::arithmetic_op scalar_to_arithmetic_op(expressions::scalar_type t) {
            switch (t) {
                case expressions::scalar_type::add:
                    return vector::arithmetic_op::add;
                case expressions::scalar_type::subtract:
                    return vector::arithmetic_op::subtract;
                case expressions::scalar_type::multiply:
                    return vector::arithmetic_op::multiply;
                case expressions::scalar_type::divide:
                    return vector::arithmetic_op::divide;
                case expressions::scalar_type::mod:
                    return vector::arithmetic_op::mod;
                default:
                    throw std::logic_error("Not an arithmetic scalar_type");
            }
        }

        resolved_operand resolve_operand(
            const expressions::param_storage& param,
            vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            std::pmr::memory_resource* resource,
            std::deque<vector::vector_t>& temp_vecs) {
            resolved_operand result;
            if (std::holds_alternative<expressions::key_t>(param)) {
                const auto& key = std::get<expressions::key_t>(param);
                for (auto& vec : chunk.data) {
                    if (vec.type().alias() == key.as_string()) {
                        result.vec = &vec;
                        return result;
                    }
                }
                // Try sub-column path via key indices
                if (!key.path().empty()) {
                    result.vec = chunk.at(key.path());
                    if (result.vec) return result;
                }
                throw std::logic_error("Column not found in chunk: " + key.as_string());
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                result.scalar = params.parameters.at(id);
                return result;
            } else {
                // expression_ptr → recursive evaluation
                const auto& expr_ptr = std::get<expressions::expression_ptr>(param);
                if (expr_ptr->group() == expressions::expression_group::scalar) {
                    auto* scalar_expr =
                        static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());

                    if (scalar_expr->type() == expressions::scalar_type::case_expr) {
                        // CASE sub-expression: evaluate per-row
                        auto computed = evaluate_case_expr(resource, scalar_expr->params(), chunk, params);
                        temp_vecs.emplace_back(std::move(computed));
                        result.vec = &temp_vecs.back();
                        return result;
                    }

                    // Arithmetic sub-expression
                    auto op = scalar_to_arithmetic_op(scalar_expr->type());
                    auto& operands = scalar_expr->params();

                    std::deque<vector::vector_t> sub_temps;
                    auto left_op = resolve_operand(operands[0], chunk, params, resource, sub_temps);
                    auto right_op = resolve_operand(operands[1], chunk, params, resource, sub_temps);
                    uint64_t count = chunk.size();

                    vector::vector_t computed(resource, types::complex_logical_type(types::logical_type::BIGINT), 0);
                    if (left_op.vec && right_op.vec) {
                        computed = vector::compute_binary_arithmetic(resource, op, *left_op.vec, *right_op.vec, count);
                    } else if (left_op.vec && right_op.scalar) {
                        computed =
                            vector::compute_vector_scalar_arithmetic(resource, op, *left_op.vec, *right_op.scalar, count);
                    } else if (left_op.scalar && right_op.vec) {
                        computed =
                            vector::compute_scalar_vector_arithmetic(resource, op, *left_op.scalar, *right_op.vec, count);
                    } else {
                        // scalar-scalar
                        auto lval = *left_op.scalar;
                        auto rval = *right_op.scalar;
                        types::logical_value_t result_val(resource, types::logical_type::NA);
                        switch (scalar_expr->type()) {
                            case expressions::scalar_type::add:
                                result_val = types::logical_value_t::sum(lval, rval);
                                break;
                            case expressions::scalar_type::subtract:
                                result_val = types::logical_value_t::subtract(lval, rval);
                                break;
                            case expressions::scalar_type::multiply:
                                result_val = types::logical_value_t::mult(lval, rval);
                                break;
                            case expressions::scalar_type::divide:
                                result_val = types::logical_value_t::divide(lval, rval);
                                break;
                            case expressions::scalar_type::mod:
                                result_val = types::logical_value_t::modulus(lval, rval);
                                break;
                            default:
                                break;
                        }
                        uint64_t out_count = count > 0 ? count : 1;
                        computed = vector::vector_t(resource, result_val.type(), out_count);
                        for (uint64_t i = 0; i < out_count; i++) {
                            computed.set_value(i, result_val);
                        }
                    }
                    // Move sub_temps into parent temp_vecs to keep them alive
                    for (auto& t : sub_temps) {
                        temp_vecs.emplace_back(std::move(t));
                    }
                    temp_vecs.emplace_back(std::move(computed));
                    result.vec = &temp_vecs.back();
                    return result;
                }
                throw std::logic_error("Unsupported expression type in arithmetic operand");
            }
        }

        types::logical_value_t resolve_row_value(
            std::pmr::memory_resource* resource,
            const expressions::param_storage& param,
            const vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            size_t row_idx) {
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                for (auto& vec : chunk.data) {
                    if (vec.type().alias() == key.as_string()) {
                        return vec.value(row_idx);
                    }
                }
                if (!key.path().empty()) {
                    auto* vec = chunk.at(key.path());
                    if (vec) return vec->value(row_idx);
                }
                throw std::logic_error("CASE: column not found: " + key.as_string());
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                return params.parameters.at(id);
            } else {
                auto& expr_ptr = std::get<expressions::expression_ptr>(param);
                if (expr_ptr->group() == expressions::expression_group::scalar) {
                    auto* scalar =
                        static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());
                    if (scalar->type() == expressions::scalar_type::case_expr) {
                        // Nested CASE — recursive per-row evaluation
                        auto& ops = scalar->params();
                        bool has_default = (ops.size() % 2 == 1);
                        size_t num_whens = ops.size() / 2;
                        for (size_t w = 0; w < num_whens; w++) {
                            auto& cond_param = ops[w * 2];
                            if (std::holds_alternative<expressions::expression_ptr>(cond_param)) {
                                auto& cond_expr = std::get<expressions::expression_ptr>(cond_param);
                                if (evaluate_row_condition(resource, cond_expr, chunk, params, row_idx)) {
                                    return resolve_row_value(resource, ops[w * 2 + 1], chunk, params, row_idx);
                                }
                            }
                        }
                        if (has_default) {
                            return resolve_row_value(resource, ops.back(), chunk, params, row_idx);
                        }
                        return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    }
                    // Arithmetic sub-expression
                    auto l = resolve_row_value(resource, scalar->params()[0], chunk, params, row_idx);
                    auto r = resolve_row_value(resource, scalar->params()[1], chunk, params, row_idx);
                    switch (scalar->type()) {
                        case expressions::scalar_type::add:
                            return types::logical_value_t::sum(l, r);
                        case expressions::scalar_type::subtract:
                            return types::logical_value_t::subtract(l, r);
                        case expressions::scalar_type::multiply:
                            return types::logical_value_t::mult(l, r);
                        case expressions::scalar_type::divide:
                            return types::logical_value_t::divide(l, r);
                        case expressions::scalar_type::mod:
                            return types::logical_value_t::modulus(l, r);
                        default:
                            break;
                    }
                }
                throw std::logic_error("CASE: unsupported sub-expression");
            }
        }

        bool evaluate_row_condition(
            std::pmr::memory_resource* resource,
            const expressions::expression_ptr& condition,
            const vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            size_t row_idx) {
            if (condition->group() != expressions::expression_group::compare) return false;
            auto* cmp = static_cast<const expressions::compare_expression_t*>(condition.get());

            if (cmp->is_union()) {
                bool is_and = (cmp->type() == expressions::compare_type::union_and);
                for (auto& child : cmp->children()) {
                    bool child_result = evaluate_row_condition(resource, child, chunk, params, row_idx);
                    if (is_and && !child_result) return false;
                    if (!is_and && child_result) return true;
                }
                return is_and;
            }

            auto left_val = resolve_row_value(resource, cmp->left(), chunk, params, row_idx);
            auto right_val = resolve_row_value(resource, cmp->right(), chunk, params, row_idx);
            auto cmp_result = left_val.compare(right_val);
            switch (cmp->type()) {
                case expressions::compare_type::gt:
                    return cmp_result == types::compare_t::more;
                case expressions::compare_type::gte:
                    return cmp_result >= types::compare_t::equals;
                case expressions::compare_type::lt:
                    return cmp_result == types::compare_t::less;
                case expressions::compare_type::lte:
                    return cmp_result <= types::compare_t::equals;
                case expressions::compare_type::eq:
                    return cmp_result == types::compare_t::equals;
                case expressions::compare_type::ne:
                    return cmp_result != types::compare_t::equals;
                default:
                    return false;
            }
        }

        vector::vector_t evaluate_case_expr(
            std::pmr::memory_resource* resource,
            const std::pmr::vector<expressions::param_storage>& operands,
            vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params) {
            uint64_t count = chunk.size();
            bool has_default = (operands.size() % 2 == 1);
            size_t num_whens = operands.size() / 2;

            // Determine result type from first matching value for row 0
            types::complex_logical_type result_type(types::logical_type::NA);
            if (count > 0) {
                // Try first THEN result
                auto val = resolve_row_value(resource, operands[1], chunk, params, 0);
                result_type = val.type();
            }

            vector::vector_t output(resource, result_type, count);

            for (uint64_t i = 0; i < count; i++) {
                bool matched = false;
                for (size_t w = 0; w < num_whens; w++) {
                    auto& cond_param = operands[w * 2];
                    if (std::holds_alternative<expressions::expression_ptr>(cond_param)) {
                        auto& cond_expr = std::get<expressions::expression_ptr>(cond_param);
                        if (evaluate_row_condition(resource, cond_expr, chunk, params, i)) {
                            auto val = resolve_row_value(resource, operands[w * 2 + 1], chunk, params, i);
                            output.set_value(i, val);
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched && has_default) {
                    auto val = resolve_row_value(resource, operands.back(), chunk, params, i);
                    output.set_value(i, val);
                }
            }
            return output;
        }

    } // namespace detail

    vector::vector_t evaluate_arithmetic(std::pmr::memory_resource* resource,
                                         expressions::scalar_type op,
                                         const std::pmr::vector<expressions::param_storage>& operands,
                                         vector::data_chunk_t& chunk,
                                         const logical_plan::storage_parameters& params) {
        // CASE expression: per-row evaluation
        if (op == expressions::scalar_type::case_expr) {
            return detail::evaluate_case_expr(resource, operands, chunk, params);
        }

        std::deque<vector::vector_t> temp_vecs;

        auto left_op = detail::resolve_operand(operands[0], chunk, params, resource, temp_vecs);
        auto right_op = detail::resolve_operand(operands[1], chunk, params, resource, temp_vecs);

        uint64_t count = chunk.size();
        auto arith_op = detail::scalar_to_arithmetic_op(op);

        if (left_op.vec && right_op.vec) {
            return vector::compute_binary_arithmetic(resource, arith_op, *left_op.vec, *right_op.vec, count);
        } else if (left_op.vec && right_op.scalar) {
            return vector::compute_vector_scalar_arithmetic(resource, arith_op, *left_op.vec, *right_op.scalar, count);
        } else if (left_op.scalar && right_op.vec) {
            return vector::compute_scalar_vector_arithmetic(resource, arith_op, *left_op.scalar, *right_op.vec, count);
        } else {
            // scalar-scalar
            auto lval = *left_op.scalar;
            auto rval = *right_op.scalar;
            types::logical_value_t result_val(resource, types::logical_type::NA);
            switch (op) {
                case expressions::scalar_type::add:
                    result_val = types::logical_value_t::sum(lval, rval);
                    break;
                case expressions::scalar_type::subtract:
                    result_val = types::logical_value_t::subtract(lval, rval);
                    break;
                case expressions::scalar_type::multiply:
                    result_val = types::logical_value_t::mult(lval, rval);
                    break;
                case expressions::scalar_type::divide:
                    result_val = types::logical_value_t::divide(lval, rval);
                    break;
                case expressions::scalar_type::mod:
                    result_val = types::logical_value_t::modulus(lval, rval);
                    break;
                default:
                    break;
            }
            uint64_t out_count = count > 0 ? count : 1;
            vector::vector_t output(resource, result_val.type(), out_count);
            for (uint64_t i = 0; i < out_count; i++) {
                output.set_value(i, result_val);
            }
            return output;
        }
    }

} // namespace components::operators