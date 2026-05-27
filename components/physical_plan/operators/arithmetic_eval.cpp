#include "arithmetic_eval.hpp"

#include <core/result_wrapper.hpp>

namespace components::operators {

    namespace detail {

        // TODO: consider removing arithmetic_op enum in favor of using scalar_type directly
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

        core::result_wrapper_t<resolved_operand> resolve_operand(const expressions::param_storage& param,
                                                                 vector::data_chunk_t& chunk,
                                                                 const logical_plan::storage_parameters& params,
                                                                 std::pmr::memory_resource* resource,
                                                                 std::deque<vector::vector_t>& temp_vecs,
                                                                 core::date::timezone_offset_t session_tz) {
            resolved_operand result;
            if (std::holds_alternative<expressions::key_t>(param)) {
                const auto& key = std::get<expressions::key_t>(param);
                assert(!key.path().empty() && "key path must be resolved before execution");
                result.vec = chunk.at(key.path());
                if (result.vec) {
                    return result;
                } else {
                    return core::error_t(core::error_code_t::arithmetics_failure,

                                         std::pmr::string{"Column not found in chunk: " + key.as_string()});
                }
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                result.scalar = params.parameters.at(id);
                return result;
            } else {
                const auto& expr_ptr = std::get<expressions::expression_ptr>(param);
                if (expr_ptr->group() == expressions::expression_group::scalar) {
                    auto* scalar_expr = static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());

                    if (scalar_expr->type() == expressions::scalar_type::case_expr) {
                        auto computed = evaluate_case_expr(resource, scalar_expr->params(), chunk, params, session_tz);
                        temp_vecs.emplace_back(std::move(computed));
                        result.vec = &temp_vecs.back();
                        return result;
                    }

                    if (scalar_expr->type() == expressions::scalar_type::unary_minus) {
                        auto& operands = scalar_expr->params();
                        if (operands.empty()) {
                            throw std::logic_error("Unary minus requires 1 operand");
                        }
                        std::deque<vector::vector_t> sub_temps;
                        auto inner_op = resolve_operand(operands[0], chunk, params, resource, sub_temps, session_tz);
                        if (inner_op.has_error()) {
                            return inner_op;
                        }

                        uint64_t count = chunk.size();
                        vector::vector_t computed(resource,
                                                  types::complex_logical_type(types::logical_type::BIGINT),
                                                  0);
                        if (inner_op.value().vec) {
                            computed = vector::compute_unary_neg(resource, *inner_op.value().vec, count);
                        } else {
                            uint64_t out_count = count > 0 ? count : 1;
                            vector::vector_t scalar_vec(resource, *inner_op.value().scalar, out_count);
                            scalar_vec.flatten(out_count);
                            computed = vector::compute_unary_neg(resource, scalar_vec, out_count);
                        }
                        for (auto& t : sub_temps) {
                            temp_vecs.emplace_back(std::move(t));
                        }
                        temp_vecs.emplace_back(std::move(computed));
                        result.vec = &temp_vecs.back();
                        return result;
                    }

                    auto op = scalar_to_arithmetic_op(scalar_expr->type());
                    auto& operands = scalar_expr->params();
                    if (operands.size() < 2) {
                        throw std::logic_error("Arithmetic expression requires at least 2 operands");
                    }

                    std::deque<vector::vector_t> sub_temps;
                    auto left_op = resolve_operand(operands[0], chunk, params, resource, sub_temps, session_tz);
                    if (left_op.has_error()) {
                        return left_op;
                    }
                    auto right_op = resolve_operand(operands[1], chunk, params, resource, sub_temps, session_tz);
                    if (right_op.has_error()) {
                        return right_op;
                    }
                    uint64_t count = chunk.size();

                    vector::vector_t computed(resource, types::complex_logical_type(types::logical_type::BIGINT), 0);
                    if (left_op.value().vec && right_op.value().vec) {
                        computed = vector::compute_binary_arithmetic(resource,
                                                                     op,
                                                                     *left_op.value().vec,
                                                                     *right_op.value().vec,
                                                                     count);
                    } else if (left_op.value().vec && right_op.value().scalar) {
                        computed = vector::compute_vector_scalar_arithmetic(resource,
                                                                            op,
                                                                            *left_op.value().vec,
                                                                            *right_op.value().scalar,
                                                                            count);
                    } else if (left_op.value().scalar && right_op.value().vec) {
                        computed = vector::compute_scalar_vector_arithmetic(resource,
                                                                            op,
                                                                            *left_op.value().scalar,
                                                                            *right_op.value().vec,
                                                                            count);
                    } else {
                        auto lval = *left_op.value().scalar;
                        auto rval = *right_op.value().scalar;
                        uint64_t out_count = count > 0 ? count : 1;
                        vector::vector_t left_vec(resource, lval, out_count);
                        left_vec.flatten(out_count);
                        computed = vector::compute_vector_scalar_arithmetic(resource, op, left_vec, rval, out_count);
                    }
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

        types::logical_value_t resolve_row_value(std::pmr::memory_resource* resource,
                                                 const expressions::param_storage& param,
                                                 const vector::data_chunk_t& chunk,
                                                 const logical_plan::storage_parameters& params,
                                                 size_t row_idx,
                                                 core::date::timezone_offset_t session_tz) {
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                assert(!key.path().empty() && "key path must be resolved before execution");
                auto* vec = chunk.at(key.path());
                if (vec) {
                    return vec->value(row_idx);
                }
                throw std::logic_error("CASE: column not found: " + key.as_string());
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                return params.parameters.at(id);
            } else {
                auto& expr_ptr = std::get<expressions::expression_ptr>(param);
                if (expr_ptr->group() == expressions::expression_group::scalar) {
                    auto* scalar = static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());
                    if (scalar->type() == expressions::scalar_type::case_expr) {
                        // Nested CASE — recursive per-row evaluation
                        auto& ops = scalar->params();
                        bool has_default = (ops.size() % 2 == 1);
                        size_t num_whens = ops.size() / 2;
                        for (size_t w = 0; w < num_whens; w++) {
                            auto& cond_param = ops[w * 2];
                            if (std::holds_alternative<expressions::expression_ptr>(cond_param)) {
                                auto& cond_expr = std::get<expressions::expression_ptr>(cond_param);
                                if (evaluate_row_condition(resource, cond_expr, chunk, params, row_idx, session_tz)) {
                                    return resolve_row_value(resource,
                                                             ops[w * 2 + 1],
                                                             chunk,
                                                             params,
                                                             row_idx,
                                                             session_tz);
                                }
                            }
                        }
                        if (has_default) {
                            return resolve_row_value(resource, ops.back(), chunk, params, row_idx, session_tz);
                        }
                        return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    }
                    // Unary minus sub-expression
                    if (scalar->type() == expressions::scalar_type::unary_minus) {
                        if (scalar->params().empty()) {
                            throw std::logic_error("CASE: unary minus requires 1 operand");
                        }
                        auto inner =
                            resolve_row_value(resource, scalar->params()[0], chunk, params, row_idx, session_tz);
                        return types::logical_value_t::subtract(types::logical_value_t(resource, int64_t(0)), inner);
                    }
                    // Arithmetic sub-expression
                    if (scalar->params().size() < 2) {
                        throw std::logic_error("CASE: arithmetic sub-expression requires 2 operands");
                    }
                    auto l = resolve_row_value(resource, scalar->params()[0], chunk, params, row_idx, session_tz);
                    auto r = resolve_row_value(resource, scalar->params()[1], chunk, params, row_idx, session_tz);
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

        bool evaluate_row_condition(std::pmr::memory_resource* resource,
                                    const expressions::expression_ptr& condition,
                                    const vector::data_chunk_t& chunk,
                                    const logical_plan::storage_parameters& params,
                                    size_t row_idx,
                                    core::date::timezone_offset_t session_tz) {
            if (condition->group() != expressions::expression_group::compare)
                return false;
            auto* cmp = static_cast<const expressions::compare_expression_t*>(condition.get());

            if (cmp->is_union()) {
                bool is_and = (cmp->type() == expressions::compare_type::union_and);
                for (auto& child : cmp->children()) {
                    bool child_result = evaluate_row_condition(resource, child, chunk, params, row_idx, session_tz);
                    if (is_and && !child_result)
                        return false;
                    if (!is_and && child_result)
                        return true;
                }
                return is_and;
            }

            auto left_val = resolve_row_value(resource, cmp->left(), chunk, params, row_idx, session_tz);
            auto right_val = resolve_row_value(resource, cmp->right(), chunk, params, row_idx, session_tz);
            if (left_val.type() != right_val.type()) {
                auto cast_right = right_val.cast_as(left_val.type(), session_tz);
                if (!cast_right.is_null()) {
                    right_val = std::move(cast_right);
                } else {
                    auto cast_left = left_val.cast_as(right_val.type(), session_tz);
                    if (!cast_left.is_null()) {
                        left_val = std::move(cast_left);
                    }
                }
            }
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

        vector::vector_t evaluate_case_expr(std::pmr::memory_resource* resource,
                                            const std::pmr::vector<expressions::param_storage>& operands,
                                            vector::data_chunk_t& chunk,
                                            const logical_plan::storage_parameters& params,
                                            core::date::timezone_offset_t session_tz) {
            uint64_t count = chunk.size();
            bool has_default = (operands.size() % 2 == 1);
            size_t num_whens = operands.size() / 2;

            // Determine result type from first matching value for row 0
            types::complex_logical_type result_type(types::logical_type::NA);
            if (count > 0) {
                // Try first THEN result
                auto val = resolve_row_value(resource, operands[1], chunk, params, 0, session_tz);
                result_type = val.type();
            }

            vector::vector_t output(resource, result_type, count);

            auto coerce_to_result = [&](types::logical_value_t val) -> types::logical_value_t {
                if (!val.is_null() && val.type() != result_type) {
                    auto casted = val.cast_as(result_type, session_tz);
                    if (!casted.is_null()) {
                        return casted;
                    }
                }
                return val;
            };

            for (uint64_t i = 0; i < count; i++) {
                bool matched = false;
                for (size_t w = 0; w < num_whens; w++) {
                    auto& cond_param = operands[w * 2];
                    if (std::holds_alternative<expressions::expression_ptr>(cond_param)) {
                        auto& cond_expr = std::get<expressions::expression_ptr>(cond_param);
                        if (evaluate_row_condition(resource, cond_expr, chunk, params, i, session_tz)) {
                            auto val = coerce_to_result(
                                resolve_row_value(resource, operands[w * 2 + 1], chunk, params, i, session_tz));
                            output.set_value(i, val);
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched && has_default) {
                    auto val =
                        coerce_to_result(resolve_row_value(resource, operands.back(), chunk, params, i, session_tz));
                    output.set_value(i, val);
                } else if (!matched) {
                    output.set_null(i, true);
                }
            }
            return output;
        }

    } // namespace detail

    // TODO: validate arithmetic column resolution during plan validation phase
    core::result_wrapper_t<vector::vector_t>
    evaluate_arithmetic(std::pmr::memory_resource* resource,
                        expressions::scalar_type op,
                        const std::pmr::vector<expressions::param_storage>& operands,
                        vector::data_chunk_t& chunk,
                        const logical_plan::storage_parameters& params,
                        core::date::timezone_offset_t session_tz) {
        if (op == expressions::scalar_type::case_expr) {
            return detail::evaluate_case_expr(resource, operands, chunk, params, session_tz);
        }

        if (op == expressions::scalar_type::unary_minus) {
            if (operands.empty()) {
                return core::error_t(core::error_code_t::arithmetics_failure,

                                     std::pmr::string{"unary minus requires 1 operand", resource});
            }
            std::deque<vector::vector_t> temp_vecs;
            auto operand_res = detail::resolve_operand(operands[0], chunk, params, resource, temp_vecs, session_tz);
            if (operand_res.has_error()) {
                return operand_res.convert_error<vector::vector_t>();
            }
            uint64_t count = chunk.size();
            if (operand_res.value().vec) {
                return vector::compute_unary_neg(resource, *operand_res.value().vec, count);
            } else {
                uint64_t out_count = count > 0 ? count : 1;
                vector::vector_t scalar_vec(resource, *operand_res.value().scalar, out_count);
                scalar_vec.flatten(out_count);
                return vector::compute_unary_neg(resource, scalar_vec, out_count);
            }
        }

        if (operands.size() < 2) {
            return core::error_t(core::error_code_t::arithmetics_failure,

                                 std::pmr::string{"arithmetic expression requires at least 2 operands", resource});
        }

        std::deque<vector::vector_t> temp_vecs;

        auto left_op = detail::resolve_operand(operands[0], chunk, params, resource, temp_vecs, session_tz);
        if (left_op.has_error()) {
            return left_op.convert_error<vector::vector_t>();
        }
        auto right_op = detail::resolve_operand(operands[1], chunk, params, resource, temp_vecs, session_tz);
        if (right_op.has_error()) {
            return right_op.convert_error<vector::vector_t>();
        }

        uint64_t count = chunk.size();
        auto arith_op = detail::scalar_to_arithmetic_op(op);

        if (left_op.value().vec && right_op.value().vec) {
            return vector::compute_binary_arithmetic(resource,
                                                     arith_op,
                                                     *left_op.value().vec,
                                                     *right_op.value().vec,
                                                     count);
        } else if (left_op.value().vec && right_op.value().scalar) {
            if (arith_op == vector::arithmetic_op::divide || arith_op == vector::arithmetic_op::mod) {
                types::logical_value_t zero(resource, right_op.value().scalar->type());
                if (*right_op.value().scalar == zero) {
                    return core::error_t(core::error_code_t::arithmetics_failure,

                                         std::pmr::string{"division by zero", resource});
                }
            }
            return vector::compute_vector_scalar_arithmetic(resource,
                                                            arith_op,
                                                            *left_op.value().vec,
                                                            *right_op.value().scalar,
                                                            count);
        } else if (left_op.value().scalar && right_op.value().vec) {
            return vector::compute_scalar_vector_arithmetic(resource,
                                                            arith_op,
                                                            *left_op.value().scalar,
                                                            *right_op.value().vec,
                                                            count);
        } else {
            auto lval = *left_op.value().scalar;
            auto rval = *right_op.value().scalar;
            if (arith_op == vector::arithmetic_op::divide || arith_op == vector::arithmetic_op::mod) {
                types::logical_value_t zero(resource, rval.type());
                if (rval == zero) {
                    return core::error_t(core::error_code_t::arithmetics_failure,

                                         std::pmr::string{"division by zero", resource});
                }
            }
            uint64_t out_count = count > 0 ? count : 1;
            vector::vector_t left_vec(resource, lval, out_count);
            left_vec.flatten(out_count);
            return vector::compute_vector_scalar_arithmetic(resource, arith_op, left_vec, rval, out_count);
        }
    }

} // namespace components::operators