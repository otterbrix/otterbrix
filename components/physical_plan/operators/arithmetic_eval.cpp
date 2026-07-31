#include "arithmetic_eval.hpp"

#include "compare_3vl.hpp"
#include <core/regex/regex.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    namespace detail {

        // TODO: consider removing arithmetic_op enum in favor of using scalar_type directly
        // Returns false if t is not an arithmetic scalar_type.
        bool scalar_to_arithmetic_op(expressions::scalar_type t, vector::arithmetic_op& out) {
            switch (t) {
                case expressions::scalar_type::add:
                    out = vector::arithmetic_op::add;
                    return true;
                case expressions::scalar_type::subtract:
                    out = vector::arithmetic_op::subtract;
                    return true;
                case expressions::scalar_type::multiply:
                    out = vector::arithmetic_op::multiply;
                    return true;
                case expressions::scalar_type::divide:
                    out = vector::arithmetic_op::divide;
                    return true;
                case expressions::scalar_type::mod:
                    out = vector::arithmetic_op::mod;
                    return true;
                default:
                    return false;
            }
        }

        types::logical_type operand_type(const resolved_operand& operand) {
            return operand.vec ? operand.vec->type().type() : operand.scalar->type().type();
        }

        core::error_t unsupported_arithmetic_error(std::pmr::memory_resource* resource) {
            return core::error_t(core::error_code_t::arithmetics_failure,
                                 std::pmr::string{"arithmetic requires numeric or compatible temporal operands",
                                                  resource});
        }

        core::error_t unsupported_unary_minus_error(std::pmr::memory_resource* resource) {
            return core::error_t(core::error_code_t::arithmetics_failure,
                                 std::pmr::string{"unary minus requires a numeric operand", resource});
        }

        core::result_wrapper_t<resolved_operand> resolve_operand(const expressions::param_storage& param,
                                                                 vector::data_chunk_t& chunk,
                                                                 const logical_plan::storage_parameters& params,
                                                                 std::pmr::memory_resource* resource,
                                                                 std::deque<vector::data_chunk_t::at_aligned_t>& temp_vecs,
                                                                 core::date::timezone_offset_t session_tz) {
            resolved_operand result;
            if (std::holds_alternative<expressions::key_t>(param)) {
                const auto& key = std::get<expressions::key_t>(param);
                assert(!key.path().empty() && "key path must be resolved before execution");
                temp_vecs.push_back(chunk.at_aligned(key.path(), resource));
                result.vec = temp_vecs.back().get();
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
                        if (computed.has_error()) {
                            return computed.convert_error<resolved_operand>();
                        }
                        temp_vecs.emplace_back(std::move(computed.value()));
                        result.vec = temp_vecs.back().get();
                        return result;
                    }

                    if (scalar_expr->type() == expressions::scalar_type::unary_minus) {
                        auto& operands = scalar_expr->params();
                        if (operands.empty()) {
                            return core::error_t(core::error_code_t::arithmetics_failure,
                                                 std::pmr::string{"Unary minus requires 1 operand", resource});
                        }
                        std::deque<vector::data_chunk_t::at_aligned_t> sub_temps;
                        auto inner_op = resolve_operand(operands[0], chunk, params, resource, sub_temps, session_tz);
                        if (inner_op.has_error()) {
                            return inner_op;
                        }
                        // A NULL literal (NA-typed operand) negates to NULL — three-valued
                        // logic, never an operator error.
                        uint64_t count = chunk.size();
                        vector::vector_t computed(resource,
                                                  types::complex_logical_type(types::logical_type::BIGINT),
                                                  0);
                        if (operand_type(inner_op.value()) == types::logical_type::NA) {
                            uint64_t out_count = count > 0 ? count : 1;
                            computed = vector::vector_t(resource,
                                                        types::complex_logical_type(types::logical_type::NA),
                                                        out_count);
                            computed.validity().set_all_invalid(out_count);
                        } else if (!types::is_arithmetic_numeric(operand_type(inner_op.value()))) {
                            return unsupported_unary_minus_error(resource);
                        } else if (inner_op.value().vec) {
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
                        result.vec = temp_vecs.back().get();
                        return result;
                    }

                    vector::arithmetic_op op;
                    if (!scalar_to_arithmetic_op(scalar_expr->type(), op)) {
                        return core::error_t(core::error_code_t::arithmetics_failure,
                                             std::pmr::string{"Not an arithmetic scalar_type", resource});
                    }
                    auto& operands = scalar_expr->params();
                    if (operands.size() < 2) {
                        return core::error_t(
                            core::error_code_t::arithmetics_failure,
                            std::pmr::string{"Arithmetic expression requires at least 2 operands", resource});
                    }

                    std::deque<vector::data_chunk_t::at_aligned_t> sub_temps;
                    auto left_op = resolve_operand(operands[0], chunk, params, resource, sub_temps, session_tz);
                    if (left_op.has_error()) {
                        return left_op;
                    }
                    auto right_op = resolve_operand(operands[1], chunk, params, resource, sub_temps, session_tz);
                    if (right_op.has_error()) {
                        return right_op;
                    }
                    // A NULL literal answers NULL through the kernels' NA path; only a pair
                    // of concrete types with no operator is an error.
                    if (operand_type(left_op.value()) != types::logical_type::NA &&
                        operand_type(right_op.value()) != types::logical_type::NA &&
                        types::arithmetic_result_type(operand_type(left_op.value()),
                                                      operand_type(right_op.value()),
                                                      op) == types::logical_type::NA) {
                        return unsupported_arithmetic_error(resource);
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
                    result.vec = temp_vecs.back().get();
                    return result;
                }
                return core::error_t(core::error_code_t::arithmetics_failure,
                                     std::pmr::string{"Unsupported expression type in arithmetic operand", resource});
            }
        }

        core::result_wrapper_t<types::logical_value_t> resolve_row_value(std::pmr::memory_resource* resource,
                                                                         const expressions::param_storage& param,
                                                                         const vector::data_chunk_t& chunk,
                                                                         const logical_plan::storage_parameters& params,
                                                                         size_t row_idx,
                                                                         core::date::timezone_offset_t session_tz) {
            // L1: per-row CASE arithmetic still boxes operands into logical_value_t and uses
            // logical_value_t::sum/subtract/mult/divide/modulus below. No typed scalar arithmetic
            // path exists for a single logical_value_t pair, so left as-is.
            // TODO(L1): provide a typed per-row scalar arithmetic helper to avoid logical_value_t round-trips.
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                assert(!key.path().empty() && "key path must be resolved before execution");
                return chunk.value(key.path(), row_idx);
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
                                auto matched =
                                    evaluate_row_condition(resource, cond_expr, chunk, params, row_idx, session_tz);
                                if (matched.has_error()) {
                                    return matched.convert_error<types::logical_value_t>();
                                }
                                if (types::selects(matched.value())) {
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
                            return core::error_t(core::error_code_t::arithmetics_failure,
                                                 std::pmr::string{"CASE: unary minus requires 1 operand", resource});
                        }
                        auto inner =
                            resolve_row_value(resource, scalar->params()[0], chunk, params, row_idx, session_tz);
                        if (inner.has_error()) {
                            return inner;
                        }
                        if (!inner.value().is_null() &&
                            !types::is_arithmetic_numeric(inner.value().type().type())) {
                            return unsupported_unary_minus_error(resource);
                        }
                        return types::logical_value_t::subtract(types::logical_value_t(resource, int64_t(0)),
                                                                inner.value());
                    }
                    // Arithmetic sub-expression
                    if (scalar->params().size() < 2) {
                        return core::error_t(
                            core::error_code_t::arithmetics_failure,
                            std::pmr::string{"CASE: arithmetic sub-expression requires 2 operands", resource});
                    }
                    auto l = resolve_row_value(resource, scalar->params()[0], chunk, params, row_idx, session_tz);
                    if (l.has_error()) {
                        return l;
                    }
                    auto r = resolve_row_value(resource, scalar->params()[1], chunk, params, row_idx, session_tz);
                    if (r.has_error()) {
                        return r;
                    }
                    vector::arithmetic_op arithmetic_op;
                    if (!scalar_to_arithmetic_op(scalar->type(), arithmetic_op)) {
                        return unsupported_arithmetic_error(resource);
                    }
                    if (!l.value().is_null() && !r.value().is_null() &&
                        types::arithmetic_result_type(l.value().type().type(),
                                                      r.value().type().type(),
                                                      arithmetic_op) == types::logical_type::NA) {
                        return unsupported_arithmetic_error(resource);
                    }
                    switch (scalar->type()) {
                        case expressions::scalar_type::add:
                            return types::logical_value_t::sum(l.value(), r.value());
                        case expressions::scalar_type::subtract:
                            return types::logical_value_t::subtract(l.value(), r.value());
                        case expressions::scalar_type::multiply:
                            return types::logical_value_t::mult(l.value(), r.value());
                        case expressions::scalar_type::divide:
                            return types::logical_value_t::divide(l.value(), r.value());
                        case expressions::scalar_type::mod:
                            return types::logical_value_t::modulus(l.value(), r.value());
                        default:
                            break;
                    }
                }
                return core::error_t(core::error_code_t::arithmetics_failure,
                                     std::pmr::string{"CASE: unsupported sub-expression", resource});
            }
        }

        // Evaluate a CASE WHEN condition in SQL three-valued logic. A WHEN fires only when the
        // result is definitely TRUE (the caller applies selects()); UNKNOWN and FALSE fall through.
        core::result_wrapper_t<types::tri_bool_t> evaluate_row_condition(std::pmr::memory_resource* resource,
                                                                         const expressions::expression_ptr& condition,
                                                                         const vector::data_chunk_t& chunk,
                                                                         const logical_plan::storage_parameters& params,
                                                                         size_t row_idx,
                                                                         core::date::timezone_offset_t session_tz) {
            using types::tri_bool_t;
            if (!condition || condition->group() != expressions::expression_group::compare)
                return tri_bool_t::unknown;
            auto* cmp = static_cast<const expressions::compare_expression_t*>(condition.get());
            const auto op = cmp->type();

            // Boolean combinators fold with three-valued logic. Crucially a NOT (union_not) NEGATES
            // its child rather than behaving like an OR, and UNKNOWN under NOT stays UNKNOWN -- so a
            // NULL operand under NOT does not resurrect a match.
            if (cmp->is_union()) {
                switch (cmp->type()) {
                    case expressions::compare_type::union_and: {
                        tri_bool_t acc = tri_bool_t::yes;
                        for (const auto& child : cmp->children()) {
                            auto r = evaluate_row_condition(resource, child, chunk, params, row_idx, session_tz);
                            if (r.has_error())
                                return r;
                            acc = types::tri_and(acc, r.value());
                            if (acc == tri_bool_t::no)
                                return acc;
                        }
                        return acc;
                    }
                    case expressions::compare_type::union_or: {
                        tri_bool_t acc = tri_bool_t::no;
                        for (const auto& child : cmp->children()) {
                            auto r = evaluate_row_condition(resource, child, chunk, params, row_idx, session_tz);
                            if (r.has_error())
                                return r;
                            acc = types::tri_or(acc, r.value());
                            if (acc == tri_bool_t::yes)
                                return acc;
                        }
                        return acc;
                    }
                    case expressions::compare_type::union_not: {
                        if (cmp->children().empty())
                            return tri_bool_t::unknown;
                        auto r = evaluate_row_condition(resource,
                                                        cmp->children().front(),
                                                        chunk,
                                                        params,
                                                        row_idx,
                                                        session_tz);
                        if (r.has_error())
                            return r;
                        return types::tri_not(r.value());
                    }
                    default:
                        return tri_bool_t::unknown;
                }
            }

            // IS NULL / IS NOT NULL: only the left operand's null-ness matters (right is a dummy param), so
            // this must run BEFORE the NULL-operand short-circuit below (a NULL subject is the point here).
            if (op == expressions::compare_type::is_null || op == expressions::compare_type::is_not_null) {
                auto subject = resolve_row_value(resource, cmp->left(), chunk, params, row_idx, session_tz);
                if (subject.has_error())
                    return subject.convert_error<types::tri_bool_t>();
                const bool subject_is_null = subject.value().is_null();
                return types::tri_of(op == expressions::compare_type::is_null ? subject_is_null : !subject_is_null);
            }

            auto left_rw = resolve_row_value(resource, cmp->left(), chunk, params, row_idx, session_tz);
            if (left_rw.has_error())
                return left_rw.convert_error<types::tri_bool_t>();
            auto right_rw = resolve_row_value(resource, cmp->right(), chunk, params, row_idx, session_tz);
            if (right_rw.has_error())
                return right_rw.convert_error<types::tri_bool_t>();
            auto left_val = std::move(left_rw.value());
            auto right_val = std::move(right_rw.value());
            // A NULL operand makes the comparison UNKNOWN (three-valued logic, mirroring
            // simple_predicate). Short-circuit before the type-coercion below: a NULL carries the
            // NA type, and casting to/from NA is a conversion_failure -- a CASE-WHEN condition
            // over a nullable column must fall through, not error.
            if (left_val.is_null() || right_val.is_null()) {
                return tri_bool_t::unknown;
            }

            // LIKE / ILIKE: the pattern (right) is already like_to_regex-converted on the scalar path, so
            // compile it as a regex (case-insensitively for ILIKE) and partial-match the subject. NOT LIKE
            // reaches here inside a union_not wrapper, so only the positive match is performed.
            if (op == expressions::compare_type::regex) {
                if (!types::is_string(left_val.type().type()) || !types::is_string(right_val.type().type())) {
                    return core::error_t{core::error_code_t::comparison_failure,
                                         std::pmr::string{"incorrect argument type for LIKE in CASE", resource}};
                }
                auto compiled =
                    core::regex_t::compile(resource, right_val.value<std::string_view>(), cmp->regex_icase());
                if (compiled.has_error())
                    return compiled.error();
                bool matched = compiled.value().match(left_val.value<std::string_view>());
                if (cmp->regex_negate())
                    matched = !matched;
                return types::tri_of(matched);
            }

            if (left_val.type() != right_val.type()) {
                auto cast_right = right_val.cast_as(left_val.type(), session_tz);
                if (cast_right.has_error()) {
                    return cast_right.convert_error<types::tri_bool_t>();
                }
                if (!cast_right.value().is_null()) {
                    right_val = std::move(cast_right.value());
                } else {
                    auto cast_left = left_val.cast_as(right_val.type(), session_tz);
                    if (cast_left.has_error()) {
                        return cast_left.convert_error<types::tri_bool_t>();
                    }
                    if (!cast_left.value().is_null()) {
                        left_val = std::move(cast_left.value());
                    }
                }
            }
            switch (cmp->type()) {
                case expressions::compare_type::gt:
                case expressions::compare_type::gte:
                case expressions::compare_type::lt:
                case expressions::compare_type::lte:
                case expressions::compare_type::eq:
                case expressions::compare_type::ne:
                    return eval_compare_3vl(cmp->type(), left_val, right_val);
                default:
                    return tri_bool_t::unknown;
            }
        }

        // Best-effort STATIC type of a CASE branch (a THEN or ELSE operand), resolved from the
        // chunk's column types / bound parameters -- WITHOUT evaluating any row. Needed to type the
        // output vector when NO row produces a value (an all-NULL CASE column must still carry the
        // branch's type; the unsized NA sentinel would bad_alloc on vector construction). Returns NA
        // only when the type genuinely cannot be determined.
        static types::complex_logical_type branch_static_type(const expressions::param_storage& param,
                                                              const vector::data_chunk_t& chunk,
                                                              const logical_plan::storage_parameters& params) {
            using types::complex_logical_type;
            using types::logical_type;
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                if (!key.path().empty()) {
                    if (auto* vec = chunk.at(key.path())) {
                        return vec->type();
                    }
                }
                return complex_logical_type(logical_type::NA);
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto it = params.parameters.find(std::get<core::parameter_id_t>(param));
                if (it != params.parameters.end()) {
                    return it->second.type();
                }
                return complex_logical_type(logical_type::NA);
            }
            auto& expr_ptr = std::get<expressions::expression_ptr>(param);
            if (expr_ptr && expr_ptr->group() == expressions::expression_group::scalar) {
                auto* scalar = static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());
                if (scalar->type() == expressions::scalar_type::case_expr) {
                    // Nested CASE: take the first branch (THEN, then ELSE) whose type is knowable.
                    auto& ops = scalar->params();
                    bool nested_default = (ops.size() % 2 == 1);
                    size_t nested_whens = ops.size() / 2;
                    for (size_t w = 0; w < nested_whens; ++w) {
                        auto t = branch_static_type(ops[w * 2 + 1], chunk, params);
                        if (t.type() != logical_type::NA) {
                            return t;
                        }
                    }
                    if (nested_default && !ops.empty()) {
                        auto t = branch_static_type(ops.back(), chunk, params);
                        if (t.type() != logical_type::NA) {
                            return t;
                        }
                    }
                    return complex_logical_type(logical_type::NA);
                }
                // unary_minus preserves its operand's type.
                if (scalar->type() == expressions::scalar_type::unary_minus && !scalar->params().empty()) {
                    auto t = branch_static_type(scalar->params()[0], chunk, params);
                    if (t.type() != logical_type::NA) {
                        return t;
                    }
                }
                // Binary arithmetic (add/subtract/multiply/divide/mod): the result type follows the
                // SAME numeric-promotion / temporal rules the arithmetic kernels apply
                // (types::arithmetic_result_type), so e.g. INT * DOUBLE resolves to DOUBLE rather
                // than the first operand's INT.
                vector::arithmetic_op aop;
                if (scalar_to_arithmetic_op(scalar->type(), aop) && scalar->params().size() >= 2) {
                    auto lt = branch_static_type(scalar->params()[0], chunk, params);
                    auto rt = branch_static_type(scalar->params()[1], chunk, params);
                    auto res = types::arithmetic_result_type(lt.type(), rt.type(), aop);
                    if (res != logical_type::NA) {
                        return complex_logical_type(res);
                    }
                }
                // Fallback for any other scalar shape: the first typeable operand, else BIGINT (the
                // arithmetic evaluator's own default).
                if (!scalar->params().empty()) {
                    auto t = branch_static_type(scalar->params()[0], chunk, params);
                    if (t.type() != logical_type::NA) {
                        return t;
                    }
                }
                return complex_logical_type(logical_type::BIGINT);
            }
            return complex_logical_type(logical_type::NA);
        }

        core::result_wrapper_t<vector::vector_t>
        evaluate_case_expr(std::pmr::memory_resource* resource,
                           const std::pmr::vector<expressions::param_storage>& operands,
                           vector::data_chunk_t& chunk,
                           const logical_plan::storage_parameters& params,
                           core::date::timezone_offset_t session_tz) {
            uint64_t count = chunk.size();
            bool has_default = (operands.size() % 2 == 1);
            size_t num_whens = operands.size() / 2;

            // The CASE output type is the COMMON type across every THEN result and the ELSE, so a
            // wider later branch (e.g. a BIGINT ELSE after an INT THEN) is not truncated -- this must
            // match the planner's own CASE type resolution (validate_logical_plan), or the aggregate/
            // projection column it feeds gets a plan type that the runtime value cannot set_value into.
            // Each branch's type is taken STATICALLY (branch_static_type: column / parameter / nested
            // CASE / arithmetic promotion, no row-0 evaluation), so the type is well-defined even for
            // an all-NULL or empty input that would otherwise bad_alloc on an unsized NA-typed vector.
            types::complex_logical_type result_type(types::logical_type::NA);
            auto fold_branch = [&](const types::complex_logical_type& branch_type) {
                if (branch_type.type() == types::logical_type::NA ||
                    branch_type.type() == types::logical_type::UNKNOWN) {
                    return;
                }
                if (result_type.type() == types::logical_type::NA) {
                    result_type = branch_type;
                } else if (result_type.type() != branch_type.type() && types::is_numeric(result_type.type()) &&
                           types::is_numeric(branch_type.type())) {
                    auto promoted = types::promote_type(result_type.type(), branch_type.type());
                    if (promoted != types::logical_type::NA) {
                        result_type = types::complex_logical_type(promoted);
                    }
                }
            };
            for (size_t w = 0; w < num_whens; ++w) {
                fold_branch(branch_static_type(operands[w * 2 + 1], chunk, params));
            }
            if (has_default) {
                fold_branch(branch_static_type(operands.back(), chunk, params));
            }

            // First pass: compute each row's result value.
            std::pmr::vector<types::logical_value_t> results(resource);
            results.reserve(count);
            for (uint64_t i = 0; i < count; i++) {
                types::logical_value_t row_val(resource, types::complex_logical_type{types::logical_type::NA});
                bool matched = false;
                for (size_t w = 0; w < num_whens; w++) {
                    auto& cond_param = operands[w * 2];
                    if (std::holds_alternative<expressions::expression_ptr>(cond_param)) {
                        auto& cond_expr = std::get<expressions::expression_ptr>(cond_param);
                        auto cond = evaluate_row_condition(resource, cond_expr, chunk, params, i, session_tz);
                        if (cond.has_error()) {
                            return cond.convert_error<vector::vector_t>();
                        }
                        // A WHEN fires only on a definite TRUE; UNKNOWN / FALSE fall through.
                        if (types::selects(cond.value())) {
                            auto resolved =
                                resolve_row_value(resource, operands[w * 2 + 1], chunk, params, i, session_tz);
                            if (resolved.has_error()) {
                                return resolved.convert_error<vector::vector_t>();
                            }
                            row_val = std::move(resolved.value());
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched && has_default) {
                    auto resolved = resolve_row_value(resource, operands.back(), chunk, params, i, session_tz);
                    if (resolved.has_error()) {
                        return resolved.convert_error<vector::vector_t>();
                    }
                    row_val = std::move(resolved.value());
                }
                // Refinement: if the branches were statically untypeable (NA), learn the type from
                // the first row that produces one. A typed NULL (e.g. a NULL read from a BIGINT
                // column) still carries its type, so learn from row_val.type() rather than gating on
                // is_null().
                if (result_type.type() == types::logical_type::NA && row_val.type().type() != types::logical_type::NA) {
                    result_type = row_val.type();
                }
                results.emplace_back(std::move(row_val));
            }

            // Last resort (e.g. a bare `THEN NULL` over an empty/all-NULL set): a concrete,
            // allocatable, nullable type so vector construction never faults on the unsized NA
            // sentinel. Every value is NULL here, so the choice is immaterial to results.
            if (result_type.type() == types::logical_type::NA) {
                result_type = types::complex_logical_type(types::logical_type::BIGINT);
            }

            // Second pass: build the typed output, coercing each value to the common result type.
            vector::vector_t output(resource, result_type, count);
            for (uint64_t i = 0; i < count; i++) {
                if (results[i].is_null()) {
                    output.set_null(i, true);
                    continue;
                }
                if (results[i].type() != result_type) {
                    auto casted = results[i].cast_as(result_type, session_tz);
                    // No error channel would help here (the row already produced a value); a
                    // non-castable pair keeps the uncoerced value instead of aborting.
                    if (!casted.has_error() && !casted.value().is_null()) {
                        output.set_value(i, std::move(casted.value()));
                    } else {
                        output.set_value(i, results[i]);
                    }
                } else {
                    output.set_value(i, results[i]);
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
            std::deque<vector::data_chunk_t::at_aligned_t> temp_vecs;
            auto operand_res = detail::resolve_operand(operands[0], chunk, params, resource, temp_vecs, session_tz);
            if (operand_res.has_error()) {
                return operand_res.convert_error<vector::vector_t>();
            }
            uint64_t count = chunk.size();
            if (detail::operand_type(operand_res.value()) == types::logical_type::NA) {
                uint64_t out_count = count > 0 ? count : 1;
                vector::vector_t nulls(resource, types::complex_logical_type(types::logical_type::NA), out_count);
                nulls.validity().set_all_invalid(out_count);
                return nulls;
            }
            if (!types::is_arithmetic_numeric(detail::operand_type(operand_res.value()))) {
                return detail::unsupported_unary_minus_error(resource);
            }
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

        std::deque<vector::data_chunk_t::at_aligned_t> temp_vecs;

        auto left_op = detail::resolve_operand(operands[0], chunk, params, resource, temp_vecs, session_tz);
        if (left_op.has_error()) {
            return left_op.convert_error<vector::vector_t>();
        }
        auto right_op = detail::resolve_operand(operands[1], chunk, params, resource, temp_vecs, session_tz);
        if (right_op.has_error()) {
            return right_op.convert_error<vector::vector_t>();
        }

        uint64_t count = chunk.size();
        vector::arithmetic_op arith_op;
        if (!detail::scalar_to_arithmetic_op(op, arith_op)) {
            return core::error_t(core::error_code_t::arithmetics_failure,
                                 std::pmr::string{"Not an arithmetic scalar_type", resource});
        }
        if (detail::operand_type(left_op.value()) != types::logical_type::NA &&
            detail::operand_type(right_op.value()) != types::logical_type::NA &&
            types::arithmetic_result_type(detail::operand_type(left_op.value()),
                                          detail::operand_type(right_op.value()),
                                          arith_op) == types::logical_type::NA) {
            return detail::unsupported_arithmetic_error(resource);
        }

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
