#include "case_when_value.hpp"

#include <expressions/compare_expression.hpp>

namespace components::operators::get {

    operator_get_ptr case_when_value_t::create(const std::pmr::vector<expressions::param_storage>& params,
                                               const logical_plan::storage_parameters* storage_params) {
        std::vector<expressions::key_t> result_keys;
        std::vector<types::logical_value_t> result_constants;
        std::vector<result_expression_t> result_expressions;
        std::vector<when_clause> clauses;
        else_kind e_type = else_kind::null_value;
        size_t e_index = 0;

        auto extract_result = [&](size_t idx, when_clause::result_kind& r_type, size_t& r_index) {
            if (std::holds_alternative<expressions::key_t>(params[idx])) {
                r_type = when_clause::result_kind::key;
                r_index = result_keys.size();
                result_keys.push_back(std::get<expressions::key_t>(params[idx]));
            } else if (std::holds_alternative<core::parameter_id_t>(params[idx])) {
                r_type = when_clause::result_kind::constant;
                r_index = result_constants.size();
                result_constants.push_back(storage_params->parameters.at(std::get<core::parameter_id_t>(params[idx])));
            } else if (std::holds_alternative<expressions::expression_ptr>(params[idx])) {
                auto& expr_ptr = std::get<expressions::expression_ptr>(params[idx]);
                if (expr_ptr->group() == expressions::expression_group::scalar) {
                    auto* scalar = static_cast<const expressions::scalar_expression_t*>(expr_ptr.get());
                    r_type = when_clause::result_kind::expression;
                    r_index = result_expressions.size();
                    result_expressions.push_back(
                        {scalar->type(),
                         std::vector<expressions::param_storage>(scalar->params().begin(), scalar->params().end())});
                }
            }
        };

        // Params layout: [condition_expr, result, condition_expr, result, ..., else_result]
        // Each condition_expr is an expression_ptr (compare_expression_t)
        // Each result is key_t, parameter_id_t, or expression_ptr (arithmetic)

        size_t i = 0;
        while (i + 1 < params.size()) {
            // condition
            if (!std::holds_alternative<expressions::expression_ptr>(params[i])) {
                break;
            }
            auto& cond_expr_ptr = std::get<expressions::expression_ptr>(params[i]);
            if (cond_expr_ptr->group() != expressions::expression_group::compare) {
                break;
            }
            auto* cond = static_cast<const expressions::compare_expression_t*>(cond_expr_ptr.get());
            const auto& cond_key = std::get<components::expressions::key_t>(cond->left());
            auto cond_cmp = cond->type();
            auto cond_val =
                (cond_cmp == expressions::compare_type::is_null || cond_cmp == expressions::compare_type::is_not_null)
                    ? types::logical_value_t(std::pmr::get_default_resource(),
                                             types::complex_logical_type{types::logical_type::NA})
                    : storage_params->parameters.at(std::get<core::parameter_id_t>(cond->right()));

            // result
            ++i;
            when_clause::result_kind r_type = when_clause::result_kind::constant;
            size_t r_index = 0;
            extract_result(i, r_type, r_index);

            clauses.push_back({std::move(cond_key), cond_cmp, std::move(cond_val), r_type, r_index});
            ++i;
        }

        // else result (last param if odd count)
        if (i < params.size()) {
            when_clause::result_kind r_type = when_clause::result_kind::constant;
            size_t r_index = 0;
            extract_result(i, r_type, r_index);
            switch (r_type) {
                case when_clause::result_kind::key:
                    e_type = else_kind::key;
                    e_index = r_index;
                    break;
                case when_clause::result_kind::constant:
                    e_type = else_kind::constant;
                    e_index = r_index;
                    break;
                case when_clause::result_kind::expression:
                    e_type = else_kind::expression;
                    e_index = r_index;
                    break;
            }
        }

        return operator_get_ptr(new case_when_value_t(std::move(result_keys),
                                                      std::move(result_constants),
                                                      std::move(result_expressions),
                                                      std::move(clauses),
                                                      e_type,
                                                      e_index,
                                                      storage_params));
    }

    case_when_value_t::case_when_value_t(std::vector<expressions::key_t> result_keys,
                                         std::vector<types::logical_value_t> result_constants,
                                         std::vector<result_expression_t> result_expressions,
                                         std::vector<when_clause> clauses,
                                         else_kind else_type,
                                         size_t else_index,
                                         const logical_plan::storage_parameters* storage_params)
        : operator_get_t()
        , result_keys_(std::move(result_keys))
        , result_constants_(std::move(result_constants))
        , result_expressions_(std::move(result_expressions))
        , clauses_(std::move(clauses))
        , else_type_(else_type)
        , else_index_(else_index)
        , storage_params_(storage_params) {}

    types::logical_value_t case_when_value_t::lookup_column(const expressions::key_t& key,
                                                            const std::pmr::vector<types::logical_value_t>& row) const {
        auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
        const auto& path = key.path();
        if (path.empty() || path[0] >= row.size()) {
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }
        auto* val = &row[path[0]];
        for (size_t i = 1; i < path.size(); i++) {
            if (path[i] >= val->children().size()) {
                return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
            }
            val = &val->children()[path[i]];
        }
        return *val;
    }

    types::logical_value_t case_when_value_t::evaluate_expression(
        const result_expression_t& expr,
        const std::pmr::vector<types::logical_value_t>& row) const {
        auto resolve_param = [&](const expressions::param_storage& p) -> types::logical_value_t {
            if (std::holds_alternative<expressions::key_t>(p)) {
                return lookup_column(std::get<expressions::key_t>(p), row);
            } else if (std::holds_alternative<core::parameter_id_t>(p)) {
                return storage_params_->parameters.at(std::get<core::parameter_id_t>(p));
            }
            auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        };

        if (expr.params.size() < 2) {
            auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }

        auto lhs = resolve_param(expr.params[0]);
        auto rhs = resolve_param(expr.params[1]);

        switch (expr.op) {
            case expressions::scalar_type::add:
                return types::logical_value_t::sum(lhs, rhs);
            case expressions::scalar_type::subtract:
                return types::logical_value_t::subtract(lhs, rhs);
            case expressions::scalar_type::multiply:
                return types::logical_value_t::mult(lhs, rhs);
            case expressions::scalar_type::divide:
                return types::logical_value_t::divide(lhs, rhs);
            case expressions::scalar_type::mod:
                return types::logical_value_t::modulus(lhs, rhs);
            default: {
                auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
                return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
            }
        }
    }

    types::logical_value_t case_when_value_t::get_result(when_clause::result_kind kind,
                                                         size_t index,
                                                         const std::pmr::vector<types::logical_value_t>& row) const {
        if (kind == when_clause::result_kind::key) {
            return lookup_column(result_keys_[index], row);
        } else if (kind == when_clause::result_kind::expression) {
            return evaluate_expression(result_expressions_[index], row);
        } else {
            return result_constants_[index];
        }
    }

    bool case_when_value_t::evaluate_condition(const when_clause& clause,
                                               const std::pmr::vector<types::logical_value_t>& row) const {
        auto col_val = lookup_column(clause.condition_key, row);
        bool is_null = col_val.type().type() == types::logical_type::NA;

        switch (clause.condition_cmp) {
            case expressions::compare_type::is_null:
                return is_null;
            case expressions::compare_type::is_not_null:
                return !is_null;
            case expressions::compare_type::eq:
                return !is_null && col_val == clause.condition_value;
            case expressions::compare_type::ne:
                return !is_null && !(col_val == clause.condition_value);
            case expressions::compare_type::gt:
                return !is_null && col_val > clause.condition_value;
            case expressions::compare_type::lt:
                return !is_null && col_val < clause.condition_value;
            case expressions::compare_type::gte:
                return !is_null && col_val >= clause.condition_value;
            case expressions::compare_type::lte:
                return !is_null && col_val <= clause.condition_value;
            default:
                return false;
        }
    }

    std::vector<types::logical_value_t>
    case_when_value_t::get_values_impl(const std::pmr::vector<types::logical_value_t>& row) {
        for (const auto& clause : clauses_) {
            if (evaluate_condition(clause, row)) {
                return {get_result(clause.res_type, clause.res_index, row)};
            }
        }
        // ELSE
        switch (else_type_) {
            case else_kind::key:
                return {lookup_column(result_keys_[else_index_], row)};
            case else_kind::constant:
                return {result_constants_[else_index_]};
            case else_kind::expression:
                return {evaluate_expression(result_expressions_[else_index_], row)};
            case else_kind::null_value:
            default: {
                auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
                return {types::logical_value_t(resource, types::logical_type::NA)};
            }
        }
    }

} // namespace components::operators::get
