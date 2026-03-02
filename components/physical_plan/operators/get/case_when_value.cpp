#include "case_when_value.hpp"

#include <expressions/compare_expression.hpp>

namespace components::operators::get {

    operator_get_ptr case_when_value_t::create(const std::pmr::vector<expressions::param_storage>& params,
                                               const logical_plan::storage_parameters* storage_params) {
        std::vector<expressions::key_t> result_keys;
        std::vector<types::logical_value_t> result_constants;
        std::vector<when_clause> clauses;
        else_kind e_type = else_kind::null_value;
        size_t e_index = 0;

        // Params layout: [condition_expr, result, condition_expr, result, ..., else_result]
        // Each condition_expr is an expression_ptr (compare_expression_t)
        // Each result and else_result is either key_t or parameter_id_t

        size_t i = 0;
        while (i + 1 < params.size()) {
            // condition
            if (!std::holds_alternative<expressions::expression_ptr>(params[i])) {
                break;
            }
            auto& cond_expr_ptr = std::get<expressions::expression_ptr>(params[i]);
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
            if (std::holds_alternative<expressions::key_t>(params[i])) {
                r_type = when_clause::result_kind::key;
                r_index = result_keys.size();
                result_keys.push_back(std::get<expressions::key_t>(params[i]));
            } else if (std::holds_alternative<core::parameter_id_t>(params[i])) {
                r_type = when_clause::result_kind::constant;
                r_index = result_constants.size();
                result_constants.push_back(storage_params->parameters.at(std::get<core::parameter_id_t>(params[i])));
            }

            clauses.push_back({std::move(cond_key), cond_cmp, std::move(cond_val), r_type, r_index});
            ++i;
        }

        // else result (last param if odd count)
        if (i < params.size()) {
            if (std::holds_alternative<expressions::key_t>(params[i])) {
                e_type = else_kind::key;
                e_index = result_keys.size();
                result_keys.push_back(std::get<expressions::key_t>(params[i]));
            } else if (std::holds_alternative<core::parameter_id_t>(params[i])) {
                e_type = else_kind::constant;
                e_index = result_constants.size();
                result_constants.push_back(storage_params->parameters.at(std::get<core::parameter_id_t>(params[i])));
            }
        }

        return operator_get_ptr(new case_when_value_t(std::move(result_keys),
                                                      std::move(result_constants),
                                                      std::move(clauses),
                                                      e_type,
                                                      e_index));
    }

    case_when_value_t::case_when_value_t(std::vector<expressions::key_t> result_keys,
                                         std::vector<types::logical_value_t> result_constants,
                                         std::vector<when_clause> clauses,
                                         else_kind else_type,
                                         size_t else_index)
        : operator_get_t()
        , result_keys_(std::move(result_keys))
        , result_constants_(std::move(result_constants))
        , clauses_(std::move(clauses))
        , else_type_(else_type)
        , else_index_(else_index) {}

    types::logical_value_t case_when_value_t::lookup_column(const expressions::key_t& key,
                                                            const std::pmr::vector<types::logical_value_t>& row) const {
        auto* local_values = row.data();
        size_t size = row.size();
        for (size_t i = 0; i < key.storage().size(); i++) {
            auto it = std::find_if(local_values, local_values + size, [&](const types::logical_value_t& value) {
                return core::pmr::operator==(value.type().alias(), key.storage()[i]);
            });
            if (it == local_values + size) {
                auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
                return types::logical_value_t(resource, types::logical_type::NA);
            }
            if (i + 1 != key.storage().size()) {
                local_values = it->children().data();
                size = it->children().size();
            } else {
                return *it;
            }
        }
        auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
        return types::logical_value_t(resource, types::logical_type::NA);
    }

    types::logical_value_t case_when_value_t::get_result(when_clause::result_kind kind,
                                                         size_t index,
                                                         const std::pmr::vector<types::logical_value_t>& row) const {
        if (kind == when_clause::result_kind::key) {
            return lookup_column(result_keys_[index], row);
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
            case else_kind::null_value:
            default: {
                auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
                return {types::logical_value_t(resource, types::logical_type::NA)};
            }
        }
    }

} // namespace components::operators::get
