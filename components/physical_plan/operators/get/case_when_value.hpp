#pragma once

#include "operator_get.hpp"

#include <expressions/expression.hpp>
#include <logical_plan/param_storage.hpp>

namespace components::operators::get {

    class case_when_value_t final : public operator_get_t {
    public:
        static operator_get_ptr create(const std::pmr::vector<expressions::param_storage>& params,
                                       const logical_plan::storage_parameters* storage_params);

    private:
        struct when_clause {
            expressions::key_t condition_key;
            expressions::compare_type condition_cmp;
            types::logical_value_t condition_value;

            enum class result_kind { key, constant };
            result_kind res_type;
            size_t res_index;
        };

        std::vector<expressions::key_t> result_keys_;
        std::vector<types::logical_value_t> result_constants_;
        std::vector<when_clause> clauses_;

        enum class else_kind { key, constant, null_value };
        else_kind else_type_{else_kind::null_value};
        size_t else_index_{0};

        case_when_value_t(std::vector<expressions::key_t> result_keys,
                          std::vector<types::logical_value_t> result_constants,
                          std::vector<when_clause> clauses,
                          else_kind else_type,
                          size_t else_index);

        std::vector<types::logical_value_t>
        get_values_impl(const std::pmr::vector<types::logical_value_t>& row) override;

        types::logical_value_t lookup_column(const expressions::key_t& key,
                                             const std::pmr::vector<types::logical_value_t>& row) const;

        types::logical_value_t get_result(when_clause::result_kind kind, size_t index,
                                          const std::pmr::vector<types::logical_value_t>& row) const;

        bool evaluate_condition(const when_clause& clause,
                                const std::pmr::vector<types::logical_value_t>& row) const;
    };

} // namespace components::operators::get
