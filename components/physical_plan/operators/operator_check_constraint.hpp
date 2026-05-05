#pragma once

#include <components/catalog/constraint_evaluator.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <vector>

namespace components::operators {

    // Checks NOT NULL columns and evaluates CHECK constraint predicates.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        struct check_entry_t {
            components::catalog::row_predicate_fn predicate;
            std::string                           conexpr;
        };

        operator_check_constraint_t(std::pmr::memory_resource*  resource,
                                     log_t                       log,
                                     std::vector<std::string>    not_null_columns,
                                     std::vector<check_entry_t>  checks);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<std::string>   not_null_columns_;
        std::vector<check_entry_t> checks_;
    };

} // namespace components::operators
