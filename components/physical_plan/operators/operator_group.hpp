#pragma once

#include "transformation.hpp"

#include <components/expressions/expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/operators/get/operator_get.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    struct group_key_t {
        std::pmr::string name;
        get::operator_get_ptr getter;
    };

    struct group_value_t {
        std::pmr::string name;
        aggregate::operator_aggregate_ptr aggregator;
    };

    // Pre-group computed column (arithmetic on raw data before grouping)
    struct computed_column_t {
        std::pmr::string alias;
        expressions::scalar_type op;
        std::pmr::vector<expressions::param_storage> operands;
    };

    // Post-aggregate computed column (arithmetic on aggregate results)
    struct post_aggregate_column_t {
        std::pmr::string alias;
        expressions::scalar_type op;
        std::pmr::vector<expressions::param_storage> operands;
    };

    class operator_group_t final : public read_write_operator_t {
    public:
        operator_group_t(std::pmr::memory_resource* resource, log_t log,
                         expressions::expression_ptr having = nullptr);

        void add_key(const std::pmr::string& name, get::operator_get_ptr&& getter);
        void add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator);
        void add_computed_column(computed_column_t&& col);
        void add_post_aggregate(post_aggregate_column_t&& col);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<computed_column_t> computed_columns_;
        std::pmr::vector<post_aggregate_column_t> post_aggregates_;
        expressions::expression_ptr having_;
        std::pmr::vector<impl::value_matrix_t> inputs_;
        std::pmr::vector<types::complex_logical_type> result_types_;
        impl::value_matrix_t transposed_output_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        void create_list_rows();
        void calc_aggregate_values(pipeline::context_t* pipeline_context);
        void calc_post_aggregates(pipeline::context_t* pipeline_context);
        void filter_having(pipeline::context_t* pipeline_context);
    };

} // namespace components::operators