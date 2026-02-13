#pragma once

#include "transformation.hpp"

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/operators/get/operator_get.hpp>

namespace components::operators {

    struct group_key_t {
        std::pmr::string name;
        get::operator_get_ptr getter;
    };

    struct group_value_t {
        std::pmr::string name;
        aggregate::operator_aggregate_ptr aggregator;
    };

    class operator_group_t final : public read_write_operator_t {
    public:
        operator_group_t(std::pmr::memory_resource* resource, log_t log);

        void add_key(const std::pmr::string& name, get::operator_get_ptr&& getter);
        void add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<impl::value_matrix_t> inputs_;
        std::pmr::vector<types::complex_logical_type> result_types_;
        impl::value_matrix_t transposed_output_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        void create_list_rows();
        void calc_aggregate_values(pipeline::context_t* pipeline_context);
    };

} // namespace components::operators