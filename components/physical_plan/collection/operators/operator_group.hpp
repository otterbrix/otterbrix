#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_aggregate.hpp>
#include <components/physical_plan/collection/operators/get/operator_get.hpp>

namespace components::collection::operators {

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
        explicit operator_group_t(services::collection::context_collection_t* context);
        explicit operator_group_t(std::pmr::memory_resource* resource);

        void add_key(const std::pmr::string& name, get::operator_get_ptr&& getter);
        void add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<base::operators::operator_data_ptr> input_documents_;

        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        void create_list_documents();
        void calc_aggregate_values(pipeline::context_t* pipeline_context);
    };

} // namespace components::collection::operators