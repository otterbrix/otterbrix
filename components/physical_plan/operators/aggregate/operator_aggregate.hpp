#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        void set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view key) const;
        types::logical_value_t value() const;

    protected:
        operator_aggregate_t(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name);

        types::logical_value_t aggregate_result_{std::pmr::null_memory_resource(), types::complex_logical_type{types::logical_type::NA}};

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        virtual types::logical_value_t aggregate_impl() = 0;
        virtual std::string key_impl() const = 0;
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace components::operators::aggregate