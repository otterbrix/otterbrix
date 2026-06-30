#pragma once

#include <components/compute/compute_kernel.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        void set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view key) const;
        types::logical_value_t value() const;
        compute::datum_t take_batch_values();

        // Public accessor for the aggregate kind (e.g. "sum", "count", "min",
        // "max", "avg"). Used by commutative partition-merge logic in
        // operator_external_group_t to decide how to combine partials across partitions.
        std::string aggregate_key() const { return key_impl(); }

    protected:
        operator_aggregate_t(std::pmr::memory_resource* resource, log_t log);

        types::logical_value_t aggregate_result_;
        compute::datum_t batch_results_{std::pmr::vector<types::logical_value_t>{resource_}};

        virtual core::result_wrapper_t<types::logical_value_t>
        aggregate_impl(pipeline::context_t* pipeline_context) = 0;
        virtual core::result_wrapper_t<compute::datum_t> aggregate_batch_impl(pipeline::context_t* pipeline_context);
        virtual std::string key_impl() const = 0;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace components::operators::aggregate