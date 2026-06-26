#pragma once

#include <components/compute/compute_kernel.hpp>
#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        compute::datum_t take_batch_values();

        // Synchronous per-group aggregation entry point for operator_group_t.
        // operator_func_t aggregation is pure-CPU (no await / cross-actor send):
        // this clears prior state, wires `source_batch` as the input child
        // (an already-executed operator_batch_t), and runs the run_aggregation
        // core. Read has_error() / take_batch_values() afterwards.
        void compute(pipeline::context_t* ctx, const operator_ptr& source_batch);

    protected:
        operator_aggregate_t(std::pmr::memory_resource* resource, log_t log);

        types::logical_value_t aggregate_result_;
        compute::datum_t batch_results_{std::pmr::vector<types::logical_value_t>{resource_}};

        virtual core::result_wrapper_t<types::logical_value_t>
        aggregate_impl(pipeline::context_t* pipeline_context) = 0;
        virtual core::result_wrapper_t<compute::datum_t> aggregate_batch_impl(pipeline::context_t* pipeline_context);
        virtual std::string key_impl() const = 0;

    private:
        // Shared dispatch reached by compute(): selects batch vs scalar aggregation
        // depending on the source child's type and stamps batch_results_/
        // aggregate_result_. The source child is already wired as left_ by the caller.
        void run_aggregation(pipeline::context_t* pipeline_context);
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace components::operators::aggregate