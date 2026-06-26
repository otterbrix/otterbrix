#include "operator_aggregate.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/key.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate)
        , aggregate_result_(resource, types::complex_logical_type{types::logical_type::NA}) {}

    void operator_aggregate_t::run_aggregation(pipeline::context_t* pipeline_context) {
        if (left_ && left_->type() == operator_type::batch) {
            if (auto res = aggregate_batch_impl(pipeline_context); res.has_error()) {
                set_error(res.error());
            } else {
                batch_results_ = std::move(res.value());
            }
        } else {
            if (auto res = aggregate_impl(pipeline_context); res.has_error()) {
                set_error(res.error());
            } else {
                aggregate_result_ = std::move(res.value());
            }
        }
    }

    void operator_aggregate_t::compute(pipeline::context_t* ctx, const operator_ptr& source_batch) {
        clear();
        set_children(source_batch);
        run_aggregation(ctx);
    }

    core::result_wrapper_t<compute::datum_t>
    operator_aggregate_t::aggregate_batch_impl(pipeline::context_t* pipeline_context) {
        auto& chunks = left_->output()->chunks();
        std::pmr::vector<types::logical_value_t> results(resource_);
        results.reserve(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            auto data = make_operator_data(resource_, std::move(chunks[i]));
            set_children(boost::intrusive_ptr<operator_t>(new operator_empty_t(resource_, std::move(data))));
            auto res = aggregate_impl(pipeline_context);
            if (res.has_error()) {
                return res.convert_error<compute::datum_t>();
            }
            aggregate_result_ = std::move(res.value());
            results.push_back(aggregate_result_);
        }
        return compute::datum_t{std::move(results)};
    }

    compute::datum_t operator_aggregate_t::take_batch_values() { return std::move(batch_results_); }
} // namespace components::operators::aggregate
