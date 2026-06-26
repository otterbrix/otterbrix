#include "operator_aggregate.hpp"

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate) {}

    void operator_aggregate_t::run_aggregation(pipeline::context_t* pipeline_context) {
        if (auto res = aggregate_batch_impl(pipeline_context); res.has_error()) {
            set_error(res.error());
        } else {
            batch_results_ = std::move(res.value());
        }
    }

    void operator_aggregate_t::compute(pipeline::context_t* ctx, const operator_ptr& source_batch) {
        clear();
        set_children(source_batch);
        run_aggregation(ctx);
    }

    compute::datum_t operator_aggregate_t::take_batch_values() { return std::move(batch_results_); }
} // namespace components::operators::aggregate
