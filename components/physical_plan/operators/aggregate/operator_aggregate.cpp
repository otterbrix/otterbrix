#include "operator_aggregate.hpp"

#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->type() == operator_type::batch) {
            batch_results_ = aggregate_batch_impl(pipeline_context);
        } else {
            aggregate_result_ = aggregate_impl(pipeline_context);
        }
    }

    compute::datum_t operator_aggregate_t::aggregate_batch_impl(pipeline::context_t* pipeline_context) {
        auto* batch = static_cast<operator_batch_t*>(left_.get());
        auto& chunks = batch->chunks();
        std::pmr::vector<types::logical_value_t> results(resource_);
        results.reserve(chunks.size());
        for (size_t i = 0; i < chunks.size(); ++i) {
            auto data = make_operator_data(resource_, std::move(chunks[i]));
            set_children(boost::intrusive_ptr<operator_t>(new operator_empty_t(resource_, std::move(data))));
            aggregate_result_ = aggregate_impl(pipeline_context);
            results.push_back(aggregate_result_);
        }
        return compute::datum_t{std::move(results)};
    }

    void operator_aggregate_t::set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view alias) const {
        auto res_it = std::find_if(row.begin(), row.end(), [&](const types::logical_value_t& v) {
            return !v.type().extension() ? false : v.type().alias() == alias;
        });
        if (res_it == row.end()) {
            row.emplace_back(aggregate_result_);
            row.back().set_alias(std::string(alias));
        } else {
            *res_it = aggregate_result_;
            res_it->set_alias(std::string(alias));
        }
    }

    types::logical_value_t operator_aggregate_t::value() const { return aggregate_result_; }
    compute::datum_t operator_aggregate_t::take_batch_values() { return std::move(batch_results_); }
} // namespace components::operators::aggregate
