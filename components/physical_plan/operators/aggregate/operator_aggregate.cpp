#include "operator_aggregate.hpp"

#include <components/physical_plan/operators/operator_empty.hpp>

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        aggregate_result_ = aggregate_impl(pipeline_context);
    }

    void operator_aggregate_t::execute_on(operator_data_ptr data, pipeline::context_t* pipeline_context) {
        if (!left_) {
            left_ = boost::intrusive_ptr(new operator_empty_t(resource_, std::move(data)));
        } else {
            left_->set_output(std::move(data));
        }
        aggregate_result_ = aggregate_impl(pipeline_context);
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
} // namespace components::operators::aggregate
