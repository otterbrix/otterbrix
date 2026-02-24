#include "operator_aggregate.hpp"

namespace components::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        aggregate_result_ = aggregate_impl(pipeline_context);
    }

    void operator_aggregate_t::set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view alias) const {
        auto res_it = std::find_if(row.begin(), row.end(), [&](const types::logical_value_t& v) {
            return !v.type().extension() ? false : v.type().alias() == alias;
        });
        if (res_it == row.end()) {
            row.emplace_back(aggregate_result_);
        } else {
            *res_it = aggregate_result_;
        }
    }

    types::logical_value_t operator_aggregate_t::value() const { return aggregate_result_; }
} // namespace components::operators::aggregate
