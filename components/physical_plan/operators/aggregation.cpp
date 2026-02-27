#include "aggregation.hpp"

#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace components::operators {

    aggregation::aggregation(std::pmr::memory_resource* resource, log_t log, collection_full_name_t name)
        : read_only_operator_t(resource, log, operator_type::aggregate)
        , name_(std::move(name)) {}

    void aggregation::set_match(operator_ptr&& match) { match_ = std::move(match); }

    void aggregation::set_group(operator_ptr&& group) { group_ = std::move(group); }

    void aggregation::set_sort(operator_ptr&& sort) { sort_ = std::move(sort); }

    void aggregation::set_having(operator_ptr&& having) { having_ = std::move(having); }

    void aggregation::set_distinct(operator_ptr&& distinct) { distinct_ = std::move(distinct); }

    void aggregation::set_limit(logical_plan::limit_t limit) { limit_ = limit; }

    void aggregation::on_execute_impl(pipeline::context_t*) { take_output(left_); }

    void aggregation::on_prepare_impl() {
        operator_ptr executor = nullptr;
        if (left_) {
            executor = std::move(left_);
            if (match_) {
                match_->set_children(std::move(executor));
                executor = std::move(match_);
            }
        } else {
            executor =
                match_ ? std::move(match_)
                       : static_cast<operator_ptr>(boost::intrusive_ptr(new transfer_scan(resource_, name_, limit_)));
        }
        if (group_) {
            group_->set_children(std::move(executor));
            executor = std::move(group_);
        }
        if (having_) {
            having_->set_children(std::move(executor));
            executor = std::move(having_);
        }
        if (sort_) {
            sort_->set_children(std::move(executor));
            executor = std::move(sort_);
        }
        if (distinct_) {
            distinct_->set_children(std::move(executor));
            executor = std::move(distinct_);
        }
        set_children(std::move(executor));
    }

} // namespace components::operators
