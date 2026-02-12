#include "aggregation.hpp"

#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace components::operators {

    aggregation::aggregation(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name)
        : read_only_operator_t(resource, log, std::move(name), operator_type::aggregate) {}

    void aggregation::set_match(operator_ptr&& match) { match_ = std::move(match); }

    void aggregation::set_group(operator_ptr&& group) { group_ = std::move(group); }

    void aggregation::set_sort(operator_ptr&& sort) { sort_ = std::move(sort); }

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
            executor = match_ ? std::move(match_)
                              : static_cast<operator_ptr>(boost::intrusive_ptr(
                                    new transfer_scan(resource_, name_, logical_plan::limit_t::unlimit())));
        }
        if (group_) {
            group_->set_children(std::move(executor));
            executor = std::move(group_);
        }
        if (sort_) {
            sort_->set_children(std::move(executor));
            executor = std::move(sort_);
        }
        set_children(std::move(executor));
    }

} // namespace components::operators
