#include "operator_count.hpp"

namespace components::operators::aggregate {

    constexpr auto key_result_ = "count";

    operator_count_t::operator_count_t(std::pmr::memory_resource* resource, log_t* log,
                                       collection_full_name_t name)
        : operator_aggregate_t(resource, log, std::move(name)) {}

    types::logical_value_t operator_count_t::aggregate_impl() {
        if (left_ && left_->output()) {
            auto result = types::logical_value_t(left_->output()->resource(), uint64_t(left_->output()->size()));
            result.set_alias(key_result_);
            return result;
        }
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), uint64_t(0));
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_count_t::key_impl() const { return key_result_; }

} // namespace components::operators::aggregate
