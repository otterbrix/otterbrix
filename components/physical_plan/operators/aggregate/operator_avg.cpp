#include "operator_avg.hpp"
#include "aggregate_helpers.hpp"
#include <services/collection/collection.hpp>

namespace components::operators::aggregate {

    constexpr auto key_result_ = "avg";

    operator_avg_t::operator_avg_t(services::collection::context_collection_t* context, expressions::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    types::logical_value_t operator_avg_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                types::logical_value_t sum_ = impl::sum(*it, chunk.size());
                sum_.set_alias(key_result_);
                // possible bug here since not every type could be cast to double
                auto result = types::logical_value_t(left_->output()->resource(), sum_.cast_as(types::logical_type::DOUBLE).value<double>() /
                                                     double(chunk.size()));
                result.set_alias(key_result_);
                return result;
            }
        }
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), types::complex_logical_type{types::logical_type::NA});
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_avg_t::key_impl() const { return key_result_; }

} // namespace components::operators::aggregate