#include "operator_sum.hpp"
#include "aggregate_helpers.hpp"
#include <services/collection/collection.hpp>

namespace components::table::operators::aggregate {

    constexpr auto key_result_ = "sum";

    operator_sum_t::operator_sum_t(services::collection::context_collection_t* context, index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    types::logical_value_t operator_sum_t::aggregate_impl() {
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                types::logical_value_t sum_ = impl::sum(*it, chunk.size());
                sum_.set_alias(key_result_);
                return sum_;
            }
        }
        auto result = types::logical_value_t(nullptr);
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_sum_t::key_impl() const { return key_result_; }

} // namespace components::table::operators::aggregate
