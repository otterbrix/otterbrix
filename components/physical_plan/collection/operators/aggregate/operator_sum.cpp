#include "operator_sum.hpp"
#include <services/collection/collection.hpp>

namespace components::collection::operators::aggregate {

    constexpr auto key_result_ = "sum";

    operator_sum_t::operator_sum_t(services::collection::context_collection_t* context, index::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key)) {}

    document::document_ptr operator_sum_t::aggregate_impl() {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        auto result = document::make_document(resource);
        if (left_ && left_->output()) {
            const auto& documents = left_->output()->documents();
            auto tape = std::make_unique<document::impl::base_document>(resource);
            document::value_t sum_{};
            std::for_each(documents.cbegin(), documents.cend(), [&](const document::document_ptr& doc) {
                sum_ = sum(sum_, doc->get_value(key_.as_string()), tape.get());
            });
            result->set(key_result_, sum_);
        } else {
            result->set(key_result_, 0);
        }
        return result;
    }

    std::string operator_sum_t::key_impl() const { return key_result_; }

} // namespace components::collection::operators::aggregate
