#include "operator_count.hpp"
#include <services/collection/collection.hpp>

namespace components::collection::operators::aggregate {

    constexpr auto key_result_ = "count";

    operator_count_t::operator_count_t(services::collection::context_collection_t* context)
        : operator_aggregate_t(context) {}

    document::document_ptr operator_count_t::aggregate_impl() {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        auto doc = document::make_document(resource);
        if (left_ && left_->output()) {
            doc->set(key_result_, uint64_t(left_->output()->size()));
        } else {
            doc->set(key_result_, 0);
        }
        return doc;
    }

    std::string operator_count_t::key_impl() const { return key_result_; }

} // namespace components::collection::operators::aggregate