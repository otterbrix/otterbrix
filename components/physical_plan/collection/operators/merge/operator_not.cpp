#include "operator_not.hpp"
#include <services/collection/collection.hpp>

namespace components::collection::operators::merge {

    operator_not_t::operator_not_t(services::collection::context_collection_t* context, logical_plan::limit_t limit)
        : operator_merge_t(context, limit) {}

    void operator_not_t::on_merge_impl(pipeline::context_t*) {
        //todo: optimize merge
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (left_ && left_->output()) {
            output_ = base::operators::make_operator_data(context_->resource());
            const auto& left_documents = left_->output()->documents();
            for (const auto& document : context_->document_storage()) {
                auto it = std::find_if(
                    left_documents.cbegin(),
                    left_documents.cend(),
                    [&document](const document::document_ptr& doc) { return get_document_id(doc) == document.first; });
                if (it == left_documents.cend()) {
                    output_->append(document.second);
                    ++count;
                    if (!limit_.check(count)) {
                        return;
                    }
                }
            }
        }
    }

} // namespace components::collection::operators::merge
