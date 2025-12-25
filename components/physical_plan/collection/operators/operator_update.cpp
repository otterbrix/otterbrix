#include "operator_update.hpp"
#include <services/collection/collection.hpp>

namespace components::collection::operators {

    operator_update::operator_update(services::collection::context_collection_t* context,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     const expressions::compare_expression_ptr& comp_expr)
        : read_write_operator_t(context, operator_type::update)
        , match_predicate_(comp_expr ? predicates::create_predicate(comp_expr)
                                     : predicates::create_all_true_predicate(context->resource()))
        , updates_(std::move(updates))
        , upsert_(upsert) {}

    operator_update::operator_update(services::collection::context_collection_t* context,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert)
        : operator_update(context, std::move(updates), upsert, {}) {}

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        // TODO: worth to create separate update_join operator or mutable_join with callback
        if (left_ && left_->output() && right_ && right_->output()) {
            auto tape = std::make_unique<document::impl::base_document>(context_->resource());
            if (left_->output()->documents().empty() && right_->output()->documents().empty()) {
                if (upsert_) {
                    output_ = base::operators::make_operator_data(context_->resource());
                    auto new_doc = document::make_document(context_->resource());
                    for (const auto& expr : updates_) {
                        expr->execute(new_doc, nullptr, &pipeline_context->parameters);
                    }
                    context_->document_storage().insert_or_assign(get_document_id(new_doc), new_doc);
                    context_->index_engine()->insert_document(new_doc, pipeline_context);
                    output_->append(new_doc);
                }
            } else {
                modified_ = base::operators::make_operator_write_data<document::document_id_t>(context_->resource());
                no_modified_ = base::operators::make_operator_write_data<document::document_id_t>(context_->resource());
                for (auto& doc_left : left_->output()->documents()) {
                    for (auto& doc_right : right_->output()->documents()) {
                        if (match_predicate_->check(doc_left, doc_right, &pipeline_context->parameters)) {
                            context_->index_engine()->delete_document(doc_left, pipeline_context); //todo: can optimized
                            bool modified = false;
                            for (const auto& expr : updates_) {
                                modified |= expr->execute(doc_left, doc_right, &pipeline_context->parameters);
                            }
                            if (modified) {
                                modified_->append(get_document_id(doc_left));
                            } else {
                                no_modified_->append(get_document_id(doc_left));
                            }
                            context_->index_engine()->insert_document(doc_left, pipeline_context);
                        }
                    }
                }
            }
        } else if (left_ && left_->output()) {
            auto tape = std::make_unique<document::impl::base_document>(context_->resource());
            if (left_->output()->documents().empty()) {
                if (upsert_) {
                    output_ = base::operators::make_operator_data(context_->resource());
                    auto new_doc = document::make_document(context_->resource());
                    for (const auto& expr : updates_) {
                        expr->execute(new_doc, nullptr, &pipeline_context->parameters);
                    }
                    context_->document_storage().insert_or_assign(get_document_id(new_doc), new_doc);
                    context_->index_engine()->insert_document(new_doc, pipeline_context);
                    output_->append(new_doc);
                }
            } else {
                modified_ = base::operators::make_operator_write_data<document::document_id_t>(context_->resource());
                no_modified_ = base::operators::make_operator_write_data<document::document_id_t>(context_->resource());
                for (auto& document : left_->output()->documents()) {
                    context_->index_engine()->delete_document(document, pipeline_context); //todo: can optimized
                    bool modified = false;
                    for (const auto& expr : updates_) {
                        modified |= expr->execute(document, nullptr, &pipeline_context->parameters);
                    }
                    if (modified) {
                        modified_->append(get_document_id(document));
                    } else {
                        no_modified_->append(get_document_id(document));
                    }
                    context_->index_engine()->insert_document(document, pipeline_context);
                }
            }
        }
    }

} // namespace components::collection::operators
