#include "operator_join.hpp"

#include <services/collection/collection.hpp>
#include <vector>

namespace components::collection::operators {

    operator_join_t::operator_join_t(services::collection::context_collection_t* context,
                                     type join_type,
                                     predicates::predicate_ptr&& predicate)
        : read_only_operator_t(context, operator_type::join)
        , join_type_(join_type)
        , predicate_(std::move(predicate)) {}

    bool operator_join_t::check_expressions_(const document::document_ptr& left,
                                             const document::document_ptr& right,
                                             pipeline::context_t* context) {
        return predicate_->check(left, right, context ? &context->parameters : nullptr);
    }

    void operator_join_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            output_ = base::operators::make_operator_data(left_->output()->resource());

            if (context_) {
                // With introduction of raw_data without context, log is not guaranteed to be here
                // TODO: acquire log from different means
                trace(context_->log(), "operator_join::left_size(): {}", left_->output()->size());
                trace(context_->log(), "operator_join::right_size(): {}", left_->output()->size());
            }

            switch (join_type_) {
                case type::inner:
                    inner_join_(context);
                    break;
                case type::full:
                    outer_full_join_(context);
                    break;
                case type::left:
                    outer_left_join_(context);
                    break;
                case type::right:
                    outer_right_join_(context);
                    break;
                case type::cross:
                    cross_join_(context);
                    break;
                default:
                    break;
            }

            if (context_) {
                // Same reason as above
                trace(context_->log(), "operator_join::result_size(): {}", output_->size());
            }
        }
    }

    void operator_join_t::inner_join_(pipeline::context_t* context) {
        for (auto doc_left : left_->output()->documents()) {
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right, context)) {
                    output_->append(document::document_t::merge(doc_left, doc_right, left_->output()->resource()));
                }
            }
        }
    }

    void operator_join_t::outer_full_join_(pipeline::context_t* context) {
        auto empty_left = document::make_document(left_->output()->resource());
        auto empty_right = document::make_document(left_->output()->resource());
        if (!left_->output()->documents().empty()) {
            auto doc = right_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_left->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }
        if (!right_->output()->documents().empty()) {
            auto doc = right_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_right->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        std::vector<bool> visited_right(right_->output()->size(), false);
        for (auto doc_left : left_->output()->documents()) {
            bool visited_left = false;
            size_t right_index = 0;
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right, context)) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    output_->append(
                        std::move(document::document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
                right_index++;
            }
            if (!visited_left) {
                output_->append(
                    std::move(document::document_t::merge(doc_left, empty_right, left_->output()->resource())));
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            output_->append(std::move(document::document_t::merge(empty_left,
                                                                  right_->output()->documents().at(i),
                                                                  left_->output()->resource())));
        }
    }

    void operator_join_t::outer_left_join_(pipeline::context_t* context) {
        auto empty_right = document::make_document(left_->output()->resource());
        if (!right_->output()->documents().empty()) {
            auto doc = right_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_right->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        for (auto doc_left : left_->output()->documents()) {
            bool visited_left = false;
            for (auto doc_right : right_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right, context)) {
                    visited_left = true;
                    output_->append(
                        std::move(document::document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
            }
            if (!visited_left) {
                output_->append(
                    std::move(document::document_t::merge(doc_left, empty_right, left_->output()->resource())));
            }
        }
    }

    void operator_join_t::outer_right_join_(pipeline::context_t* context) {
        auto empty_left = document::make_document(left_->output()->resource());
        if (!left_->output()->documents().empty()) {
            auto doc = left_->output()->documents().front();
            auto fields = doc->json_trie()->as_object();
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                empty_left->set(it_field->first->get_mut()->get_string(), nullptr);
            }
        }

        for (auto doc_right : right_->output()->documents()) {
            bool visited_right = false;
            for (auto doc_left : left_->output()->documents()) {
                if (check_expressions_(doc_left, doc_right, context)) {
                    visited_right = true;
                    output_->append(
                        std::move(document::document_t::merge(doc_left, doc_right, left_->output()->resource())));
                }
            }
            if (!visited_right) {
                output_->append(
                    std::move(document::document_t::merge(empty_left, doc_right, left_->output()->resource())));
            }
        }
    }

    void operator_join_t::cross_join_(pipeline::context_t*) {
        for (auto doc_left : left_->output()->documents()) {
            for (auto doc_right : right_->output()->documents()) {
                output_->append(
                    std::move(document::document_t::merge(doc_left, doc_right, left_->output()->resource())));
            }
        }
    }

} // namespace components::collection::operators
