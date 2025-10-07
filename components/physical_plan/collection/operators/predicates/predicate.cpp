#include "predicate.hpp"
#include "simple_predicate.hpp"
#include <components/document/document.hpp>

namespace components::collection::operators::predicates {

    bool predicate::check(const document::document_ptr& document, const logical_plan::storage_parameters* parameters) {
        return check_impl(document, nullptr, parameters);
    }
    bool predicate::check(const document::document_ptr& document_left,
                          const document::document_ptr& document_right,
                          const logical_plan::storage_parameters* parameters) {
        return check_impl(document_left, document_right, parameters);
    }

    predicate_ptr create_predicate(const expressions::compare_expression_ptr& expr) {
        // TODO: use schema to deduce expr side, if it is not set, before this
        auto result = create_simple_predicate(expr);
        if (result) {
            return result;
        }
        //todo: other predicates
        static_assert(true, "not valid condition type");
        return nullptr;
    }

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource) {
        return create_simple_predicate(make_compare_expression(resource, expressions::compare_type::all_true));
    }
} // namespace components::collection::operators::predicates
