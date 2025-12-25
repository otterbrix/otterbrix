#pragma once

#include "predicate.hpp"
#include <functional>

namespace components::collection::operators::predicates {

    class simple_predicate final : public predicate {
    public:
        using check_function_t = std::function<bool(const document::document_ptr&,
                                                    const document::document_ptr&,
                                                    const logical_plan::storage_parameters*)>;
        explicit simple_predicate(check_function_t func);
        simple_predicate(std::vector<predicate_ptr>&& nested, expressions::compare_type nested_type);

    private:
        bool check_impl(const document::document_ptr& document_left,
                        const document::document_ptr& document_right,
                        const logical_plan::storage_parameters* parameters) override;

        check_function_t func_;
        std::vector<predicate_ptr> nested_;
        expressions::compare_type nested_type_ = expressions::compare_type::invalid;
    };

    predicate_ptr create_simple_predicate(const expressions::compare_expression_ptr& expr);

} // namespace components::collection::operators::predicates
