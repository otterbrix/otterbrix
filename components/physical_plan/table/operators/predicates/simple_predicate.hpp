#pragma once

#include "predicate.hpp"
#include <functional>

namespace components::table::operators::predicates {

    class simple_predicate : public predicate {
    public:
        using check_function_t = std::function<bool(const vector::data_chunk_t& chunk_left,
                                                    const vector::data_chunk_t& chunk_right,
                                                    size_t index_left,
                                                    size_t index_right)>;
        explicit simple_predicate(check_function_t func);
        simple_predicate(std::vector<predicate_ptr>&& nested, expressions::compare_type nested_type);

    private:
        bool check_impl(const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t& chunk_right,
                        size_t index_left,
                        size_t index_right) final;

        check_function_t func_;
        std::vector<predicate_ptr> nested_;
        expressions::compare_type nested_type_ = expressions::compare_type::invalid;
    };

    predicate_ptr create_simple_predicate(const expressions::compare_expression_ptr& expr,
                                          const std::pmr::vector<types::complex_logical_type>& types_left,
                                          const std::pmr::vector<types::complex_logical_type>& types_right,
                                          const logical_plan::storage_parameters* parameters);

} // namespace components::table::operators::predicates
