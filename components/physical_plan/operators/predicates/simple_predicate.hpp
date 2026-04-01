#pragma once

#include "predicate.hpp"
#include <functional>

namespace components::operators::predicates {

    class simple_predicate final : public predicate {
    public:
        explicit simple_predicate(row_check_fn_t func);
        simple_predicate(std::vector<predicate_ptr>&& nested, expressions::compare_type nested_type);

    private:
        bool check_impl(const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t& chunk_right,
                        size_t index_left,
                        size_t index_right) override;

        std::vector<bool> batch_check_impl(const vector::data_chunk_t& left,
                                           const vector::data_chunk_t& right,
                                           const vector::indexing_vector_t& left_indices,
                                           const vector::indexing_vector_t& right_indices,
                                           uint64_t count) override;

        row_check_fn_t func_;
        std::vector<predicate_ptr> nested_;
        expressions::compare_type nested_type_ = expressions::compare_type::invalid;
    };

    predicate_ptr create_simple_predicate(std::pmr::memory_resource* resource,
                                          const compute::function_registry_t* function_registry,
                                          const expressions::compare_expression_ptr& expr,
                                          const std::pmr::vector<types::complex_logical_type>& types_left,
                                          const std::pmr::vector<types::complex_logical_type>& types_right,
                                          const logical_plan::storage_parameters* parameters);

} // namespace components::operators::predicates
