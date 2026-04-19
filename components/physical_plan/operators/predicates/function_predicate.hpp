#pragma once

#include "predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::operators::predicates {

    class function_predicate final : public predicate {
    public:
        using batch_check_fn_t =
            std::function<core::result_wrapper_t<std::vector<bool>>(const vector::data_chunk_t&,
                                                                    const vector::data_chunk_t&,
                                                                    const vector::indexing_vector_t&,
                                                                    const vector::indexing_vector_t&,
                                                                    uint64_t count)>;

        explicit function_predicate(row_check_fn_t func);
        explicit function_predicate(row_check_fn_t func, batch_check_fn_t batch_func);

    private:
        core::result_wrapper_t<bool> check_impl(const vector::data_chunk_t& chunk_left,
                                                const vector::data_chunk_t& chunk_right,
                                                size_t index_left,
                                                size_t index_right) override;

        core::result_wrapper_t<std::vector<bool>> batch_check_impl(const vector::data_chunk_t& left,
                                                                   const vector::data_chunk_t& right,
                                                                   const vector::indexing_vector_t& left_indices,
                                                                   const vector::indexing_vector_t& right_indices,
                                                                   uint64_t count) override;

        row_check_fn_t func_;
        batch_check_fn_t batch_func_;
    };

    predicate_ptr create_function_predicate(std::pmr::memory_resource* resource,
                                            const compute::function_registry_t* function_registry,
                                            const expressions::function_expression_ptr& expr,
                                            const logical_plan::storage_parameters* parameters);

} // namespace components::operators::predicates
