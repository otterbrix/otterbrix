#pragma once

#include "predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::table::operators::predicates {

    class function_predicate final : public predicate {
    public:
        explicit function_predicate(check_function_t func);

    private:
        bool check_impl(const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t& chunk_right,
                        size_t index_left,
                        size_t index_right) override;

        check_function_t func_;
    };

    predicate_ptr create_function_predicate(std::pmr::memory_resource* resource,
                                            const compute::function_registry_t* function_registry,
                                            const expressions::function_expression_ptr& expr,
                                            const logical_plan::storage_parameters* parameters);

} // namespace components::table::operators::predicates
