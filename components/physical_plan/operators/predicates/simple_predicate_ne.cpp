#include "simple_predicate_impl.hpp"

namespace components::operators::predicates {

    simple_predicate::check_function_t
    create_ne_simple_predicate(std::pmr::memory_resource* resource,
                               const compute::function_registry_t* function_registry,
                               const expressions::compare_expression_ptr& expr,
                               const std::pmr::vector<types::complex_logical_type>& types_left,
                               const std::pmr::vector<types::complex_logical_type>& types_right,
                               const logical_plan::storage_parameters* parameters) {
        return impl::create_comparator<std::not_equal_to<>>(resource, function_registry, expr, types_left, types_right, parameters);
    }

} // namespace components::operators::predicates
