#include "predicate.hpp"
#include "function_predicate.hpp"
#include "simple_predicate.hpp"

namespace components::operators::predicates {

    bool predicate::check(const vector::data_chunk_t& chunk, size_t index) {
        return check_impl(chunk, chunk, index, index);
    }
    bool predicate::check(const vector::data_chunk_t& chunk_left,
                          const vector::data_chunk_t& chunk_right,
                          size_t index_left,
                          size_t index_right) {
        return check_impl(chunk_left, chunk_right, index_left, index_right);
    }

    predicate_ptr create_predicate(std::pmr::memory_resource* resource,
                                   const compute::function_registry_t* function_registry,
                                   const expressions::expression_ptr& expr,
                                   const std::pmr::vector<types::complex_logical_type>& types_left,
                                   const std::pmr::vector<types::complex_logical_type>& types_right,
                                   const logical_plan::storage_parameters* parameters) {
        if (expr->group() == expressions::expression_group::function) {
            const auto& func_expr = reinterpret_cast<const expressions::function_expression_ptr&>(expr);
            return create_function_predicate(resource, function_registry, func_expr, parameters);
        } else {
            assert(expr->group() == expressions::expression_group::compare);
            const auto& comp_expr = reinterpret_cast<const expressions::compare_expression_ptr&>(expr);
            return create_simple_predicate(resource, function_registry, comp_expr, types_left, types_right, parameters);
        }
    }

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource) {
        return create_simple_predicate(resource,
                                       nullptr,
                                       make_compare_expression(resource, expressions::compare_type::all_true),
                                       {},
                                       {},
                                       nullptr);
    }
} // namespace components::operators::predicates
