#include "predicate.hpp"
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
                                   const expressions::compare_expression_ptr& expr,
                                   const std::pmr::vector<types::complex_logical_type>& types_left,
                                   const std::pmr::vector<types::complex_logical_type>& types_right,
                                   const logical_plan::storage_parameters* parameters) {
        // TODO: use schema to deduce expr side, if it is not set, before this
        auto result = create_simple_predicate(resource, expr, types_left, types_right, parameters);
        if (result) {
            return result;
        }
        //todo: other predicates
        static_assert(true, "not valid condition type");
        return nullptr;
    }

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource) {
        return create_simple_predicate(resource,
                                       make_compare_expression(resource, expressions::compare_type::all_true),
                                       {},
                                       {},
                                       nullptr);
    }
} // namespace components::operators::predicates
