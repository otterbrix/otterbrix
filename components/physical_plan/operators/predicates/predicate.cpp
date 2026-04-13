#include "predicate.hpp"
#include "function_predicate.hpp"
#include "simple_predicate.hpp"

namespace components::operators::predicates {

    core::result_wrapper_t<bool> predicate::check(const vector::data_chunk_t& chunk, size_t index) {
        return check_impl(chunk, chunk, index, index);
    }
    core::result_wrapper_t<bool> predicate::check(const vector::data_chunk_t& chunk_left,
                                                  const vector::data_chunk_t& chunk_right,
                                                  size_t index_left,
                                                  size_t index_right) {
        return check_impl(chunk_left, chunk_right, index_left, index_right);
    }

    core::result_wrapper_t<std::vector<bool>> predicate::batch_check(const vector::data_chunk_t& left,
                                                                     const vector::data_chunk_t& right,
                                                                     const vector::indexing_vector_t& left_indices,
                                                                     const vector::indexing_vector_t& right_indices,
                                                                     uint64_t count) {
        return batch_check_impl(left, right, left_indices, right_indices, count);
    }

    core::result_wrapper_t<std::vector<bool>>
    predicate::batch_check_impl(const vector::data_chunk_t& left,
                                const vector::data_chunk_t& right,
                                const vector::indexing_vector_t& left_indices,
                                const vector::indexing_vector_t& right_indices,
                                uint64_t count) {
        std::vector<bool> results(count);
        for (uint64_t k = 0; k < count; ++k) {
            auto res = check_impl(left, right, left_indices.get_index(k), right_indices.get_index(k));
            if (res.has_error()) {
                return res.convert_error<std::vector<bool>>();
            } else {
                results[k] = res.value();
            }
        }
        return results;
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

    core::result_wrapper_t<std::vector<bool>> batch_check_1vN(const predicate_ptr& pred,
                                                              const vector::data_chunk_t& left,
                                                              const vector::data_chunk_t& right,
                                                              size_t left_index,
                                                              uint64_t right_count) {
        if (right_count == 0) {
            return {};
        }
        vector::indexing_vector_t seq(nullptr, nullptr);
        vector::indexing_vector_t broadcast(left.resource(), right_count);
        for (uint64_t k = 0; k < right_count; ++k) {
            broadcast.set_index(k, left_index);
        }
        return pred->batch_check(left, right, broadcast, seq, right_count);
    }

    core::result_wrapper_t<std::vector<bool>> batch_check_Nv1(const predicate_ptr& pred,
                                                              const vector::data_chunk_t& left,
                                                              const vector::data_chunk_t& right,
                                                              uint64_t left_count,
                                                              size_t right_index) {
        if (left_count == 0) {
            return {};
        }
        vector::indexing_vector_t seq(nullptr, nullptr);
        vector::indexing_vector_t broadcast(right.resource(), left_count);
        for (uint64_t k = 0; k < left_count; ++k) {
            broadcast.set_index(k, right_index);
        }
        return pred->batch_check(left, right, seq, broadcast, left_count);
    }
} // namespace components::operators::predicates
