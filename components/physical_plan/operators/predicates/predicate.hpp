#pragma once

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>

namespace components::operators::predicates {

    class predicate : public boost::intrusive_ref_counter<predicate> {
    public:
        using row_check_fn_t = std::function<bool(const vector::data_chunk_t& chunk_left,
                                                  const vector::data_chunk_t& chunk_right,
                                                  size_t index_left,
                                                  size_t index_right)>;
        predicate() = default;
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const vector::data_chunk_t& chunk, size_t index);
        bool check(const vector::data_chunk_t& chunk_left,
                   const vector::data_chunk_t& chunk_right,
                   size_t index_left,
                   size_t index_right);

        // evaluate predicate for a batch of (left_indices[k], right_indices[k]) pairs
        // returns result[k] = predicate(left[left_indices[k]], right[right_indices[k]])
        std::vector<bool> batch_check(const vector::data_chunk_t& left,
                                      const vector::data_chunk_t& right,
                                      const vector::indexing_vector_t& left_indices,
                                      const vector::indexing_vector_t& right_indices,
                                      uint64_t count);

    private:
        virtual bool check_impl(const vector::data_chunk_t& chunk_left,
                                const vector::data_chunk_t& chunk_right,
                                size_t index_left,
                                size_t index_right) = 0;

        // default implementation loops over with check_impl, batch-capable subclasses can override
        virtual std::vector<bool> batch_check_impl(const vector::data_chunk_t& left,
                                                   const vector::data_chunk_t& right,
                                                   const vector::indexing_vector_t& left_indices,
                                                   const vector::indexing_vector_t& right_indices,
                                                   uint64_t count);
    };

    using predicate_ptr = boost::intrusive_ptr<predicate>;

    predicate_ptr create_predicate(std::pmr::memory_resource* resource,
                                   const compute::function_registry_t* function_registry,
                                   const expressions::expression_ptr& expr,
                                   const std::pmr::vector<types::complex_logical_type>& types_left,
                                   const std::pmr::vector<types::complex_logical_type>& types_right,
                                   const logical_plan::storage_parameters* parameters);

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource);

    // check left[left_index] against right[0..right_count).
    std::vector<bool> batch_check_1vN(const predicate_ptr& pred,
                                      const vector::data_chunk_t& left,
                                      const vector::data_chunk_t& right,
                                      size_t left_index,
                                      uint64_t right_count);

    // check left[0..left_count) against right[right_index].
    std::vector<bool> batch_check_Nv1(const predicate_ptr& pred,
                                      const vector::data_chunk_t& left,
                                      const vector::data_chunk_t& right,
                                      uint64_t left_count,
                                      size_t right_index);

} // namespace components::operators::predicates
