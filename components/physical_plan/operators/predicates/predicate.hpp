#pragma once

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/tri_bool.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>
#include <core/date/date_types.hpp>

namespace components::operators::predicates {

    // A predicate evaluates to SQL three-valued logic: TRUE / FALSE / UNKNOWN (types::tri_bool_t).
    // A NULL operand makes a comparison UNKNOWN, which is distinct from FALSE -- the two diverge
    // under NOT. Each consumer collapses the tri-state to a bool at the very end with the rule that
    // fits its context: selects() for a WHERE / join / DML filter (only TRUE admits a row),
    // permits() for a CHECK constraint (only FALSE violates; UNKNOWN passes).
    class predicate : public boost::intrusive_ref_counter<predicate> {
    public:
        using row_check_fn_t =
            std::function<core::result_wrapper_t<types::tri_bool_t>(const vector::data_chunk_t& chunk_left,
                                                                    const vector::data_chunk_t& chunk_right,
                                                                    size_t index_left,
                                                                    size_t index_right)>;
        predicate() = default;
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        core::result_wrapper_t<types::tri_bool_t> check(const vector::data_chunk_t& chunk, size_t index);
        core::result_wrapper_t<types::tri_bool_t> check(const vector::data_chunk_t& chunk_left,
                                                        const vector::data_chunk_t& chunk_right,
                                                        size_t index_left,
                                                        size_t index_right);

        // evaluate predicate for a batch of (left_indices[k], right_indices[k]) pairs
        // returns result[k] = predicate(left[left_indices[k]], right[right_indices[k]])
        core::result_wrapper_t<std::vector<types::tri_bool_t>>
        batch_check(const vector::data_chunk_t& left,
                    const vector::data_chunk_t& right,
                    const vector::indexing_vector_t& left_indices,
                    const vector::indexing_vector_t& right_indices,
                    uint64_t count);

    protected:
        virtual core::result_wrapper_t<types::tri_bool_t> check_impl(const vector::data_chunk_t& chunk_left,
                                                                     const vector::data_chunk_t& chunk_right,
                                                                     size_t index_left,
                                                                     size_t index_right) = 0;

        // default implementation loops over with check_impl, batch-capable subclasses can override
        virtual core::result_wrapper_t<std::vector<types::tri_bool_t>>
        batch_check_impl(const vector::data_chunk_t& left,
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
                                   const logical_plan::storage_parameters* parameters,
                                   core::date::timezone_offset_t session_tz);

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource);

    // check left[left_index] against right[0..right_count).
    core::result_wrapper_t<std::vector<types::tri_bool_t>> batch_check_1vN(const predicate_ptr& pred,
                                                                           const vector::data_chunk_t& left,
                                                                           const vector::data_chunk_t& right,
                                                                           size_t left_index,
                                                                           uint64_t right_count);

    // check left[0..left_count) against right[right_index].
    core::result_wrapper_t<std::vector<types::tri_bool_t>> batch_check_Nv1(const predicate_ptr& pred,
                                                                           const vector::data_chunk_t& left,
                                                                           const vector::data_chunk_t& right,
                                                                           uint64_t left_count,
                                                                           size_t right_index);

} // namespace components::operators::predicates
