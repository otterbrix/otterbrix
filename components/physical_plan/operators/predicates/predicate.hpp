#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators::predicates {

    class predicate : public boost::intrusive_ref_counter<predicate> {
    public:
        predicate() = default;
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const vector::data_chunk_t& chunk, size_t index);
        bool check(const vector::data_chunk_t& chunk_left,
                   const vector::data_chunk_t& chunk_right,
                   size_t index_left,
                   size_t index_right);

    private:
        virtual bool check_impl(const vector::data_chunk_t& chunk_left,
                                const vector::data_chunk_t& chunk_right,
                                size_t index_left,
                                size_t index_right) = 0;
    };

    using predicate_ptr = boost::intrusive_ptr<predicate>;

    predicate_ptr create_predicate(std::pmr::memory_resource* resource,
                                   const expressions::compare_expression_ptr& expr,
                                   const std::pmr::vector<types::complex_logical_type>& types_left,
                                   const std::pmr::vector<types::complex_logical_type>& types_right,
                                   const logical_plan::storage_parameters* parameters);

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource);

} // namespace components::operators::predicates
