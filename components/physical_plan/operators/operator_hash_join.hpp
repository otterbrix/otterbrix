#pragma once

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {

    // Equi-join fast path. create_plan_join substitutes this for operator_join_t
    // when the ON condition is a single eq(left.key, right.key); the matching
    // columns (`left_col`/`right_col`, into the respective input chunks) are
    // detected at plan time and passed in.
    //
    // Builds a hash table over the right side once and probes it with the left,
    // turning the nested-loop O(L·R) join into O(L + R). Output layout, NULL
    // padding and chunk-streaming match operator_join_t exactly (shared
    // join_detail helpers), so results are identical to the nested-loop path.
    //
    // Only inner / left / right / full are ever substituted (cross is not an
    // equi-join); any other join_type is treated as a no-op.
    class operator_hash_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_hash_join_t(std::pmr::memory_resource* resource,
                             log_t log,
                             type join_type,
                             size_t left_col,
                             size_t right_col);

    private:
        type join_type_;
        // Equi-key column index into the left / right input chunks respectively.
        size_t left_col_;
        size_t right_col_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        void on_execute_impl(pipeline::context_t* context) override;

        void inner_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                              chunks_vector_t& out_chunks);
        void outer_left_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                   chunks_vector_t& out_chunks);
        void outer_right_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                    chunks_vector_t& out_chunks);
        void outer_full_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                   chunks_vector_t& out_chunks);
    };

} // namespace components::operators
