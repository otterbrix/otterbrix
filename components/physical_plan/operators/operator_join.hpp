#pragma once

#include "predicates/predicate.hpp"
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/expressions/compare_expression.hpp>

namespace components::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_join_t(std::pmr::memory_resource* resource,
                        log_t log,
                        type join_type,
                        const expressions::expression_ptr& expression);

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        struct stats_t {
            bool hash_join = false;
            uint64_t build_rows = 0;
            uint64_t hash_table_size = 0;
            uint64_t probe_batches = 0;
            uint64_t probe_rows = 0;
            uint64_t matches = 0;
        };

        void on_execute_impl(pipeline::context_t* context) override;
        void inner_join_(const predicates::predicate_ptr&,
                         pipeline::context_t* context,
                         const std::pmr::vector<types::complex_logical_type>& out_types,
                         chunks_vector_t& out_chunks,
                         stats_t* stats);
        bool inner_hash_join_(const predicates::predicate_ptr&,
                              const std::pmr::vector<types::complex_logical_type>& out_types,
                              chunks_vector_t& out_chunks,
                              stats_t* stats);
        void outer_full_join_(const predicates::predicate_ptr&,
                              pipeline::context_t* context,
                              const std::pmr::vector<types::complex_logical_type>& out_types,
                              chunks_vector_t& out_chunks,
                              stats_t* stats);
        void outer_left_join_(const predicates::predicate_ptr&,
                              pipeline::context_t* context,
                              const std::pmr::vector<types::complex_logical_type>& out_types,
                              chunks_vector_t& out_chunks,
                              stats_t* stats);
        void outer_right_join_(const predicates::predicate_ptr&,
                               pipeline::context_t* context,
                               const std::pmr::vector<types::complex_logical_type>& out_types,
                               chunks_vector_t& out_chunks,
                               stats_t* stats);
        void cross_join_(pipeline::context_t* context,
                         const std::pmr::vector<types::complex_logical_type>& out_types,
                         chunks_vector_t& out_chunks,
                         stats_t* stats);
    };

} // namespace components::operators
