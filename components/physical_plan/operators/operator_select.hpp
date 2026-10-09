#pragma once

#include <components/expressions/execution_dag_builder.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <memory>

namespace components::operators {

    struct projected_column_t {
        std::pmr::string name; // output alias
        expressions::param_storage value;

        projected_column_t(std::pmr::memory_resource* resource,
                           std::string_view alias,
                           expressions::param_storage projected)
            : name(alias, resource)
            , value(std::move(projected)) {}

        // A bare '*'
        [[nodiscard]] bool is_star() const noexcept;
    };

    core::error_t build_projection_graph(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<projected_column_t>& columns,
                                         const logical_plan::storage_parameters& parameters,
                                         const vector::data_chunk_t& input,
                                         size_t right_offset,
                                         std::unique_ptr<execution_dag::execution_dag_t>* graph);

    // Evaluate a projection column list against ONE input chunk, producing an
    // output chunk with one column per projected_column_t (row count == input row
    // count). Because the projection is a 1:1 row mapping, a <=1024-row input
    // yields a <=1024-row output, so callers stay within DEFAULT_VECTOR_CAPACITY
    // by feeding one chunk at a time and accumulating a chunks_vector_t.
    // Shared by operator_select_t and the DML operators' RETURNING path
    core::result_wrapper_t<vector::data_chunk_t>
    evaluate_projection(std::pmr::memory_resource* resource,
                        const std::pmr::vector<projected_column_t>& columns,
                        vector::data_chunk_t* left_input,
                        const logical_plan::storage_parameters& parameters,
                        const components::graph_execution_context& context,
                        std::unique_ptr<execution_dag::execution_dag_t>* graph,
                        const vector::data_chunk_t* right_input = nullptr);

    // operator_select_t — always the last operator before DISTINCT.
    // Processes rows one-by-one (evaluation mode): output row count equals input row count.
    // Aggregation is always handled upstream by operator_hash_group_t.
    class operator_select_t final : public read_write_operator_t {
    public:
        operator_select_t(std::pmr::memory_resource* resource, log_t log);

        void add_column(projected_column_t&& col);

        // --- Push-based streaming pipeline (STEP 3 / phase C) ---
        // A SELECT is a pure 1-batch-in -> 1-batch-out projection: each input
        // chunk maps to exactly one output chunk via evaluate_projection(), with
        // no cross-batch accumulation, so it is a streaming operator. finalize()
        // keeps the default no-op.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::streaming; }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        std::pmr::vector<projected_column_t> columns_;
        std::unique_ptr<execution_dag::execution_dag_t> graph_;
    };

} // namespace components::operators
