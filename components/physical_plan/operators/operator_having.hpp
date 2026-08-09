#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/execution_graph_builder.hpp>
#include <components/expressions/expression.hpp>

#include <memory>

namespace components::operators {

    // SQL HAVING — a post-aggregation streaming filter placed ABOVE the group operator.
    //
    // It computes its condition on an execution graph exactly as operator_match does, but is a
    // DISTINCT operator (operator_type::having, rendered "Having").
    // It is kept separate from operator_match because three WHERE-only branches are structurally
    // unreachable for HAVING (its child is ALWAYS the group operator, never a scan or nothing):
    //   * NO LIMIT / read-cap  — HAVING is always lowered unlimit(); operator_limit is the window.
    //   * NO row-id propagation — the input is a group SINK whose output row_ids are the zero
    //     sentinel; the typed gather (data_chunk_t::copy) reproduces that sentinel for free.
    //   * NO sourceless shape   — role() is unconditionally streaming; there is no source_next.
    // The row copy uses the TYPED, no-box data_chunk_t::copy gather, not match's per-cell
    // logical_value_t box/unbox loop.
    class operator_having_t final : public read_only_operator_t {
    public:
        operator_having_t(std::pmr::memory_resource* resource,
                          log_t log,
                          const expressions::expression_ptr& expression);

        // Always streaming: create_plan_aggregate always splices this above the group operator, so
        // left_ is never null and the operator is never a source (no source_next override needed).
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::streaming; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        const expressions::expression_ptr expression_;

        std::unique_ptr<execution_graph::execution_graph_t> graph_;
        expressions::condition_kind condition_{expressions::condition_kind::always};
        std::pmr::vector<types::complex_logical_type> stream_types_{resource_};
        std::vector<size_t> stream_populated_cols_;
        bool stream_sparse_{false};
        bool stream_ready_{false};
    };

} // namespace components::operators
