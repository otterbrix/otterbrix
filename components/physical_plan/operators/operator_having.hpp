#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include "predicate_executor.hpp"

#include <optional>
#include <components/expressions/expression.hpp>

namespace components::operators {

    // SQL HAVING — a post-aggregation streaming filter placed ABOVE the group operator.
    //
    // It reuses the shared predicate_executor_t facade (one bound tree + select) exactly as
    // operator_match does, but is a DISTINCT operator (operator_type::having, rendered "Having").
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

        // Streaming predicate cache: the group-output schema is stable across the several
        // <=1024-row chunks the group emits (operator_group fixes out_types once), so the predicate
        // + projection metadata are built ONCE on the first batch, on the operator's stable resource_
        // (always context.resource, non-null), and reused for every subsequent chunk.
        std::optional<predicate_executor_t> stream_predicate_;
        // Sized once to the widest batch; reset() per batch because set_index is unchecked.
        vector::indexing_vector_t stream_selection_{nullptr, nullptr};
        // The group-output SCHEMA, cloned once off the first batch: what the predicate binds
        // against and what each surviving batch is rebuilt as. A gather (data_chunk_t::copy) moves
        // values, not identities, so the output chunk describes its columns from here — the same
        // columns the input described, and not merely the same shapes.
        vector::schema_t stream_schema_{resource_};
        std::vector<size_t> stream_populated_cols_;
        bool stream_sparse_{false};
        bool stream_ready_{false};
    };

} // namespace components::operators
