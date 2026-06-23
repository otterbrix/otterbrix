#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include "predicates/predicate.hpp"
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_limit.hpp>

namespace components::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(std::pmr::memory_resource* resource,
                         log_t log,
                         const expressions::expression_ptr& expression,
                         logical_plan::limit_t limit);

        // --- Push-based streaming pipeline (filter operator) — SHAPE-BASED role ---
        // A match is a pure per-batch filter+projection: each input chunk yields the
        // surviving (predicate-true) rows of that same chunk, with no cross-batch
        // accumulation of OUTPUT, so per batch it IS a streaming transform. The ONLY
        // cross-batch state is the LIMIT/OFFSET running row counter (stream_limit_total_),
        // carried as operator state so a LIMIT caps the total emitted across ALL batches
        // and an OFFSET skips the head rows of the stream (a per-batch reset would be
        // wrong). finalize() keeps the default no-op (no deferred rows: every surviving
        // row is emitted in the push() that produced it).
        //
        // But role() is SHAPE-BASED (like operator_delete): match streams ONLY when it
        // sits DIRECTLY on a scan SOURCE (the create_plan_match_ "full_scan under a
        // non-pure-compare predicate" shape). The OTHER shapes stay role()==none and run
        // the legacy materialized on_execute path:
        //   - no-table match (sourceless sub-plan, left_ == nullptr);
        //   - HAVING / post-JOIN match (left_ is an aggregate/group SINK or a JOIN) —
        //     streaming a filter ABOVE a sink/join would re-route the join's emission
        //     through a streaming combination that is out of scope for this phase and
        //     regresses join/subquery plans, so those keep materializing.
        // Both entry points route through the SAME filter_batch_ core (R6).
        [[nodiscard]] pipeline_role role() const noexcept override {
            return (left_ && is_scan(left_->type()) && left_->role() == pipeline_role::source)
                       ? pipeline_role::streaming
                       : pipeline_role::none;
        }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        const expressions::expression_ptr expression_;
        const logical_plan::limit_t limit_;

        // LIMIT/OFFSET running total for the STREAMING path: count of predicate-true
        // rows SEEN across all push() batches so far. limit_.is_skipping() /
        // limit_.check() read it to decide OFFSET-skip vs emit vs limit-reached, so a
        // LIMIT caps the total across ALL batches and an OFFSET skips the head of the
        // stream. on_execute_impl owns a LOCAL counter instead (so a recursive-CTE
        // re-drive via reset_for_reuse restarts at 0) — both feed the same filter core.
        int64_t stream_limit_total_{0};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // Shared filter core (R6): filter ONE input chunk through the predicate +
        // projection, advancing the caller-owned LIMIT/OFFSET counter `limit_total`,
        // and append the surviving-rows chunk to `out` (nothing appended when zero
        // rows survive). Called by BOTH push() (per streamed batch, member counter)
        // and on_execute_impl (per materialized chunk, local counter). `predicate` and
        // the populated-column projection are passed in so the predicate is built ONCE
        // per execution, not per chunk.
        [[nodiscard]] core::error_t filter_batch_(std::pmr::memory_resource* resource,
                                                  predicates::predicate_ptr& predicate,
                                                  const std::vector<size_t>& populated_cols,
                                                  bool sparse,
                                                  const std::pmr::vector<types::complex_logical_type>& types,
                                                  const vector::data_chunk_t& chunk,
                                                  int64_t& limit_total,
                                                  chunks_vector_t& out);

        // Build the predicate + projection metadata (populated_cols / sparse / types)
        // for an input chunk schema. Shared one-time setup for both entry points; the
        // predicate depends only on the (stable) chunk schema, so push() rebuilds it
        // only on the first batch it sees.
        [[nodiscard]] core::error_t build_predicate_(pipeline::context_t* ctx,
                                                     const vector::data_chunk_t& sample,
                                                     std::pmr::vector<types::complex_logical_type>& types,
                                                     std::vector<size_t>& populated_cols,
                                                     bool& sparse,
                                                     predicates::predicate_ptr& predicate);

        // Streaming-path predicate cache: built lazily on the first push() batch and
        // reused for every subsequent batch (schema is stable across a scan's batches).
        predicates::predicate_ptr stream_predicate_{nullptr};
        std::pmr::vector<types::complex_logical_type> stream_types_{resource_};
        std::vector<size_t> stream_populated_cols_;
        bool stream_sparse_{false};
        bool stream_ready_{false};
    };

} // namespace components::operators
