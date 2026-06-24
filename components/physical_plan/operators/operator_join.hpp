#pragma once

#include "predicates/predicate.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {

    // Nested-loop join, all join types (inner/left/right/full/cross). The equi-join
    // fast path (single eq(left.key, right.key)) is substituted at plan time by
    // operator_hash_join_t; everything else lands here.
    //
    // Like operator_hash_join_t, the join is a SINK on its build (RIGHT) side and
    // STREAMING on its probe (LEFT) side. The build (right_->output()) is
    // materialized by a separate sub-plan (traverse_plan_) before the first push();
    // execute_pipeline pushes each LEFT probe batch through push() and, after the
    // probe is drained, drains unmatched build rows (right/full) via finalize().
    //
    // A SINGLE core (probe_batch_ + emit_unmatched_build_, built on the shared
    // join_builder) serves BOTH entry points: push()/finalize() (streaming, sourced
    // left chain) and on_execute_impl (materialized, sourceless sub-plans). Both
    // produce identical row order, NULL padding and dedup — two entry points to one
    // implementation, not a divergent dual path (R6).
    class operator_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_join_t(std::pmr::memory_resource* resource,
                        log_t log,
                        type join_type,
                        const expressions::expression_ptr& expression);

        // SINK on the build side, streaming on the probe side (see class comment).
        // Cross-join is the one exception (its probe needs the build row order that
        // only the materialized path guarantees); it keeps role()==none so the
        // executor routes it through on_execute_impl. role() is therefore
        // conditional on join_type_.
        [[nodiscard]] pipeline_role role() const noexcept override {
            return (join_type_ == type::cross) ? pipeline_role::none : pipeline_role::sink;
        }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        // Drop the lazily-built layout/predicate + matched marker so a re-driven sub-plan
        // (recursive-CTE recursive term, re-run per fixpoint iteration over a repointed
        // working set) rebuilds from the NEW build side. reset_for_reuse() clears
        // state_/output_ but not this build state.
        void reset_pipeline_state() noexcept override {
            layout_built_ = false;
            res_types_.clear();
            predicate_ = nullptr;
            build_matched_.clear();
            build_chunk_offsets_.clear();
            indices_left_.clear();
            indices_right_.clear();
        }

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        // --- Build/probe state (push + on_execute_impl share this) ---
        bool layout_built_{false};
        std::pmr::vector<types::complex_logical_type> res_types_{resource_};
        predicates::predicate_ptr predicate_{nullptr};
        // RIGHT/FULL only: a flat "matched" marker (one byte per build row) over all
        // build chunks, with per-chunk start offsets so build row (chunk,row) maps to
        // build_matched_[build_chunk_offsets_[chunk] + row]. Unmatched build rows are
        // NULL-padded on the left at finalize() / emit_unmatched_build_().
        std::pmr::vector<uint8_t> build_matched_{resource_};
        std::pmr::vector<uint64_t> build_chunk_offsets_{resource_};

        void on_execute_impl(pipeline::context_t* context) override;

        // Derive the output layout + predicate + (right/full) the matched marker once,
        // lazily, from the materialized build (right) side and a probe schema chunk.
        void build_layout_(pipeline::context_t* context, const vector::data_chunk_t& probe_front);
        // Probe one left batch against the materialized build chunks and emit per
        // join_type_ via the shared join_builder. Marks matched build rows for
        // right/full. Sets error on predicate failure.
        void probe_batch_(const vector::data_chunk_t& probe, chunks_vector_t& out);
        // Emit unmatched build rows (right/full) NULL-padded on the left side.
        void emit_unmatched_build_(chunks_vector_t& out);
        // Cross join is the one type that never streams (role()==none): full
        // cartesian product of the two materialized sides. Driven only by
        // on_execute_impl, it does not share the predicate-based probe core.
        void cross_join_(const std::pmr::vector<types::complex_logical_type>& out_types, chunks_vector_t& out);
    };

} // namespace components::operators
