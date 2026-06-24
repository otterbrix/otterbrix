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

        // --- Push-based streaming pipeline (filter operator) — UNCONDITIONALLY streaming ---
        // A match is a pure per-batch filter+projection: each input chunk yields the
        // surviving (predicate-true) rows of that same chunk, with no cross-batch
        // accumulation of OUTPUT, so per batch it IS a streaming transform. The ONLY
        // cross-batch state is the LIMIT/OFFSET running row counter (stream_limit_total_),
        // carried as operator state so a LIMIT caps the total emitted across ALL batches
        // and an OFFSET skips the head rows of the stream (a per-batch reset would be
        // wrong). finalize() keeps the default no-op (no deferred rows: every surviving
        // row is emitted in the push() that produced it).
        //
        // role() is now streaming WHENEVER there is an input (left_ != nullptr),
        // independent of whether the input is a scan SOURCE or a SINK (group/join).
        // The two operator_match-specific defects that previously made match-over-sink
        // unsafe to stream are fixed in this operator:
        //   (a) row_ids: filter_batch_ only propagates the input row_id when it is real
        //       (row_ids_meaningful_(): left_ is a scan source); over a sink it leaves
        //       the zero sentinel so no bogus absolute id reaches a downstream consumer.
        //   (b) predicate resource: build_predicate_ allocates the predicate + the types
        //       working copy on the operator's STABLE resource_, not the (foreign /
        //       transient) sink chunk's arena, so the cached predicate's value-getter
        //       closures stay valid across the second finalize chunk.
        // The executor FLUSH/PUMP path itself already streams a filter above a sink
        // correctly (select-over-sink works), so with both defects fixed the filter
        // streams in every shape.
        //
        // The sourceless no-table match (left_ == nullptr) is a degenerate shape: the
        // physical-plan generator only emits it when create_plan_match_ cannot resolve
        // the table_oid (has_table_oid() == false), but the executor re-captures
        // known_oids from the enriched plan before create_plan runs, so a valid table
        // never reaches it. It produces an empty result (the materialized path returned
        // early with a null output_). Modeled as a SOURCE that drains immediately
        // (source_next emits the 0-column sentinel on the first call), so it is a valid
        // streaming-pipeline root that yields the same empty result. Both transforming
        // entry points route through the SAME filter_batch_ core (R6).
        [[nodiscard]] pipeline_role role() const noexcept override {
            return left_ != nullptr ? pipeline_role::streaming : pipeline_role::source;
        }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        // SOURCE entry for the sourceless no-table shape (left_ == nullptr): there is
        // no input to filter, so drain immediately with the 0-column sentinel — the
        // streaming equivalent of the materialized path's early-return-with-no-output.
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

    private:
        // The input chunks carry REAL absolute row_ids ONLY when this match sits
        // directly on a scan SOURCE (full_scan / transfer_scan / index_scan stamp
        // them). Over a SINK (group/join) — or the sourceless no-table shape — the
        // input's row_ids are zero-filled, so they MUST NOT be propagated forward
        // (see filter_batch_ defect fix (a)). Computed from left_, which is fixed
        // once the plan is built, so it is stable across batches.
        [[nodiscard]] bool row_ids_meaningful_() const noexcept {
            return left_ != nullptr && is_scan(left_->type());
        }

        const expressions::expression_ptr expression_;
        const logical_plan::limit_t limit_;

        // LIMIT/OFFSET running total for the STREAMING path: count of predicate-true
        // rows SEEN across all push() batches so far. limit_.is_skipping() /
        // limit_.check() read it to decide OFFSET-skip vs emit vs limit-reached, so a
        // LIMIT caps the total across ALL batches and an OFFSET skips the head of the
        // stream. It feeds the shared filter core.
        int64_t stream_limit_total_{0};

        // Shared filter core (R6): filter ONE input chunk through the predicate +
        // projection, advancing the caller-owned LIMIT/OFFSET counter `limit_total`,
        // and append the surviving-rows chunk to `out` (nothing appended when zero
        // rows survive). Called by push() (per streamed batch, member counter).
        // `predicate` and the populated-column projection are passed in so the
        // predicate is built ONCE per execution, not per chunk.
        //
        // `row_ids_meaningful`: whether the input chunk carries REAL absolute row_ids
        // (true ONLY when left_ is a scan SOURCE, which stamps them). A SINK output
        // (group/join) never writes row_ids — they stay zero-filled — so propagating
        // them forward would hand a downstream DML/index consumer a bogus absolute id
        // 0. When false the surviving rows' row_ids are left at the chunk's own
        // zero-initialized sentinel (the same value the materialized path produced
        // over a sink), and NO foreign id is copied in.
        [[nodiscard]] core::error_t filter_batch_(std::pmr::memory_resource* resource,
                                                  predicates::predicate_ptr& predicate,
                                                  const std::vector<size_t>& populated_cols,
                                                  bool sparse,
                                                  bool row_ids_meaningful,
                                                  const std::pmr::vector<types::complex_logical_type>& types,
                                                  const vector::data_chunk_t& chunk,
                                                  int64_t& limit_total,
                                                  chunks_vector_t& out);

        // Build the predicate + projection metadata (populated_cols / sparse / types)
        // for an input chunk schema. Shared one-time setup for both entry points; the
        // predicate depends only on the (stable) chunk schema, so push() rebuilds it
        // only on the first batch it sees. `resource` is the STABLE resource to allocate
        // the predicate (and the types working copy) on — the caller chooses it (the
        // operator's resource_ for a scan-source match, the captured input-chunk
        // resource for a sink match whose resource_ is null).
        [[nodiscard]] core::error_t build_predicate_(pipeline::context_t* ctx,
                                                     const vector::data_chunk_t& sample,
                                                     std::pmr::memory_resource* resource,
                                                     std::pmr::vector<types::complex_logical_type>& types,
                                                     std::vector<size_t>& populated_cols,
                                                     bool& sparse,
                                                     predicates::predicate_ptr& predicate);

        // Streaming-path predicate cache: built lazily on the first push() batch and
        // reused for every subsequent batch (schema is stable across batches).
        predicates::predicate_ptr stream_predicate_{nullptr};
        // The stable resource the streaming run allocates on (resource_ when non-null,
        // else the first batch's resource — captured once). stream_types_ is rebound to
        // it in push() before first use, so it is never left bound to a null resource_.
        std::pmr::memory_resource* stream_resource_{nullptr};
        std::pmr::vector<types::complex_logical_type> stream_types_{resource_};
        std::vector<size_t> stream_populated_cols_;
        bool stream_sparse_{false};
        bool stream_ready_{false};
    };

} // namespace components::operators
