#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit,
                  std::vector<size_t> projected_cols = {});

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
        // role()==source drives the streaming push/finalize pipeline. The FIRST source_next call
        // OPENs a position-only fetch-next cursor on the owning agent (storage_fetch_next_batch,
        // cursor_id==0), passing the filter + projection + the (offset+limit) head cap; each
        // subsequent call ADVANCEs the SAME cursor (cursor_id!=0, filter dropped) and reads exactly
        // ONE batch — zero pins survive the round-trip, so peak scan memory is one batch regardless
        // of table size. A drained cursor (cardinality-0 reply) yields the 0-column sentinel so
        // execute_pipeline stops. The N sequential cross-actor co_awaits live in this NESTED
        // operator coroutine (driven by co_await from execute_pipeline), not in a behavior() handler,
        // so the actor-zeta single-slot awaited continuation is republished+cleared between each
        // sequential await (same shape as await_async_and_resume's two sequential awaits) — no
        // lost-wakeup.
        // A no-table sentinel scan (INVALID_OID, e.g. a no-FROM `SELECT 2+3`) is ALSO a
        // source: source_next emits ONE synthetic single-row batch with one placeholder
        // column (not the 0-column drain sentinel), then drains, so the downstream
        // operator_select_t projects its constant/arithmetic columns over that one row —
        // matching the legacy virtual-row path. role() is therefore unconditionally source.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the per-batch fetch-next cursor so a re-driven sub-plan (recursive-CTE
        // recursive term, re-run once per fixpoint iteration) re-OPENs the scan from the
        // head of the stream instead of seeing a drained cursor. Clears only the streaming
        // cursor bookkeeping; reset_for_reuse() handles the generic state_/output_.
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            remaining_offset_ = 0;
            guard_types_.clear();
        }

    private:
        // Projected empty chunk (drained / short-circuit sentinel) carrying the table schema, so a
        // downstream OUTER join can NULL-pad and a scalar aggregate can emit COUNT=0.
        vector::data_chunk_t make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types);

        // Apply per-batch OFFSET skip and the drained empty-guard to one fetched batch, re-fetching
        // (ADVANCE) while OFFSET still consumes whole batches. Returns the batch to emit, the schema'd
        // empty-guard, or the 0-column drain sentinel.
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;

        // Per-batch fetch-next cursor state.
        //   opened_   : false until the first source_next runs the one-time setup (short-circuits,
        //               filter build, the storage_types await for the empty-guard schema).
        //   cursor_id_: 0 ⇒ next fetch is the OPEN; non-zero ⇒ the agent-minted id to ADVANCE.
        //   drained_  : the agent returned a cardinality-0 batch ⇒ source exhausted.
        //   emitted_any_ / guard_types_: if the scan drains having produced zero real rows, emit ONE
        //               schema'd 0-row guard chunk (so a scalar aggregate emits COUNT=0 and an OUTER
        //               join NULL-pads), then the 0-column sentinel.
        //   remaining_offset_: OFFSET rows still to skip from the head of the stream (the agent caps
        //               offset+limit but does not skip; the source skips per-batch).
        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        uint64_t remaining_offset_{0};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
    };

} // namespace components::operators
