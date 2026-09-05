#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/storage/storage.hpp>
#include <components/table/column_state.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    // Build a table_filter_t from a compare-expression predicate + bound parameters, resolving
    // parameter ids against `parameters` and coercing literals in the context's timezone. An
    // expression filter is compiled into its execution graph HERE, so the filter crosses the
    // mailbox ready to run. Defined in
    // full_scan.cpp; used by the full_scan disk-send OPEN to lower the WHERE once.
    // Returns a null unique_ptr for an all-true / absent predicate, or a physical_plan_error /
    // invalid_parameter on a malformed expression — never throws (R2).
    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters,
                        const components::graph_execution_context& context);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit,
                  std::vector<size_t> projected_cols = {});

        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }

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
        // sequential await — no lost-wakeup.
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
            guard_types_.clear();
        }

        // Release an OPEN, un-drained cursor. Nothing agent-side reclaims it — the drain
        // paths only fire when the source keeps pulling — and a live cursor permanently gates
        // compact() on its table. Called by the executor when it stops pumping a source early
        // (error mid-pump, satisfied LIMIT, abandoned sub-plan). Idempotent: after the cursor
        // drains, or once released, cursor_id_ is 0 and this is a no-op.
        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return cursor_id_ != 0 && !drained_; }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        // Projected empty chunk (drained / short-circuit sentinel) carrying the table schema, so a
        // downstream OUTER join can NULL-pad and a scalar aggregate can emit COUNT=0.
        vector::data_chunk_t make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types);

        // Apply the drained empty-guard to one fetched batch. Returns the batch to emit, the schema'd
        // empty-guard, or the 0-column drain sentinel. (OFFSET is applied by operator_limit above.)
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
        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
    };

} // namespace components::operators
