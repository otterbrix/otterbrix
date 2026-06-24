#pragma once

#include "operator.hpp"
#include "operator_data.hpp"

namespace components::operators {

    // UNION-ALL recursive-CTE fixpoint driver.
    //
    // The anchor and recursive-term sub-plans are held as PRIVATE members (not
    // left_/right_) so the executor's plan walks treat this operator as a leaf:
    //   - traverse_plan_ does NOT split them into separate top-level sub-plans, and
    //   - is_streaming_pipeline's left-chain walk sees no children here.
    // This operator owns driving BOTH sub-plans itself, through the streaming executor
    // via ctx->runner->run_subplan, inside await_async_and_resume.
    //
    // ROLE — sourceless async-finalize SINK. Because anchor_/recursive_ are private
    // members (NOT left_/right_), left()==nullptr, so to is_streaming_pipeline this is a
    // SOURCELESS SINK ROOT: role()==sink + needs_async_finalize()==true + no left child.
    // That admits it to the streaming routing seam WHEN it is itself the chain bottom and
    // every operator above it on the left-chain is a sink/source (no role()==none op). The
    // fixpoint co_awaits run_subplan, which can only run from inside await_async_and_resume
    // (a synchronous on_execute_impl cannot co_await); execute_pipeline drives that await in
    // its bottom-up needs_async_finalize pass and then streams the produced output_ UP
    // through the sink ancestors (the materialized-input pump, see executor.cpp).
    //
    // At the TOP level the outer query `SELECT ... FROM cte ...` always lowers to
    // [select -> sort -> match -> recursive_cte]. The chain bottom is this sourceless
    // sink, but a STREAMING operator_match_t sits above it, so the chain is NOT all-sink
    // and is_streaming_pipeline's sourceless-sink-root admission (which requires
    // all_sink) returns FALSE — the outer plan still runs the MATERIALIZED path. (The
    // streaming sourceless-sink-root shape assumes the bottom sink's effect is a pure
    // async commit; this operator instead PRODUCES rows in await_async_and_resume that
    // must flow UP through the match/sort/select ancestors, which execute_pipeline's
    // pass order — PUMP/FLUSH before async-finalize — does not yet support.) The sink
    // role only takes effect if this operator is ever driven as a bare sourceless-sink
    // root. on_execute_impl is therefore RETAINED as the materialized entry (arm
    // async_wait; the find_waiting_operator loop dispatches await_async_and_resume —
    // exactly how a waiting DML/scan operator is driven). Both entry points reach the
    // SAME drive_fixpoint_ core.
    class operator_recursive_cte_t final : public read_only_operator_t {
    public:
        operator_recursive_cte_t(std::pmr::memory_resource* resource, log_t log, bool all);

        // Sourceless async-finalize sink: see the class note. left()==nullptr (anchor_/
        // recursive_ are private members), so is_streaming_pipeline admits this as a
        // sourceless sink root; needs_async_finalize routes the fixpoint through
        // execute_pipeline's bottom-up await_async_and_resume pass.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // Returns a pointer to the working-set slot so operator_cte_scan_t can point into it.
        operator_data_ptr* working_set_slot() noexcept { return &working_set_; }

        // The planner injects the anchor + recursive-term sub-plan roots here (NOT via
        // set_children — see the class note). working_set_slot() must already be wired
        // into the recursive term's cte_scan(s) before this call.
        void set_recursive_terms(operator_ptr anchor, operator_ptr recursive) noexcept {
            anchor_ = std::move(anchor);
            recursive_ = std::move(recursive);
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        bool all_;
        // The current recursive-CTE working set: the rows the LAST iteration produced.
        // operator_cte_scan_t holds a raw pointer into this field (working_set_slot());
        // the fixpoint repoints it each pass. Owned here (intrusive_ptr) — run_subplan
        // must never null/move it.
        operator_data_ptr working_set_;
        operator_ptr anchor_{nullptr};
        operator_ptr recursive_{nullptr};

        // on_execute_impl arms async_wait so the materialized drive routes the fixpoint
        // through await_async_and_resume (the only place run_subplan can be co_awaited).
        void on_execute_impl(pipeline::context_t* context) override;

        // Run the anchor + fixpoint loop through ctx->runner->run_subplan. Shared core
        // for the (sole) await_async_and_resume entry point. Returns an error_t.
        actor_zeta::unique_future<core::error_t> drive_fixpoint_(pipeline::context_t* ctx);

        // Reset every operator in the recursive-term subtree (state_/output_ AND any
        // source cursor) so run_subplan re-drives it from the top each fixpoint pass:
        // the base scan re-OPENs and the cte_scan(s) re-walk the freshly-repointed
        // working set. Descends BOTH children so a join's build-side cte_scan resets too.
        static void reset_recursive_subtree(const operator_ptr& op);
    };

} // namespace components::operators
