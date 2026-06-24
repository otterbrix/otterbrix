#pragma once

#include "operator.hpp"
#include "operator_data.hpp"

namespace components::operators {

    // Leaf SOURCE that streams the current recursive-CTE working set. The pointer slot
    // is owned by operator_recursive_cte_t and repointed each fixpoint iteration to the
    // rows the previous iteration produced; the recursive driver resets this source
    // (reset_source) before each pass so the walk restarts over the freshly-repointed set.
    //
    // role()==source lets the recursive term (typically JOIN(scan, cte_scan)) stream
    // through execute_pipeline: traverse_plan_ / the runner's build-side materialization
    // pre-runs the join's RIGHT child (this cte_scan) into its output_, so the join builds
    // its hash table from the working set and probes the streaming base scan. Each call
    // emits a COPY of one working-set chunk (the working set is shared — owned by the
    // recursive_cte and read again by the NEXT iteration — so its chunks must not be moved
    // out), then a 0-column drain sentinel so the pump stops. Mirrors operator_raw_data's
    // source_next, but reads *working_set_ (which moves under it) instead of a fixed output_.
    class operator_cte_scan_t final : public read_only_operator_t {
    public:
        operator_cte_scan_t(std::pmr::memory_resource* resource, log_t log, operator_data_ptr* working_set);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the working-set walk so the next fixpoint iteration re-reads from the head
        // of the (now repointed) working set. reset_for_reuse() clears state_/output_; this
        // clears the source cursor.
        void reset_pipeline_state() noexcept override { cursor_ = 0; }

    private:
        // Pointer into operator_recursive_cte_t::working_set_ (set at create-plan time,
        // never moved). Null when the CTE name did not resolve (degenerate plan).
        operator_data_ptr* working_set_;

        // Index of the next working-set chunk to emit. Past the chunk count ⇒ drained.
        std::size_t cursor_{0};

        // Materialized entry point (legacy on_execute path): adopt the working set wholesale.
        // Shares the working-set source-of-truth with source_next.
        void on_execute_impl(pipeline::context_t*) override;
    };

} // namespace components::operators
