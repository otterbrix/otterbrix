#pragma once

#include "operator.hpp"

namespace components::operators {

    class operator_raw_data_t final : public read_only_operator_t {
    public:
        explicit operator_raw_data_t(vector::data_chunk_t&& chunk);
        explicit operator_raw_data_t(const vector::data_chunk_t& chunk);

        std::pmr::memory_resource* resource() const noexcept override;

        // --- Push-based streaming pipeline source (VALUES / literal rows) ---
        // A VALUES (or no-FROM constant) literal carrier is a SOURCE: the rows are
        // already materialized in output_ from the ctor, so source_next() just walks
        // output_->chunks() by a cursor and emits a COPY of each. role()==source lets
        // a VALUES SELECT / INSERT...VALUES chain stream through execute_pipeline (the
        // INSERT sink folds the VALUES batches via push() instead of adopting
        // left_->output() wholesale) rather than forcing the legacy materialize path.
        //
        // A VALUES literal ALWAYS carries real columns (the value schema), so a
        // schema'd 0-row chunk is NOT the drain sentinel — only the 0-column chunk
        // returned past the cursor end is. A 0-row VALUES still emits its one schema'd
        // 0-row chunk first (so an OUTER join NULL-pads / a COUNT over it sees 0),
        // then drains.
        //
        // source_next does NO cross-actor await (the data is in-process); it resolves
        // the future synchronously with a co_return.
        //
        // SAFE as a right child: traverse_plan_ splits the right side into its own
        // sub-plan that runs first and materializes into output_ (set_output +
        // mark_executed), and is_streaming_pipeline only walks the LEFT chain — so a
        // join/update/delete with a VALUES right side reads the materialized output_,
        // not source_next.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

    private:
        // No-op: output_ is populated in the ctor. Kept as the materialized entry
        // point for the right-child case (pre-executed by its own sub-plan) and any
        // legacy materialize caller. Shares the same source-of-truth (output_) as
        // source_next — two entry points to one already-materialized result.
        void on_execute_impl(pipeline::context_t*) override;

        // Build the 0-column drain sentinel that tells execute_pipeline's pump to
        // stop (mirrors the scan sources' drain chunk).
        vector::data_chunk_t make_drain_chunk();

        // Cursor over output_->chunks(): index of the next chunk to emit. When it
        // passes the chunk count the source is drained. output_ always carries the
        // VALUES schema chunk (>=1 chunk), so the schema'd 0-row guard a 0-row VALUES
        // needs is just the first cursor step — no separate empty-guard bookkeeping.
        std::size_t cursor_{0};
    };

} // namespace components::operators
