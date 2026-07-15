#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <components/logical_plan/node_limit.hpp>

namespace components::operators {

    // Canonical LIMIT/OFFSET operator. A STREAMING window over its single input: skip
    // the first `offset` rows of the merged stream, emit at most `limit` rows. No
    // buffering — each batch is trimmed to its surviving contiguous slice and
    // forwarded, so peak memory is one batch.
    //
    // It exists because some sources CANNOT apply a merged outer LIMIT/OFFSET without
    // corrupting their result:
    //   - a UNION forwards the limit into BOTH arms, so an OFFSET over `A UNION B`
    //     skips rows of A *and* B independently — wrong;
    //   - a GROUP BY that pushes the limit into its scan sees only rows `[m, m+n)`,
    //     producing the WRONG groups and counts.
    // create_plan_aggregate passes `unlimit` to such a source and wraps the terminal
    // chain in this operator as the OUTERMOST node (above DISTINCT — LIMIT applies
    // after DISTINCT), so the window is applied once over the fully-merged stream. The
    // ORDER-BY path is untouched: its sort already applies the limit.
    //
    // The executor's root count-cap re-fires over this already-windowed (<= limit)
    // output as a harmless no-op — it caps the count only.
    class operator_limit_t final : public read_only_operator_t {
    public:
        operator_limit_t(std::pmr::memory_resource* resource, log_t log, logical_plan::limit_t limit);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::streaming; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        const logical_plan::limit_t limit_;

        // Count of input rows SEEN across all push() batches so far — the stream
        // position of the first row of the NEXT batch. The emit window is intersected
        // against [stream_pos_, stream_pos_ + batch.size()) so a LIMIT caps the total
        // across ALL batches and an OFFSET skips the head of the whole stream.
        int64_t stream_pos_{0};
    };

} // namespace components::operators
