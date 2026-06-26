#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <unordered_set>

namespace components::operators {

    // SELECT DISTINCT. A SINK on its single (LEFT) input: rows arrive batch-by-batch
    // through push(), the first occurrence of each distinct row is retained, and the
    // unique rows are emitted in input order at finalize(). The seen-set and the
    // emission go through emit_distinct_().
    class operator_distinct_t final : public read_only_operator_t {
    public:
        operator_distinct_t(std::pmr::memory_resource* resource, log_t log);


        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        // Distinct-row identity set, accumulated ACROSS input batches (push) so the
        // first occurrence of a row anywhere in the stream wins. Survives until
        // finalize().
        std::unordered_set<std::string> seen_;

        // The shared dedup core: for each row of each chunk, build the all-column
        // identity key, and on first occurrence copy the row into `out` (chunks of
        // ≤ DEFAULT_VECTOR_CAPACITY). `seen` carries across calls so the streaming
        // path dedups across batches. Output preserves input order.
        void emit_distinct_(std::pmr::memory_resource* res, const chunks_vector_t& chunks, chunks_vector_t& out);
    };

} // namespace components::operators
