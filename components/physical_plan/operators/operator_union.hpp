#pragma once

#include "operator.hpp"

namespace components::operators {

    // UNION / UNION ALL. A SINK whose two inputs are BOTH materialized by separate
    // sub-plans (traverse_plan_ splits a binary node's left and right children) before
    // the union runs: when the union is reached, left_->output() and right_->output()
    // are ready. push() therefore folds nothing (the streaming pump's left batches are
    // a redundant view of the already-materialized left_->output()); finalize() emits
    // the union of the two materialized sides via the emit_union_() core — left rows
    // first (in order), then right rows (deduped across both sides for UNION,
    // concatenated for UNION ALL).
    class operator_union_t final : public read_only_operator_t {
    public:
        operator_union_t(std::pmr::memory_resource* resource, log_t log, bool all);

        // The validator-stamped union output schema: validate_schema reconciles the two
        // branches' column types data-INDEPENDENTLY (a genuine mismatch is rejected, a
        // bare NULL-literal branch adopts the other side's type, column names come from
        // the first SELECT) and create_plan_union forwards the stamp here, so
        // emit_union_() types AND names its result from the plan, never from row contents.
        void set_output_schema(const vector::schema_t& schema) override;

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        bool all_;
        vector::schema_t output_schema_;

        // The shared dedup/concat core: emit the union of `left_chunks` then
        // `right_chunks` into `out` (allocated from `res`). UNION ALL concatenates;
        // UNION dedups across both sides (left rows first, in order; then right rows
        // not already seen).
        [[nodiscard]] core::error_t emit_union_(std::pmr::memory_resource* res,
                                                const chunks_vector_t& left_chunks,
                                                const chunks_vector_t& right_chunks,
                                                chunks_vector_t& out);
    };

} // namespace components::operators
