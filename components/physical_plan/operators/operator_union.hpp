#pragma once

#include "operator.hpp"

namespace components::operators {

    // UNION / UNION ALL. A SINK: push() folds each left batch into buffered_left_,
    // finalize() emits the union of that buffer and the materialized right_->output()
    class operator_union_t final : public read_only_operator_t {
    public:
        operator_union_t(std::pmr::memory_resource* resource, log_t log, bool all);

        // The validator-stamped union output schema: validate_schema reconciles the two
        // branches' column types data-INDEPENDENTLY (a genuine mismatch is rejected, a
        // bare NULL-literal branch adopts the other side's type, column names come from
        // the first SELECT) and create_plan_union forwards the stamp here, so
        // emit_union_() types its result from the plan, never from row contents.
        void set_output_types(const std::pmr::vector<types::complex_logical_type>& types) override;

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        void reset_pipeline_state() noexcept override { buffered_left_.clear(); }

    private:
        bool all_;
        std::pmr::vector<types::complex_logical_type> output_types_;
        chunks_vector_t buffered_left_{resource_};

        // The shared dedup/concat core: emit the union of `left_chunks` then
        // `right_chunks` into `out` (allocated from `res`). UNION ALL concatenates;
        // UNION dedups across both sides (left rows first, in order; then right rows
        // not already seen).
        void emit_union_(std::pmr::memory_resource* res,
                         const chunks_vector_t& left_chunks,
                         const chunks_vector_t& right_chunks,
                         chunks_vector_t& out);
    };

} // namespace components::operators
