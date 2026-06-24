#pragma once

#include "operator.hpp"

namespace components::operators {

    // A pre-set single-data carrier (output_ holds the data handed to the ctor). Today
    // it is only ever read synchronously via output() by its holder (operator_aggregate_t
    // wraps an input chunk in one and runs the scalar aggregate over left_->output()) — it
    // is never a plan root nor an operator on a driven left-chain. It is nonetheless a
    // trivial SOURCE: when execute_pipeline ever drives it, source_next() streams its held
    // chunks then the 0-column drain sentinel, so no plan it sits in is forced to legacy.
    class operator_empty_t final : public read_only_operator_t {
    public:
        operator_empty_t(std::pmr::memory_resource* resource, operator_data_ptr&& data);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;
        void reset_pipeline_state() noexcept override { emit_index_ = 0; }

    private:
        void on_execute_impl(pipeline::context_t*) override;

        size_t emit_index_{0};
    };

} // namespace components::operators
