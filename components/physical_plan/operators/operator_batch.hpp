#pragma once

#include "operator.hpp"
#include "operator_data.hpp"
#include <boost/intrusive_ptr.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {
    // A pre-materialized data carrier: holds a fixed set of chunks (set in the ctor,
    // mark_executed() immediately). Today it is only ever read synchronously via
    // output() by its holder (operator_group_t's non-vectorizable aggregator gather,
    // operator_aggregate_t's scalar path) — it is never a plan root nor an operator
    // on a driven left-chain. It is nonetheless a trivial SOURCE: when execute_pipeline
    // ever drives it, source_next() streams its held chunks one at a time and then the
    // 0-column drain sentinel, so no plan it sits in is forced onto the legacy path.
    class operator_batch_t final : public read_only_operator_t {
    public:
        operator_batch_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;
        void reset_pipeline_state() noexcept override { emit_index_ = 0; }

    private:
        void on_execute_impl(pipeline::context_t*) override {}

        // Index of the next held chunk source_next() will emit; past the end it returns
        // the 0-column drain sentinel.
        size_t emit_index_{0};
    };

    using operator_batch_ptr = boost::intrusive_ptr<operator_batch_t>;

    inline operator_batch_ptr make_operator_batch(std::pmr::memory_resource* resource, chunks_vector_t&& chunks) {
        return {new operator_batch_t(resource, std::move(chunks))};
    }
} // namespace components::operators
