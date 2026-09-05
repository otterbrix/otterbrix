#pragma once

#include "compute_kernel.hpp"
#include "kernel_signature.hpp"

#include <memory>
#include <memory_resource>
#include <vector>

namespace components::compute::detail {
    class kernel_executor_t {
    public:
        virtual ~kernel_executor_t() = default;

        [[nodiscard]] virtual core::error_t init(kernel_context& kernel_ctx, kernel_init_args args) = 0;

        [[nodiscard]] virtual core::result_wrapper_t<datum_t> execute(const vector::data_chunk_t& inputs) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<datum_t>
        execute(const std::vector<vector::data_chunk_t>& inputs) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<datum_t>
        execute(const std::pmr::vector<types::logical_value_t>& inputs) = 0;

        // Aggregates only: fold a chunk into one accumulator per group, then emit one value per
        // group. The caller owns the accumulators and says how big one is by asking state_layout.
        [[nodiscard]] virtual aggregate_state_layout_t state_layout() const = 0;
        [[nodiscard]] virtual core::error_t
        update(const vector::data_chunk_t& inputs, core::span<const uint32_t> groups, aggregate_states_t states) = 0;
        [[nodiscard]] virtual core::error_t
        finalize(aggregate_states_t states, uint64_t first, uint64_t count, vector::vector_t& output) = 0;

        // `resource` is the resource the caller resolved the executor with. It outlives the
        // executor and is the only one on hand before init() has run, which is exactly when a
        // refusal about the missing init has to be worded.
        static std::unique_ptr<kernel_executor_t> make_vector(std::pmr::memory_resource* resource);
        static std::unique_ptr<kernel_executor_t> make_aggregate(std::pmr::memory_resource* resource);
        static std::unique_ptr<kernel_executor_t> make_row(std::pmr::memory_resource* resource);
    };
} // namespace components::compute::detail
