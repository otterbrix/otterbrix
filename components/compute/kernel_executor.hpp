#pragma once

#include "compute_kernel.hpp"
#include "compute_result.hpp"
#include "kernel_signature.hpp"

#include <memory>
#include <vector>

namespace components::compute::detail {
    class kernel_executor_t {
    public:
        virtual ~kernel_executor_t() = default;

        virtual compute_status init(kernel_context& kernel_ctx, kernel_init_args args) = 0;

        virtual compute_result<datum_t> execute(const vector::data_chunk_t& inputs, size_t exec_length) = 0;
        virtual compute_result<datum_t> execute(const std::vector<vector::data_chunk_t>& inputs,
                                                size_t exec_length) = 0;

        static std::unique_ptr<kernel_executor_t> make_vector();
        static std::unique_ptr<kernel_executor_t> make_aggregate();
    };
} // namespace components::compute::detail
