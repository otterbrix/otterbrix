#pragma once

#include "compute_kernel.hpp"
#include "kernel_signature.hpp"

#include <memory>
#include <vector>

namespace components::compute::detail {
    class kernel_executor_t {
    public:
        virtual ~kernel_executor_t() = default;

        virtual core::error_t init(kernel_context& kernel_ctx, kernel_init_args args) = 0;

        [[nodiscard]] virtual core::result_wrapper_t<datum_t> execute(const vector::data_chunk_t& inputs) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<datum_t>
        execute(const std::vector<vector::data_chunk_t>& inputs) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<datum_t>
        execute(const std::pmr::vector<types::logical_value_t>& inputs) = 0;

        static std::unique_ptr<kernel_executor_t> make_vector();
        static std::unique_ptr<kernel_executor_t> make_aggregate();
        static std::unique_ptr<kernel_executor_t> make_row();
    };
} // namespace components::compute::detail
