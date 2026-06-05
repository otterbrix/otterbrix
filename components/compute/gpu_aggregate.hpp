#pragma once

#include "compute_kernel.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace components::compute {

    enum class gpu_aggregate_op : uint8_t
    {
        sum,
        min,
        max,
        count,
        count_star,
        avg,
        custom,
    };

    [[nodiscard]] std::string_view to_string(gpu_aggregate_op op) noexcept;

    struct grouped_aggregate_input_t {
        const vector::vector_t* input{nullptr};
        const uint32_t* group_ids{nullptr};
        uint64_t row_count{0};
        size_t group_count{0};
    };

    struct grouped_aggregate_output_t {
        std::pmr::vector<types::logical_value_t>* values{nullptr};
        std::pmr::vector<uint64_t>* counts{nullptr};
    };

    class gpu_runtime_handle_t {
    public:
        virtual ~gpu_runtime_handle_t() = default;
    };

    using grouped_aggregate_gpu_fn = std::function<core::error_t(kernel_context& ctx,
                                                                 gpu_runtime_handle_t& runtime,
                                                                 const grouped_aggregate_input_t& input,
                                                                 grouped_aggregate_output_t& output)>;

    class gpu_aggregate_descriptor_t : public kernel_target_data_t {
    public:
        explicit gpu_aggregate_descriptor_t(gpu_aggregate_op op) noexcept
            : op_(op) {}

        gpu_aggregate_descriptor_t(gpu_aggregate_op op, grouped_aggregate_gpu_fn fn)
            : op_(op)
            , custom_fn_(std::move(fn)) {}

        [[nodiscard]] gpu_aggregate_op op() const noexcept { return op_; }
        [[nodiscard]] bool is_custom() const noexcept { return op_ == gpu_aggregate_op::custom; }
        [[nodiscard]] const grouped_aggregate_gpu_fn& custom_fn() const noexcept { return custom_fn_; }

        static std::shared_ptr<gpu_aggregate_descriptor_t> builtin(gpu_aggregate_op op);

        static std::shared_ptr<gpu_aggregate_descriptor_t> custom(grouped_aggregate_gpu_fn fn);

    private:
        gpu_aggregate_op op_;
        grouped_aggregate_gpu_fn custom_fn_;
    };

    using gpu_aggregate_descriptor_ptr = std::shared_ptr<gpu_aggregate_descriptor_t>;

    [[nodiscard]] const gpu_aggregate_descriptor_t* gpu_aggregate_descriptor_of(const compute_kernel& kernel) noexcept;

    aggregate_kernel make_gpu_aggregate_kernel(kernel_signature_t signature,
                                               kernel_init_fn init,
                                               aggregate_consume_fn fallback_consume,
                                               aggregate_merge_fn merge,
                                               aggregate_finalize_fn finalize,
                                               gpu_aggregate_op op);

    aggregate_kernel make_gpu_aggregate_kernel(kernel_signature_t signature,
                                               kernel_init_fn init,
                                               aggregate_consume_fn fallback_consume,
                                               aggregate_merge_fn merge,
                                               aggregate_finalize_fn finalize,
                                               grouped_aggregate_gpu_fn custom_fn);

} // namespace components::compute
