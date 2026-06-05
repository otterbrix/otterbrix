#include "gpu_aggregate.hpp"

namespace components::compute {

    std::string_view to_string(gpu_aggregate_op op) noexcept {
        switch (op) {
            case gpu_aggregate_op::sum:
                return "sum";
            case gpu_aggregate_op::min:
                return "min";
            case gpu_aggregate_op::max:
                return "max";
            case gpu_aggregate_op::count:
                return "count";
            case gpu_aggregate_op::count_star:
                return "count_star";
            case gpu_aggregate_op::avg:
                return "avg";
            case gpu_aggregate_op::custom:
                return "custom";
        }
        return "unknown";
    }

    std::shared_ptr<gpu_aggregate_descriptor_t> gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op op) {
        return std::make_shared<gpu_aggregate_descriptor_t>(op);
    }

    std::shared_ptr<gpu_aggregate_descriptor_t> gpu_aggregate_descriptor_t::custom(grouped_aggregate_gpu_fn fn) {
        return std::make_shared<gpu_aggregate_descriptor_t>(gpu_aggregate_op::custom, std::move(fn));
    }

    const gpu_aggregate_descriptor_t* gpu_aggregate_descriptor_of(const compute_kernel& kernel) noexcept {
        if (!kernel.is_gpu_target()) {
            return nullptr;
        }
        return dynamic_cast<const gpu_aggregate_descriptor_t*>(kernel.target_data());
    }

    aggregate_kernel make_gpu_aggregate_kernel(kernel_signature_t signature,
                                               kernel_init_fn init,
                                               aggregate_consume_fn fallback_consume,
                                               aggregate_merge_fn merge,
                                               aggregate_finalize_fn finalize,
                                               gpu_aggregate_op op) {
        aggregate_kernel kernel(std::move(signature),
                                std::move(init),
                                std::move(fallback_consume),
                                std::move(merge),
                                std::move(finalize));
        kernel.set_target(kernel_target::gpu_opencl);
        kernel.set_target_data(gpu_aggregate_descriptor_t::builtin(op));
        return kernel;
    }

    aggregate_kernel make_gpu_aggregate_kernel(kernel_signature_t signature,
                                               kernel_init_fn init,
                                               aggregate_consume_fn fallback_consume,
                                               aggregate_merge_fn merge,
                                               aggregate_finalize_fn finalize,
                                               grouped_aggregate_gpu_fn custom_fn) {
        aggregate_kernel kernel(std::move(signature),
                                std::move(init),
                                std::move(fallback_consume),
                                std::move(merge),
                                std::move(finalize));
        kernel.set_target(kernel_target::gpu_opencl);
        kernel.set_target_data(gpu_aggregate_descriptor_t::custom(std::move(custom_fn)));
        return kernel;
    }

} // namespace components::compute
