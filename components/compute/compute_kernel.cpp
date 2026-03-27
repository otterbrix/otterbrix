#include "compute_kernel.hpp"

using namespace components::vector;

namespace components::compute {
    kernel_context::kernel_context(exec_context_t& exec_ctx, const compute_kernel& kernel)
        : exec_ctx_(exec_ctx)
        , kernel_(kernel)
        , state_(nullptr) {}

    exec_context_t& kernel_context::exec_context() const { return exec_ctx_; }

    const compute_kernel& kernel_context::kernel() const { return kernel_; }

    void kernel_context::set_state(kernel_state* state) { state_ = state; }

    kernel_state* kernel_context::state() const { return state_; }

    compute_kernel::compute_kernel(kernel_signature_t signature, kernel_init_fn init)
        : signature_(std::move(signature))
        , init_(init) {}

    compute_result<kernel_state_ptr> compute_kernel::init(kernel_context& ctx, const kernel_init_args& args) const {
        if (init_) {
            return init_(ctx, args);
        }
        return kernel_state_ptr(nullptr);
    }

    vector_kernel::vector_kernel(kernel_signature_t signature,
                                 vector_exec_fn exec,
                                 kernel_init_fn init,
                                 vector_finalize_fn finalize)
        : compute_kernel(std::move(signature), init)
        , exec_(exec)
        , finalize_(finalize) {}

    compute_status vector_kernel::execute(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) const {
        return exec_(ctx, inputs, output);
    }

    compute_status vector_kernel::finalize(kernel_context& ctx, data_chunk_t& output) const {
        if (finalize_) {
            return finalize_(ctx, output);
        }
        return compute_status::ok();
    }

    aggregate_kernel::aggregate_kernel(kernel_signature_t signature,
                                       kernel_init_fn init,
                                       aggregate_consume_fn consume,
                                       aggregate_merge_fn merge,
                                       aggregate_finalize_fn finalize)
        : compute_kernel(std::move(signature), init)
        , consume_(consume)
        , merge_(merge)
        , finalize_(finalize) {
        if (!init_) {
            throw std::logic_error("Aggregate kernels require init function!");
        }
    }

    compute_status aggregate_kernel::consume(kernel_context& ctx, const data_chunk_t& input) const {
        return consume_(ctx, input);
    }

    compute_status
    aggregate_kernel::merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state& into) const {
        return merge_(ctx, std::move(from), into);
    }

    compute_status aggregate_kernel::finalize(aggregate_kernel_context& ctx) const { return finalize_(ctx); }

    row_kernel::row_kernel(kernel_signature_t signature, row_exec_fn exec)
        : compute_kernel(std::move(signature))
        , exec_(exec) {}

    compute_status row_kernel::execute(kernel_context& ctx,
                                       const std::pmr::vector<types::logical_value_t>& inputs,
                                       std::pmr::vector<types::logical_value_t>& output) const {
        return exec_(ctx, inputs, output);
    }

} // namespace components::compute
