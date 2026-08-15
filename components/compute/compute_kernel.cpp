#include "compute_kernel.hpp"

using namespace components::vector;

namespace components::compute {
    kernel_context::kernel_context(exec_context_t& exec_ctx, const compute_kernel& kernel)
        : exec_ctx_(exec_ctx)
        , kernel_(kernel)
        , state_(nullptr) {
        assert(exec_context().resource());
    }

    exec_context_t& kernel_context::exec_context() const { return exec_ctx_; }

    const compute_kernel& kernel_context::kernel() const { return kernel_; }

    void kernel_context::set_state(kernel_state* state) { state_ = state; }

    kernel_state* kernel_context::state() const { return state_; }

    compute_kernel::compute_kernel(kernel_signature_t signature, kernel_init_fn init)
        : signature_(std::move(signature))
        , init_(init) {}

    core::result_wrapper_t<kernel_state_ptr> compute_kernel::init(kernel_context& ctx,
                                                                  const kernel_init_args& args) const {
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

    core::error_t vector_kernel::execute(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) const {
        return exec_(ctx, inputs, output);
    }

    core::error_t vector_kernel::finalize(kernel_context& ctx, data_chunk_t& output) const {
        if (finalize_) {
            return finalize_(ctx, output);
        }
        return core::error_t::no_error();
    }

    aggregate_kernel::aggregate_kernel(kernel_signature_t signature,
                                       aggregate_layout_fn layout,
                                       aggregate_update_fn update,
                                       aggregate_finalize_fn finalize)
        : compute_kernel(std::move(signature))
        , layout_(layout)
        , update_(update)
        , finalize_(finalize) {
        if (!layout_ || !update_ || !finalize_) {
            throw std::logic_error("Aggregate kernels require a state layout, an update and a finalize!");
        }
    }

    aggregate_state_layout_t
    aggregate_kernel::state_layout(const std::pmr::vector<types::complex_logical_type>& inputs) const {
        return layout_(inputs);
    }

    core::error_t aggregate_kernel::update(kernel_context& ctx,
                                           const data_chunk_t& input,
                                           core::span<const uint32_t> groups,
                                           aggregate_states_t states) const {
        return update_(ctx, input, groups, states);
    }

    core::error_t aggregate_kernel::finalize(kernel_context& ctx,
                                             aggregate_states_t states,
                                             uint64_t first,
                                             uint64_t count,
                                             vector_t& output) const {
        return finalize_(ctx, states, first, count, output);
    }

    row_kernel::row_kernel(kernel_signature_t signature, row_exec_fn exec, kernel_init_fn init)
        : compute_kernel(std::move(signature), init)
        , exec_(exec) {}

    core::error_t row_kernel::execute(kernel_context& ctx,
                                      const std::pmr::vector<types::logical_value_t>& inputs,
                                      std::pmr::vector<types::logical_value_t>& output) const {
        return exec_(ctx, inputs, output);
    }

    expand_kernel::expand_kernel(kernel_signature_t signature, expand_exec_fn exec)
        : compute_kernel(std::move(signature))
        , exec_(exec) {}

    core::error_t expand_kernel::execute(kernel_context& ctx,
                                         const vector::data_chunk_t& inputs,
                                         std::pmr::vector<vector::data_chunk_t>& outputs) const {
        return exec_(ctx, inputs, outputs);
    }

} // namespace components::compute
