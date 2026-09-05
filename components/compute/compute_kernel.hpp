#pragma once

#include "aggregate_state.hpp"
#include "kernel_signature.hpp"
#include "kernel_utils.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/span.hpp>
#include <functional>
#include <memory>
#include <vector>

namespace components::compute {
    class compute_kernel;

    using datum_t = vector::data_chunk_t;

    // opaque kernel-specific state, for example, if there is some kind of initialization required
    class kernel_state {
    public:
        virtual ~kernel_state() = default;
    };
    using kernel_state_ptr = std::unique_ptr<kernel_state>;

    class kernel_context {
    public:
        // exec_context may be null
        kernel_context(exec_context_t& exec_ctx, const compute_kernel& kernel);

        kernel_context(const kernel_context&) = delete;
        kernel_context(kernel_context&& other) = default;
        kernel_context& operator=(const kernel_context&) = delete;
        kernel_context& operator=(kernel_context&& other) = default;

        exec_context_t& exec_context() const;
        const compute_kernel& kernel() const;

        void set_state(kernel_state* state);
        kernel_state* state() const;

    private:
        std::reference_wrapper<exec_context_t> exec_ctx_;
        std::reference_wrapper<const compute_kernel> kernel_;
        kernel_state* state_;
    };

    using kernel_init_fn = core::result_wrapper_t<kernel_state_ptr> (*)(kernel_context&, kernel_init_args);

    class compute_kernel {
    public:
        explicit compute_kernel(kernel_signature_t signature, kernel_init_fn init = nullptr);
        virtual ~compute_kernel() = default;

        const kernel_signature_t& signature() const { return signature_; }
        core::result_wrapper_t<kernel_state_ptr> init(kernel_context& ctx, const kernel_init_args& args) const;

    protected:
        kernel_signature_t signature_;
        kernel_init_fn init_;
    };

    using vector_exec_fn = core::error_t (*)(kernel_context& ctx,
                                             const vector::data_chunk_t& inputs,
                                             vector::vector_t& output);

    // datum are results aggregated over batches
    using vector_finalize_fn = core::error_t (*)(kernel_context& ctx, vector::data_chunk_t& output);

    class vector_kernel : public compute_kernel {
    public:
        vector_kernel(kernel_signature_t signature,
                      vector_exec_fn exec,
                      kernel_init_fn init = nullptr,
                      vector_finalize_fn finalize = nullptr);

        core::error_t execute(kernel_context& ctx, const vector::data_chunk_t& inputs, vector::vector_t& output) const;
        core::error_t finalize(kernel_context& ctx, vector::data_chunk_t& output) const;

    private:
        vector_exec_fn exec_;
        vector_finalize_fn finalize_;
    };

    // An aggregate accumulates into one state PER GROUP, addressed by group id. An ungrouped
    // aggregate is the same thing over a single group, so there is one shape, not two.
    //
    // state_layout answers how big one accumulator is for the resolved argument types;
    // update folds a whole chunk, row i into the accumulator groups[i] selects;
    // finalize emits one value per group into `output`, for groups [first, first + count).
    using aggregate_layout_fn =
        aggregate_state_layout_t (*)(const std::pmr::vector<types::complex_logical_type>& inputs);
    using aggregate_update_fn = core::error_t (*)(kernel_context& ctx,
                                                  const vector::data_chunk_t& input,
                                                  core::span<const uint32_t> groups,
                                                  aggregate_states_t states);
    using aggregate_finalize_fn = core::error_t (*)(kernel_context& ctx,
                                                    aggregate_states_t states,
                                                    uint64_t first,
                                                    uint64_t count,
                                                    vector::vector_t& output);

    class aggregate_kernel : public compute_kernel {
    public:
        aggregate_kernel(kernel_signature_t signature,
                         aggregate_layout_fn layout,
                         aggregate_update_fn update,
                         aggregate_finalize_fn finalize);

        [[nodiscard]] aggregate_state_layout_t
        state_layout(const std::pmr::vector<types::complex_logical_type>& inputs) const;
        core::error_t update(kernel_context& ctx,
                             const vector::data_chunk_t& input,
                             core::span<const uint32_t> groups,
                             aggregate_states_t states) const;
        core::error_t finalize(kernel_context& ctx,
                               aggregate_states_t states,
                               uint64_t first,
                               uint64_t count,
                               vector::vector_t& output) const;

    private:
        aggregate_layout_fn layout_;
        aggregate_update_fn update_;
        aggregate_finalize_fn finalize_;
    };

    using expand_exec_fn = core::error_t (*)(kernel_context& ctx,
                                             const vector::data_chunk_t& inputs,
                                             std::pmr::vector<vector::data_chunk_t>& outputs);

    class expand_kernel : public compute_kernel {
    public:
        expand_kernel(kernel_signature_t signature, expand_exec_fn exec);

        core::error_t execute(kernel_context& ctx,
                              const vector::data_chunk_t& inputs,
                              std::pmr::vector<vector::data_chunk_t>& outputs) const;

    private:
        expand_exec_fn exec_;
    };

} // namespace components::compute
