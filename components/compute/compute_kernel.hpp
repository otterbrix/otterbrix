#pragma once

#include "kernel_signature.hpp"
#include "kernel_utils.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace components::compute {
    class compute_kernel;

    // originally, arrow-compute's datum is a variant<scalar, vector<T>, data_chunk>.
    // in our implementation it is a little bit simplified
    using datum_t = std::variant<std::pmr::vector<types::logical_value_t>, vector::data_chunk_t>;

    enum class kernel_target : uint8_t
    {
        cpu = 0,
        gpu_opencl = 1,
    };

    constexpr inline size_t kernel_target_count = 2;

    [[nodiscard]] std::string_view to_string(kernel_target target) noexcept;

    class kernel_target_data_t {
    public:
        virtual ~kernel_target_data_t() = default;
    };
    using kernel_target_data_ptr = std::shared_ptr<kernel_target_data_t>;

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

    class aggregate_kernel_context : public kernel_context {
    public:
        aggregate_kernel_context(exec_context_t& exec_ctx, const compute_kernel& kernel)
            : kernel_context(exec_ctx, kernel)
            , batch_results(exec_ctx.resource()) {}

        std::pmr::vector<types::logical_value_t> batch_results;
    };

    using kernel_init_fn = core::result_wrapper_t<kernel_state_ptr> (*)(kernel_context&, kernel_init_args);

    class compute_kernel {
    public:
        explicit compute_kernel(kernel_signature_t signature, kernel_init_fn init = nullptr);
        virtual ~compute_kernel() = default;

        const kernel_signature_t& signature() const { return signature_; }

        [[nodiscard]] kernel_target target() const noexcept { return target_; }
        void set_target(kernel_target t) noexcept { target_ = t; }
        [[nodiscard]] bool is_gpu_target() const noexcept { return target_ != kernel_target::cpu; }

        void set_target_data(kernel_target_data_ptr data) noexcept { target_data_ = std::move(data); }
        [[nodiscard]] const kernel_target_data_t* target_data() const noexcept { return target_data_.get(); }

        core::result_wrapper_t<kernel_state_ptr> init(kernel_context& ctx, const kernel_init_args& args) const;

    protected:
        kernel_signature_t signature_;
        kernel_init_fn init_;
        kernel_target target_{kernel_target::cpu};
        kernel_target_data_ptr target_data_;
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

    using aggregate_consume_fn = core::error_t (*)(kernel_context& ctx, const vector::data_chunk_t& input);
    using aggregate_merge_fn = core::error_t (*)(aggregate_kernel_context& ctx,
                                                 kernel_state&& next_state,
                                                 kernel_state& prev_state);
    using aggregate_finalize_fn = core::error_t (*)(aggregate_kernel_context& ctx);

    class aggregate_kernel : public compute_kernel {
    public:
        aggregate_kernel(kernel_signature_t signature,
                         kernel_init_fn init,
                         aggregate_consume_fn consume,
                         aggregate_merge_fn merge,
                         aggregate_finalize_fn finalize);

        core::error_t consume(kernel_context& ctx, const vector::data_chunk_t& input) const;
        core::error_t merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state& into) const;
        core::error_t finalize(aggregate_kernel_context& ctx) const;

    private:
        aggregate_consume_fn consume_;
        aggregate_merge_fn merge_;
        aggregate_finalize_fn finalize_;
    };

    using row_exec_fn = core::error_t (*)(kernel_context& ctx,
                                          const std::pmr::vector<types::logical_value_t>& inputs,
                                          std::pmr::vector<types::logical_value_t>& output);

    class row_kernel : public compute_kernel {
    public:
        row_kernel(kernel_signature_t signature, row_exec_fn exec);

        core::error_t execute(kernel_context& ctx,
                              const std::pmr::vector<types::logical_value_t>& inputs,
                              std::pmr::vector<types::logical_value_t>& output) const;

    private:
        row_exec_fn exec_;
    };

} // namespace components::compute
