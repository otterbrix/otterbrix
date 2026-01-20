#pragma once

#include "compute_result.hpp"
#include "kernel_signature.hpp"
#include "kernel_utils.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <functional>
#include <memory>
#include <vector>

namespace components::compute {
    class compute_kernel;

    // originally, arrow-compute's datum is a variant<scalar, vector<T>, data_chunk>.
    // in our implementation scalar holding is not required, vector is held inside data_chunk (which is vector<vector>)
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

    using kernel_init_fn = std::function<compute_result<kernel_state_ptr>(kernel_context&, const kernel_init_args&)>;

    class compute_kernel {
    public:
        explicit compute_kernel(kernel_signature_t signature, kernel_init_fn init = nullptr);
        virtual ~compute_kernel() = default;

        const kernel_signature_t& signature() const { return signature_; }
        compute_result<kernel_state_ptr> init(kernel_context& ctx, const kernel_init_args& args) const;

    protected:
        kernel_signature_t signature_;
        kernel_init_fn init_;
    };

    using vector_exec_fn = std::function<compute_status(kernel_context& ctx,
                                                        const vector::data_chunk_t& inputs,
                                                        size_t exec_length,
                                                        vector::vector_t& output)>;

    // datum are results aggregated over batches
    using vector_finalize_fn = std::function<compute_status(kernel_context& ctx, size_t exec_length, datum_t& output)>;

    class vector_kernel : public compute_kernel {
    public:
        vector_kernel(kernel_signature_t signature,
                      vector_exec_fn exec,
                      kernel_init_fn init = nullptr,
                      vector_finalize_fn finalize = nullptr);

        compute_status execute(kernel_context& ctx,
                               const vector::data_chunk_t& inputs,
                               size_t exec_length,
                               vector::vector_t& output) const;
        compute_status finalize(kernel_context& ctx, size_t exec_length, datum_t& output) const;

    private:
        vector_exec_fn exec_;
        vector_finalize_fn finalize_;
    };

    using aggregate_consume_fn =
        std::function<compute_status(kernel_context& ctx, const vector::data_chunk_t& input, size_t exec_length)>;
    using aggregate_merge_fn =
        std::function<compute_status(kernel_context& ctx, kernel_state&& next_state, kernel_state& prev_state)>;
    using aggregate_finalize_fn = std::function<compute_status(kernel_context& ctx, datum_t& output)>;

    class aggregate_kernel : public compute_kernel {
    public:
        aggregate_kernel(kernel_signature_t signature,
                         kernel_init_fn init,
                         aggregate_consume_fn consume,
                         aggregate_merge_fn merge,
                         aggregate_finalize_fn finalize);

        compute_status consume(kernel_context& ctx, const vector::data_chunk_t& input, size_t exec_length) const;
        compute_status merge(kernel_context& ctx, kernel_state&& from, kernel_state& into) const;
        compute_status finalize(kernel_context& ctx, datum_t& output) const;

    private:
        aggregate_consume_fn consume_;
        aggregate_merge_fn merge_;
        aggregate_finalize_fn finalize_;
    };

} // namespace components::compute
