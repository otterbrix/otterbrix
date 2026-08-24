#include "kernel_executor.hpp"

#include <optional>

using namespace components::types;
using namespace components::vector;

namespace components::compute::detail {
    // kernel_executor_impl is a non-owning executor, init() MUST be called before execute()
    template<typename KernelType>
    class kernel_executor_impl : public kernel_executor_t {
    public:
        kernel_executor_impl() = default;

        core::error_t init(kernel_context& kernel_ctx, kernel_init_args args) override {
            kernel_ctx_ = &kernel_ctx;
            kernel_ = static_cast<const KernelType*>(&args.kernel);

            // TODO: support multiple output types
            auto out =
                kernel_->signature().output_types.front().resolve(kernel_ctx_->exec_context().resource(), args.inputs);
            if (out.has_error()) {
                return out.error();
            }

            output_type_ = out.value();
            return core::error_t::no_error();
        }

        [[nodiscard]] aggregate_state_layout_t state_layout() const override { return {}; }

        [[nodiscard]] core::error_t
        update(const data_chunk_t&, core::span<const uint32_t>, aggregate_states_t) override {
            return not_accumulating();
        }

        [[nodiscard]] core::error_t finalize(aggregate_states_t, uint64_t, uint64_t, vector_t&) override {
            return not_accumulating();
        }

    protected:
        [[nodiscard]] core::error_t not_accumulating() const {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"this kernel does not accumulate across chunks",
                                                  kernel_ctx_ ? kernel_ctx_->exec_context().resource()
                                                              : std::pmr::get_default_resource()});
        }

        vector_t prepare_vector_output(size_t length) {
            assert(kernel_ctx_);
            return vector_t(exec_ctx().resource(), output_type_, length);
        }

        [[nodiscard]] core::error_t check_kernel() const {
            if (!kernel_ctx_) {
                // TODO: find another way to get memory_resource
                return core::error_t(core::error_code_t::kernel_error,

                                     std::pmr::string{"Kernel context is null, init() method must be called first!",
                                                      std::pmr::get_default_resource()});
            }

            if (!kernel_) {
                return core::error_t(core::error_code_t::kernel_error,

                                     std::pmr::string{"Kernel is null, init() method must be called first!",
                                                      kernel_ctx_->exec_context().resource()});
            }

            return core::error_t::no_error();
        }

        inline const KernelType& kernel() const {
            assert(kernel_);
            return *kernel_;
        }

        inline kernel_context& kernel_ctx() const {
            assert(kernel_ctx_);
            return *kernel_ctx_;
        }

        inline exec_context_t& exec_ctx() const { return kernel_ctx().exec_context(); }

        inline kernel_state* state() const { return kernel_ctx().state(); }

        kernel_context* kernel_ctx_ = nullptr;
        const KernelType* kernel_ = nullptr;
        complex_logical_type output_type_;
    };

    class vector_executor final : public kernel_executor_impl<vector_kernel> {
    public:
        [[nodiscard]] core::result_wrapper_t<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            auto produced = execute_batch(inputs);
            if (produced.has_error()) {
                return produced.error();
            }

            data_chunk_t out(kernel_ctx().exec_context().resource(), {});
            out.data.emplace_back(std::move(produced.value()));
            out.set_cardinality(inputs.size());
            if (auto st = kernel().finalize(kernel_ctx(), out); st.contains_error()) {
                return st;
            }
            return out;
        }

        [[nodiscard]] core::result_wrapper_t<datum_t> execute(const std::vector<data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            data_chunk_t merged(exec_ctx().resource(), {});
            if (inputs.empty()) {
                return merged;
            }

            for (const auto& in : inputs) {
                auto produced = execute_batch(in);
                if (produced.has_error()) {
                    return produced.error();
                }
                merged.data.emplace_back(std::move(produced.value()));
            }

            if (auto st = kernel().finalize(kernel_ctx(), merged); st.contains_error()) {
                return st;
            }

            return merged;
        }

    private:
        core::result_wrapper_t<vector_t> execute_batch(const data_chunk_t& inputs) {
            auto output = prepare_vector_output(inputs.size());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); st.contains_error()) {
                return st;
            }
            return output;
        }
    };

    class aggregate_executor final : public kernel_executor_impl<aggregate_kernel> {
    public:
        core::error_t init(kernel_context& kernel_ctx, kernel_init_args args) override {
            if (auto st = kernel_executor_impl<aggregate_kernel>::init(kernel_ctx, args); st.contains_error()) {
                return st;
            }
            input_types_ = &args.inputs;
            return core::error_t::no_error();
        }

        aggregate_state_layout_t state_layout() const override {
            if (!kernel_ || input_types_ == nullptr) {
                return {};
            }
            return kernel().state_layout(*input_types_);
        }

        core::error_t
        update(const data_chunk_t& inputs, core::span<const uint32_t> groups, aggregate_states_t states) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }
            if (groups.size() != inputs.size()) {
                return core::error_t(
                    core::error_code_t::kernel_error,
                    std::pmr::string{"an aggregate update needs one group id per input row", exec_ctx().resource()});
            }
            return kernel().update(kernel_ctx(), inputs, groups, states);
        }

        core::error_t finalize(aggregate_states_t states, uint64_t first, uint64_t count, vector_t& output) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }
            return kernel().finalize(kernel_ctx(), states, first, count, output);
        }

        core::result_wrapper_t<datum_t> execute(const data_chunk_t&) override { return not_directly_executable(); }

        core::result_wrapper_t<datum_t> execute(const std::vector<vector::data_chunk_t>&) override {
            return not_directly_executable();
        }

    private:
        // An aggregate reduces many rows into per-group accumulators, so it is driven by
        // update()/finalize() and never produces a value straight out of one call.
        [[nodiscard]] core::error_t not_directly_executable() const {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"an aggregate is driven by update()/finalize(), not execute()",
                                                  kernel_ctx_ ? kernel_ctx_->exec_context().resource()
                                                              : std::pmr::get_default_resource()});
        }

        const std::pmr::vector<types::complex_logical_type>* input_types_ = nullptr;
    };

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_vector() { return std::make_unique<vector_executor>(); }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_aggregate() {
        return std::make_unique<aggregate_executor>();
    }
} // namespace components::compute::detail
