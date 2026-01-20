#include "kernel_executor.hpp"

using namespace components::types;
using namespace components::vector;

namespace components::compute::detail {
    // kernel_executor_impl is a non-owning executor, init() MUST be called before execute()
    template<typename KernelType>
    class kernel_executor_impl : public kernel_executor_t {
    public:
        kernel_executor_impl() = default;

        compute_status init(kernel_context& kernel_ctx, kernel_init_args args) override {
            kernel_ctx_ = &kernel_ctx;
            kernel_ = static_cast<const KernelType*>(&args.kernel);

            auto out = kernel_->signature().output_type.resolve(args.inputs);
            if (!out) {
                return compute_status::execution_error("Failed to resolve function type");
            }

            output_type_ = out.value();
            return compute_status::ok();
        }

    protected:
        vector_t prepare_vector_output(size_t length) {
            assert(kernel_ctx_);
            return vector_t(exec_ctx().resource(), output_type_, length);
        }

        compute_status check_kernel() const {
            if (!kernel_) {
                return compute_status::invalid("Kernel is null, init() method must be called first!");
            }

            if (!kernel_ctx_) {
                return compute_status::invalid("Kernel context is null, init() method must be called first!");
            }

            return compute_status::ok();
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
        compute_result<datum_t> execute(const data_chunk_t& inputs, size_t exec_length) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            if (auto st = execute_batch(inputs, exec_length); !st) {
                return st;
            }

            datum_t out(kernel_ctx().exec_context().resource(), {});
            out.data.emplace_back(std::move(results_.front()));
            if (auto st = kernel().finalize(kernel_ctx(), exec_length, out); !st) {
                return st;
            }
            return out;
        }

        compute_result<datum_t> execute(const std::vector<data_chunk_t>& inputs, size_t exec_length) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            data_chunk_t merged(exec_ctx().resource(), {});
            if (inputs.empty()) {
                return merged;
            }

            for (const auto& in : inputs) {
                if (auto st = execute_batch(in, exec_length); !st) {
                    return st;
                }
            }

            // fuse all vectors into one
            for (auto&& res : results_) {
                merged.data.emplace_back(std::move(res));
            }

            if (auto st = kernel().finalize(kernel_ctx(), exec_length, merged); !st) {
                return st;
            }

            return merged;
        }

    private:
        compute_status execute_batch(const data_chunk_t& inputs, size_t exec_length) {
            auto output = prepare_vector_output(exec_length);
            if (auto st = kernel().execute(kernel_ctx(), inputs, exec_length, output); !st) {
                return st;
            }

            results_.emplace_back(std::move(output));
            return compute_status::ok();
        }

        std::vector<vector_t> results_;
    };

    class aggregate_executor final : public kernel_executor_impl<aggregate_kernel> {
    public:
        compute_status init(kernel_context& kernel_ctx, kernel_init_args args) override {
            input_types_ = &args.inputs;
            options_ = args.options;
            return kernel_executor_impl<aggregate_kernel>::init(kernel_ctx, args);
        }

        compute_result<datum_t> execute(const data_chunk_t& inputs, size_t exec_length) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            if (auto st = consume(inputs, exec_length); !st) {
                return st;
            }

            return finalize();
        }

        compute_result<datum_t> execute(const std::vector<vector::data_chunk_t>& inputs, size_t exec_length) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            for (const auto& in : inputs) {
                if (auto st = consume(in, exec_length); !st) {
                    return st;
                }
            }

            return finalize();
        }

    private:
        compute_status consume(const data_chunk_t& inputs, size_t exec_length) {
            if (state() == nullptr) {
                return compute_status::invalid("Aggregation requires non-null kernel state, init returned null state!");
            }

            auto batch_state = kernel().init(kernel_ctx(), {kernel(), *input_types_, options_});
            if (!batch_state) {
                return batch_state.status();
            }

            if (batch_state.value() == nullptr) {
                return compute_status::invalid("Aggregation requires non-null kernel state, init returned null state!");
            }

            kernel_context batch_ctx(exec_ctx(), kernel());
            batch_ctx.set_state(batch_state.value().get());
            if (auto st = kernel().consume(batch_ctx, inputs, exec_length); !st) {
                return st;
            }

            auto state_ptr = std::move(batch_state.value());
            if (auto st = kernel().merge(kernel_ctx(), std::move(*state_ptr), *state()); !st) {
                return st;
            }

            return compute_status::ok();
        }

        compute_result<datum_t> finalize() {
            datum_t out(kernel_ctx().exec_context().resource(), {});
            if (auto st = kernel().finalize(kernel_ctx(), out); !st) {
                return st;
            }

            return out;
        }

        const std::pmr::vector<types::complex_logical_type>* input_types_;
        const function_options* options_;
    };

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_vector() { return std::make_unique<vector_executor>(); }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_aggregate() {
        return std::make_unique<aggregate_executor>();
    }
} // namespace components::compute::detail
