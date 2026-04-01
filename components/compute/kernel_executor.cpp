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

            // TODO: support multiple output types
            auto out = kernel_->signature().output_types.front().resolve(args.inputs);
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
        compute_result<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            if (auto st = execute_batch(inputs); !st) {
                return st;
            }

            data_chunk_t out(kernel_ctx().exec_context().resource(), {});
            out.data.emplace_back(std::move(results_.front()));
            if (auto st = kernel().finalize(kernel_ctx(), out); !st) {
                return st;
            }
            return compute_result{datum_t{std::move(out)}};
        }

        compute_result<datum_t> execute(const std::vector<data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            data_chunk_t merged(exec_ctx().resource(), {});
            if (inputs.empty()) {
                return compute_result{datum_t{std::move(merged)}};
            }

            for (const auto& in : inputs) {
                if (auto st = execute_batch(in); !st) {
                    return st;
                }
            }

            // fuse all vectors into one
            for (auto&& res : results_) {
                merged.data.emplace_back(std::move(res));
            }

            if (auto st = kernel().finalize(kernel_ctx(), merged); !st) {
                return st;
            }

            return compute_result{datum_t{std::move(merged)}};
        }

        compute_result<datum_t> execute(const std::pmr::vector<logical_value_t>& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            std::pmr::vector<complex_logical_type> types(exec_ctx().resource());
            types.reserve(inputs.size());
            for (const auto& v : inputs) {
                types.emplace_back(v.type());
            }

            data_chunk_t single_row(exec_ctx().resource(), types, static_cast<uint64_t>(1));
            for (size_t i = 0; i < inputs.size(); ++i) {
                single_row.set_value(static_cast<uint64_t>(i), 0, inputs[i]);
            }
            single_row.set_cardinality(1);

            if (auto st = execute_batch(single_row); !st) {
                return st;
            }

            data_chunk_t out(exec_ctx().resource(), {});
            out.data.emplace_back(std::move(results_.front()));
            if (auto st = kernel().finalize(kernel_ctx(), out); !st) {
                return st;
            }

            std::pmr::vector<logical_value_t> result(exec_ctx().resource());
            result.push_back(out.data.front().value(0));
            return compute_result{datum_t{std::move(result)}};
        }

    private:
        compute_status execute_batch(const data_chunk_t& inputs) {
            auto output = prepare_vector_output(inputs.size());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); !st) {
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
            if (auto st = kernel_executor_impl<aggregate_kernel>::init(kernel_ctx, args); !st) {
                return st;
            }
            // wrap provided context with an aggregate-specific one
            agg_ctx_.emplace(kernel_ctx.exec_context(), kernel_ctx.kernel());
            agg_ctx_->set_state(kernel_ctx.state());
            kernel_ctx_ = &*agg_ctx_;
            input_types_ = &args.inputs;
            options_ = args.options;
            return compute_status::ok();
        }

        compute_result<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            if (auto st = consume(inputs); !st) {
                return st;
            }

            if (auto st = kernel().finalize(*agg_ctx_); !st) {
                return st;
            }
            return compute_result{datum_t{std::move(agg_ctx_->batch_results)}};
        }

        compute_result<datum_t> execute(const std::vector<vector::data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            agg_ctx_->batch_results.reserve(inputs.size());
            for (const auto& in : inputs) {
                if (auto st = consume(in); !st) {
                    return st;
                }
            }

            if (auto st = kernel().finalize(*agg_ctx_); !st) {
                return st;
            }

            // resize to expected count — kernel may have written fewer or more
            agg_ctx_->batch_results.resize(
                inputs.size(),
                types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA));
            return compute_result{datum_t{std::move(agg_ctx_->batch_results)}};
        }

        compute_result<datum_t> execute(const std::pmr::vector<logical_value_t>&) override {
            return compute_result<datum_t>{
                compute_status::not_implemented("aggregate_executor does not support row operations")};
        }

    private:
        compute_status consume(const data_chunk_t& inputs) {
            if (state() == nullptr) {
                return compute_status::invalid("Aggregation requires non-null kernel state, init returned null state!");
            }

            auto batch_state = kernel().init(*agg_ctx_, {kernel(), *input_types_, options_});
            if (!batch_state) {
                return batch_state.status();
            }

            if (batch_state.value() == nullptr) {
                return compute_status::invalid("Aggregation requires non-null kernel state, init returned null state!");
            }

            kernel_context batch_ctx(exec_ctx(), kernel());
            batch_ctx.set_state(batch_state.value().get());
            if (auto st = kernel().consume(batch_ctx, inputs); !st) {
                return st;
            }

            auto state_ptr = std::move(batch_state.value());
            if (auto st = kernel().merge(*agg_ctx_, std::move(*state_ptr), *state()); !st) {
                return st;
            }

            return compute_status::ok();
        }

        std::optional<aggregate_kernel_context> agg_ctx_;
        const std::pmr::vector<types::complex_logical_type>* input_types_ = nullptr;
        const function_options* options_ = nullptr;
    };

    class row_executor final : public kernel_executor_impl<row_kernel> {
    public:
        compute_result<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            std::pmr::vector<logical_value_t> results(exec_ctx().resource());
            results.reserve(inputs.size());

            if (auto st = execute_chunk(inputs, results); !st) {
                return st;
            }
            return compute_result{datum_t{std::move(results)}};
        }

        compute_result<datum_t> execute(const std::vector<data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            std::pmr::vector<logical_value_t> results(exec_ctx().resource());
            size_t total = 0;
            for (const auto& chunk : inputs) {
                total += chunk.size();
            }
            results.reserve(total);

            for (const auto& chunk : inputs) {
                if (auto st = execute_chunk(chunk, results); !st) {
                    return st;
                }
            }
            return compute_result{datum_t{std::move(results)}};
        }

        compute_result<datum_t> execute(const std::pmr::vector<logical_value_t>& inputs) override {
            if (auto st = check_kernel(); !st) {
                return st;
            }

            std::pmr::vector<logical_value_t> output(inputs.get_allocator().resource());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); !st) {
                return st;
            }

            return compute_result{datum_t{std::move(output)}};
        }

    private:
        compute_status execute_chunk(const data_chunk_t& chunk, std::pmr::vector<logical_value_t>& results) {
            for (size_t i = 0; i < chunk.size(); ++i) {
                std::pmr::vector<logical_value_t> row_in(exec_ctx().resource());
                row_in.reserve(chunk.column_count());

                for (size_t j = 0; j < chunk.column_count(); ++j) {
                    row_in.emplace_back(chunk.value(j, i));
                }

                std::pmr::vector<logical_value_t> row_out(exec_ctx().resource());
                if (auto st = kernel().execute(kernel_ctx(), row_in, row_out); !st) {
                    return st;
                }

                // row_kernel contract: one scalar output per call
                if (!row_out.empty()) {
                    results.emplace_back(std::move(row_out.front()));
                }
            }
            return compute_status::ok();
        }
    };

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_vector() { return std::make_unique<vector_executor>(); }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_aggregate() {
        return std::make_unique<aggregate_executor>();
    }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_row() { return std::make_unique<row_executor>(); }
} // namespace components::compute::detail
