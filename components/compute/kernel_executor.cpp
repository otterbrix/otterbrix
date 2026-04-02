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

    protected:
        vector_t prepare_vector_output(size_t length) {
            assert(kernel_ctx_);
            return vector_t(exec_ctx().resource(), output_type_, length);
        }

        [[nodiscard]] core::error_t check_kernel() const {
            if (!kernel_ctx_) {
                // TODO: find another way to get memory_resource
                return core::error_t(core::error_code_t::kernel_error,
                                     core::error_tag_t::compute_kernel,
                                     std::pmr::string{"Kernel context is null, init() method must be called first!",
                                                      std::pmr::get_default_resource()});
            }

            if (!kernel_) {
                return core::error_t(core::error_code_t::kernel_error,
                                     core::error_tag_t::compute_kernel,
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

            if (auto st = execute_batch(inputs); st.contains_error()) {
                return st;
            }

            data_chunk_t out(kernel_ctx().exec_context().resource(), {});
            out.data.emplace_back(std::move(results_.front()));
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
                if (auto st = execute_batch(in); st.contains_error()) {
                    return st;
                }
            }

            // fuse all vectors into one
            for (auto&& res : results_) {
                merged.data.emplace_back(std::move(res));
            }

            if (auto st = kernel().finalize(kernel_ctx(), merged); st.contains_error()) {
                return st;
            }

            return merged;
        }

        core::result_wrapper_t<datum_t> execute(const std::pmr::vector<logical_value_t>& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
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

            if (auto st = execute_batch(single_row); st.contains_error()) {
                return st;
            }

            data_chunk_t out(exec_ctx().resource(), {});
            out.data.emplace_back(std::move(results_.front()));
            if (auto st = kernel().finalize(kernel_ctx(), out); st.contains_error()) {
                return st;
            }

            std::pmr::vector<logical_value_t> result(exec_ctx().resource());
            result.push_back(out.data.front().value(0));
            return result;
        }

    private:
        core::error_t execute_batch(const data_chunk_t& inputs) {
            auto output = prepare_vector_output(inputs.size());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); st.contains_error()) {
                return st;
            }

            results_.emplace_back(std::move(output));
            return core::error_t::no_error();
        }

        std::vector<vector_t> results_;
    };

    class aggregate_executor final : public kernel_executor_impl<aggregate_kernel> {
    public:
        core::error_t init(kernel_context& kernel_ctx, kernel_init_args args) override {
            if (auto st = kernel_executor_impl<aggregate_kernel>::init(kernel_ctx, args); st.contains_error()) {
                return st;
            }
            // wrap provided context with an aggregate-specific one
            agg_ctx_.emplace(kernel_ctx.exec_context(), kernel_ctx.kernel());
            agg_ctx_->set_state(kernel_ctx.state());
            kernel_ctx_ = &*agg_ctx_;
            input_types_ = &args.inputs;
            options_ = args.options;
            return core::error_t::no_error();
        }

        core::result_wrapper_t<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            if (auto st = consume(inputs); st.contains_error()) {
                return st;
            }

            if (auto st = kernel().finalize(*agg_ctx_); st.contains_error()) {
                return st;
            }
            return agg_ctx_->batch_results;
        }

        core::result_wrapper_t<datum_t> execute(const std::vector<vector::data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            agg_ctx_->batch_results.reserve(inputs.size());
            for (const auto& in : inputs) {
                if (auto st = consume(in); st.contains_error()) {
                    return st;
                }
            }

            if (auto st = kernel().finalize(*agg_ctx_); st.contains_error()) {
                return st;
            }

            // resize to expected count — kernel may have written fewer or more
            agg_ctx_->batch_results.resize(
                inputs.size(),
                types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA));
            return agg_ctx_->batch_results;
        }

        core::result_wrapper_t<datum_t> execute(const std::pmr::vector<logical_value_t>&) override {
            return core::error_t(core::error_code_t::kernel_error,
                                 core::error_tag_t::compute_kernel,
                                 std::pmr::string{"vector_executor does not support row operations",
                                                  kernel_ctx_->exec_context().resource()});
        }

    private:
        core::error_t consume(const data_chunk_t& inputs) {
            // TODO: find another way of getting memory_resource
            if (state() == nullptr) {
                core::error_t(core::error_code_t::kernel_error,
                              core::error_tag_t::compute_kernel,
                              std::pmr::string{"Aggregation requires non-null kernel state, init returned null state!",
                                               std::pmr::get_default_resource()});
            }

            auto batch_state = kernel().init(*agg_ctx_, {kernel(), *input_types_, options_});
            if (batch_state.has_error()) {
                return batch_state.error();
            }

            if (batch_state.value() == nullptr) {
                core::error_t(core::error_code_t::kernel_error,
                              core::error_tag_t::compute_kernel,
                              std::pmr::string{"Aggregation requires non-null kernel state, init returned null state!",
                                               kernel_ctx_->exec_context().resource()});
            }

            kernel_context batch_ctx(exec_ctx(), kernel());
            batch_ctx.set_state(batch_state.value().get());
            if (auto st = kernel().consume(batch_ctx, inputs); st.contains_error()) {
                return st;
            }

            auto state_ptr = std::move(batch_state.value());
            if (auto st = kernel().merge(*agg_ctx_, std::move(*state_ptr), *state()); st.contains_error()) {
                return st;
            }

            return core::error_t::no_error();
        }

        std::optional<aggregate_kernel_context> agg_ctx_;
        const std::pmr::vector<types::complex_logical_type>* input_types_ = nullptr;
        const function_options* options_ = nullptr;
    };

    class row_executor final : public kernel_executor_impl<row_kernel> {
    public:
        core::result_wrapper_t<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            std::pmr::vector<logical_value_t> results(exec_ctx().resource());
            results.reserve(inputs.size());

            if (auto st = execute_chunk(inputs, results); st.contains_error()) {
                return st;
            }
            return results;
        }

        core::result_wrapper_t<datum_t> execute(const std::vector<data_chunk_t>& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            std::pmr::vector<logical_value_t> results(exec_ctx().resource());
            size_t total = 0;
            for (const auto& chunk : inputs) {
                total += chunk.size();
            }
            results.reserve(total);

            for (const auto& chunk : inputs) {
                if (auto st = execute_chunk(chunk, results); st.contains_error()) {
                    return st;
                }
            }
            return results;
        }

        core::result_wrapper_t<datum_t> execute(const std::pmr::vector<logical_value_t>& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            std::pmr::vector<logical_value_t> output(inputs.get_allocator().resource());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); st.contains_error()) {
                return st;
            }

            return output;
        }

    private:
        core::error_t execute_chunk(const data_chunk_t& chunk, std::pmr::vector<logical_value_t>& results) {
            for (size_t i = 0; i < chunk.size(); ++i) {
                std::pmr::vector<logical_value_t> row_in(exec_ctx().resource());
                row_in.reserve(chunk.column_count());

                for (size_t j = 0; j < chunk.column_count(); ++j) {
                    row_in.emplace_back(chunk.value(j, i));
                }

                std::pmr::vector<logical_value_t> row_out(exec_ctx().resource());
                if (auto st = kernel().execute(kernel_ctx(), row_in, row_out); st.contains_error()) {
                    return st;
                }

                // row_kernel contract: one scalar output per call
                if (!row_out.empty()) {
                    results.emplace_back(std::move(row_out.front()));
                }
            }
            return core::error_t::no_error();
        }
    };

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_vector() { return std::make_unique<vector_executor>(); }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_aggregate() {
        return std::make_unique<aggregate_executor>();
    }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_row() { return std::make_unique<row_executor>(); }
} // namespace components::compute::detail
