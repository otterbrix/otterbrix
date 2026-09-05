#include "kernel_executor.hpp"

#include <optional>
#include <string>

using namespace components::types;
using namespace components::vector;

namespace components::compute::detail {
    // kernel_executor_impl is a non-owning executor, init() MUST be called before execute()
    template<typename KernelType>
    class kernel_executor_impl : public kernel_executor_t {
    public:
        explicit kernel_executor_impl(std::pmr::memory_resource* resource)
            : resource_(resource) {
            assert(resource_);
        }

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
        // A refusal raised before init() has run has no kernel_context to borrow a resource
        // from, so it words itself with the resource the executor was built with rather than
        // reaching for the process-wide default.
        [[nodiscard]] std::pmr::memory_resource* error_resource() const {
            return kernel_ctx_ ? kernel_ctx_->exec_context().resource() : resource_;
        }

        [[nodiscard]] core::error_t not_accumulating() const {
            return core::error_t(
                core::error_code_t::kernel_error,
                std::pmr::string{"this kernel does not accumulate across chunks", error_resource()});
        }

        vector_t prepare_vector_output(size_t length) {
            assert(kernel_ctx_);
            return vector_t(exec_ctx().resource(), output_type_, length);
        }

        [[nodiscard]] core::error_t check_kernel() const {
            if (!kernel_ctx_) {
                return core::error_t(core::error_code_t::kernel_error,

                                     std::pmr::string{"Kernel context is null, init() method must be called first!",
                                                      error_resource()});
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

        std::pmr::memory_resource* resource_ = nullptr;
        kernel_context* kernel_ctx_ = nullptr;
        const KernelType* kernel_ = nullptr;
        complex_logical_type output_type_;
    };

    class vector_executor final : public kernel_executor_impl<vector_kernel> {
    public:
        using kernel_executor_impl<vector_kernel>::kernel_executor_impl;

        [[nodiscard]] core::result_wrapper_t<datum_t> execute(const data_chunk_t& inputs) override {
            if (auto st = check_kernel(); st.contains_error()) {
                return st;
            }

            std::pmr::vector<vector_t> results(exec_ctx().resource());
            if (auto st = execute_batch(inputs, results); st.contains_error()) {
                return st;
            }

            data_chunk_t out(kernel_ctx().exec_context().resource(), {});
            out.data.emplace_back(std::move(results.front()));
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

            // The per-chunk outputs are fused side by side, one column each, so the fused chunk
            // has a row count only when every input chunk is the same height. Ragged input has no
            // honest count to report, so refuse rather than stamp one and mislabel the rest.
            //
            // The refusal is deliberately narrow, not a claim about what batching SHOULD mean:
            // set_cardinality below is not optional (a chunk with no count reports zero rows to
            // everyone who asks size()), and a count has to be defensible. It rejects exactly the
            // inputs for which the fuse-as-columns shape has no correct answer, so whenever that
            // shape is revisited the refusal goes away with the code it guards.
            const uint64_t rows = inputs.front().size();
            for (const auto& in : inputs) {
                if (in.size() != rows) {
                    return core::error_t(
                        core::error_code_t::kernel_error,
                        std::pmr::string{"a fused vector batch needs every chunk to hold the same number of rows",
                                         exec_ctx().resource()});
                }
            }

            std::pmr::vector<vector_t> results(exec_ctx().resource());
            results.reserve(inputs.size());
            for (const auto& in : inputs) {
                if (auto st = execute_batch(in, results); st.contains_error()) {
                    return st;
                }
            }

            // fuse all vectors into one
            for (auto&& res : results) {
                merged.data.emplace_back(std::move(res));
            }
            merged.set_cardinality(rows);

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

            std::pmr::vector<vector_t> results(exec_ctx().resource());
            if (auto st = execute_batch(single_row, results); st.contains_error()) {
                return st;
            }

            data_chunk_t out(exec_ctx().resource(), {});
            out.data.emplace_back(std::move(results.front()));
            out.set_cardinality(1);
            if (auto st = kernel().finalize(kernel_ctx(), out); st.contains_error()) {
                return st;
            }

            std::pmr::vector<logical_value_t> result(exec_ctx().resource());
            result.push_back(out.data.front().value(0));
            return result;
        }

    private:
        // The produced vectors are a scratch buffer of ONE call, so the caller owns them and
        // they must not live in a member: an executor is cached per function node and driven
        // chunk after chunk, so a member nothing clears answers the second chunk with the
        // moved-from remains of the first.
        core::error_t execute_batch(const data_chunk_t& inputs, std::pmr::vector<vector_t>& results) {
            auto output = prepare_vector_output(inputs.size());
            if (auto st = kernel().execute(kernel_ctx(), inputs, output); st.contains_error()) {
                return st;
            }

            results.emplace_back(std::move(output));
            return core::error_t::no_error();
        }
    };

    class aggregate_executor final : public kernel_executor_impl<aggregate_kernel> {
    public:
        using kernel_executor_impl<aggregate_kernel>::kernel_executor_impl;

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

        core::result_wrapper_t<datum_t> execute(const std::pmr::vector<logical_value_t>&) override {
            return not_directly_executable();
        }

    private:
        // An aggregate reduces many rows into per-group accumulators, so it is driven by
        // update()/finalize() and never produces a value straight out of one call.
        [[nodiscard]] core::error_t not_directly_executable() const {
            return core::error_t(
                core::error_code_t::kernel_error,
                std::pmr::string{"an aggregate is driven by update()/finalize(), not execute()", error_resource()});
        }

        const std::pmr::vector<types::complex_logical_type>* input_types_ = nullptr;
    };

    class row_executor final : public kernel_executor_impl<row_kernel> {
    public:
        using kernel_executor_impl<row_kernel>::kernel_executor_impl;

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
        // The row_kernel contract is exactly one scalar per row, and it is FOREIGN code that has
        // to honour it: a UDF in this project is a row_function carrying a user-supplied
        // row_exec_fn. A break must not be swallowed here - skipping a row the kernel left
        // empty, or dropping surplus values past front(), makes `results` come back shorter than
        // the chunk while the call still reports success. execution_dag walks that vector by row
        // index and only guards it with an assert(), so under -DNDEBUG the rows the kernel never
        // answered keep whatever the output vector held before. Refuse, loudly and by row.
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

                if (row_out.size() != 1) {
                    return core::error_t(
                        core::error_code_t::kernel_error,
                        std::pmr::string{"a row kernel must produce exactly one value per row, but row " +
                                             std::to_string(i) + " of the chunk produced " +
                                             std::to_string(row_out.size()),
                                         exec_ctx().resource()});
                }

                results.emplace_back(std::move(row_out.front()));
            }
            return core::error_t::no_error();
        }
    };

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_vector(std::pmr::memory_resource* resource) {
        return std::make_unique<vector_executor>(resource);
    }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_aggregate(std::pmr::memory_resource* resource) {
        return std::make_unique<aggregate_executor>(resource);
    }

    std::unique_ptr<kernel_executor_t> kernel_executor_t::make_row(std::pmr::memory_resource* resource) {
        return std::make_unique<row_executor>(resource);
    }
} // namespace components::compute::detail
