#include "operator_function.hpp"

#include <components/context/context.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::operators {

    operator_function_t::operator_function_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             compute::function_uid uid,
                                             std::pmr::vector<expressions::param_storage> args,
                                             std::string result_alias)
        : read_only_operator_t(resource, std::move(log), operator_type::function)
        , uid_(uid)
        , args_(std::move(args))
        , result_alias_(std::move(result_alias)) {}

    void operator_function_t::reset_pipeline_state() noexcept {
        materialized_ = false;
        cursor_ = 0;
        output_ = nullptr;
    }

    core::error_t operator_function_t::materialize_(pipeline::context_t* ctx) {
        if (!ctx->function_registry) {
            return core::error_t(core::error_code_t::create_physical_plan_error,
                                 std::pmr::string{"table function: no function registry in context", resource_});
        }

        // Resolve the argument values for the single (per-run) input row.
        std::pmr::vector<types::logical_value_t> arg_values(resource_);
        arg_values.reserve(args_.size());
        for (const auto& arg : args_) {
            if (std::holds_alternative<core::parameter_id_t>(arg)) {
                arg_values.emplace_back(
                    logical_plan::get_parameter(&ctx->parameters, std::get<core::parameter_id_t>(arg)));
            } else {
                // A column-ref argument is a correlated (LATERAL) reference; resolving
                // it needs an outer row, which only the LATERAL-join path provides.
                return core::error_t(
                    core::error_code_t::create_physical_plan_error,
                    std::pmr::string{"table function: correlated arguments require LATERAL (not yet supported)",
                                     resource_});
            }
        }

        std::pmr::vector<types::complex_logical_type> arg_types(resource_);
        arg_types.reserve(arg_values.size());
        for (const auto& value : arg_values) {
            arg_types.emplace_back(value.type());
        }
        vector::data_chunk_t args(resource_, arg_types, 1);
        for (size_t col = 0; col < arg_values.size(); ++col) {
            args.set_value(col, 0, arg_values[col]);
        }
        args.set_cardinality(1);

        auto* function = ctx->function_registry->get_function(uid_);
        if (!function) {
            return core::error_t(core::error_code_t::create_physical_plan_error,
                                 std::pmr::string{"table function: uid not found in registry", resource_});
        }
        auto kernel_res = function->dispatch_exact(resource_, arg_types);
        if (kernel_res.has_error()) {
            return kernel_res.error();
        }
        const auto& kernel = static_cast<const compute::expand_kernel&>(kernel_res.value().get());

        auto out_type_res = kernel.signature().output_types.front().resolve(resource_, arg_types);
        if (out_type_res.has_error()) {
            return out_type_res.error();
        }
        types::complex_logical_type out_type = out_type_res.value();
        out_type.set_alias(result_alias_);

        compute::exec_context_t exec_ctx(resource_,
                                         const_cast<compute::function_registry_t*>(ctx->function_registry));
        compute::kernel_context kernel_ctx(exec_ctx, kernel);
        std::pmr::vector<vector::data_chunk_t> produced(resource_);
        if (auto err = kernel.execute(kernel_ctx, args, produced); err.contains_error()) {
            return err;
        }

        chunks_vector_t chunks(resource_);
        if (produced.empty()) {
            // No rows produced: still emit one schema'd 0-row chunk so downstream sees
            // the output column type (mirrors operator_raw_data's 0-row VALUES guard).
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(out_type);
            vector::data_chunk_t empty(resource_, types, 1);
            empty.set_cardinality(0);
            chunks.emplace_back(std::move(empty));
        } else {
            for (auto& chunk : produced) {
                chunk.data[0].set_type_alias(result_alias_);
                chunks.emplace_back(std::move(chunk));
            }
        }
        output_ = make_operator_data(resource_, std::move(chunks));
        materialized_ = true;
        return core::error_t::no_error();
    }

    vector::data_chunk_t operator_function_t::make_drain_chunk() {
        std::pmr::vector<types::complex_logical_type> empty_types(resource_);
        return vector::data_chunk_t{resource_, empty_types, 0};
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_function_t::source_next(pipeline::context_t* ctx) {
        if (!materialized_) {
            if (auto err = materialize_(ctx); err.contains_error()) {
                co_return core::result_wrapper_t<vector::data_chunk_t>(err);
            }
        }

        const auto& chunks = output_->chunks();
        if (cursor_ < chunks.size()) {
            const auto& chunk = chunks[cursor_];
            ++cursor_;
            co_return core::result_wrapper_t<vector::data_chunk_t>(chunk.partial_copy(resource_, 0, chunk.size()));
        }
        co_return core::result_wrapper_t<vector::data_chunk_t>(make_drain_chunk());
    }

} // namespace components::operators
