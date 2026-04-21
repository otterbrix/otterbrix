#include "operator_func.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/arithmetic_eval.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>

namespace {
    using namespace components;
    using namespace components::operators::aggregate;
    using column_it = decltype(vector::data_chunk_t::data)::const_iterator;
    using columns_var = std::variant<column_it, types::logical_value_t>;

    // Pre-compute any arithmetic expression arguments, returns false (sets error) on failure
    bool compute_expression_args(std::pmr::memory_resource* resource,
                                 const std::pmr::vector<expressions::param_storage>& args,
                                 vector::data_chunk_t& chunk,
                                 operator_func_t& op,
                                 pipeline::context_t* pipeline_context,
                                 std::vector<vector::vector_t>& computed_vecs) {
        for (const auto& arg : args) {
            if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                auto& expr = std::get<expressions::expression_ptr>(arg);
                if (expr->group() == expressions::expression_group::scalar) {
                    auto* scalar_expr = static_cast<const expressions::scalar_expression_t*>(expr.get());
                    auto res = operators::evaluate_arithmetic(resource,
                                                              scalar_expr->type(),
                                                              scalar_expr->params(),
                                                              chunk,
                                                              pipeline_context->parameters);
                    if (res.has_error()) {
                        op.set_error(res.error());
                        return false;
                    }
                    computed_vecs.emplace_back(std::move(res.value()));
                }
            }
        }
        return true;
    }

    void resolve_columns(const std::pmr::vector<expressions::param_storage>& args,
                         vector::data_chunk_t& chunk,
                         pipeline::context_t* pipeline_context,
                         std::pmr::vector<columns_var>& columns,
                         std::vector<vector::vector_t>& computed_vecs) {
        size_t computed_idx = 0;
        for (const auto& arg : args) {
            if (std::holds_alternative<expressions::key_t>(arg)) {
                const auto& key = std::get<expressions::key_t>(arg);
                assert(!key.path().empty() && "aggregate key path must be resolved");
                assert(key.path().front() < chunk.data.size() && "aggregate key path out of range");
                columns.emplace_back(chunk.data.begin() + static_cast<std::ptrdiff_t>(key.path().front()));
            } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                const auto& id = std::get<core::parameter_id_t>(arg);
                columns.emplace_back(pipeline_context->parameters.parameters.at(id));
            } else if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                if (computed_idx < computed_vecs.size()) {
                    chunk.data.emplace_back(std::move(computed_vecs[computed_idx]));
                    auto it = chunk.data.end() - 1;
                    columns.emplace_back(static_cast<column_it>(it));
                    computed_idx++;
                }
            }
        }
    }

    vector::data_chunk_t build_arg_chunk(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<columns_var>& columns,
                                         const vector::data_chunk_t& chunk) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto& it : columns) {
            if (std::holds_alternative<column_it>(it)) {
                types.emplace_back(std::get<column_it>(it)->type());
            } else {
                types.emplace_back(std::get<types::logical_value_t>(it).type());
            }
        }
        vector::data_chunk_t c(resource, types, chunk.size());
        c.set_cardinality(chunk.size());
        for (size_t i = 0; i < c.column_count(); i++) {
            if (std::holds_alternative<column_it>(columns.at(i))) {
                c.data[i].reference(*std::get<column_it>(columns.at(i)));
            } else {
                c.data[i].reference(std::get<types::logical_value_t>(columns.at(i)));
                c.data[i].flatten(vector::indexing_vector_t(resource, chunk.size()), chunk.size());
            }
        }
        return c;
    }

    void apply_distinct(std::pmr::memory_resource* resource,
                        vector::data_chunk_t& c,
                        const std::pmr::vector<types::complex_logical_type>& types) {
        if (c.size() == 0 || c.column_count() == 0) {
            return;
        }
        std::pmr::vector<uint64_t> unique_indices(resource);
        unique_indices.reserve(c.size());
        for (uint64_t row = 0; row < c.size(); row++) {
            bool dup = false;
            for (auto idx : unique_indices) {
                if (c.data[0].value(row) == c.data[0].value(idx)) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                unique_indices.push_back(row);
            }
        }
        vector::indexing_vector_t indexing(resource, unique_indices.data());
        vector::data_chunk_t unique_c(resource, types, unique_indices.size());
        c.copy(unique_c, indexing, unique_indices.size(), 0);
        c = std::move(unique_c);
    }
} // namespace

namespace components::operators::aggregate {
    operator_func_t::operator_func_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     compute::function* func,
                                     std::pmr::vector<expressions::param_storage> args,
                                     bool distinct)
        : operator_aggregate_t(resource, std::move(log))
        , args_(std::move(args))
        , func_(func)
        , distinct_(distinct) {
        assert(func);
    }

    core::result_wrapper_t<types::logical_value_t>
    operator_func_t::aggregate_impl(pipeline::context_t* pipeline_context) {
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA);
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();

            std::vector<vector::vector_t> computed_vecs;
            if (!compute_expression_args(left_->output()->resource(),
                                         args_,
                                         chunk,
                                         *this,
                                         pipeline_context,
                                         computed_vecs)) {
                return result;
            }

            std::pmr::vector<columns_var> columns(left_->output()->resource());
            columns.reserve(args_.size());
            resolve_columns(args_, chunk, pipeline_context, columns, computed_vecs);

            if (columns.size() == args_.size()) {
                auto c = build_arg_chunk(left_->output()->resource(), columns, chunk);
                auto types = c.types();
                if (distinct_) {
                    apply_distinct(resource_, c, types);
                }
                auto res = func_->execute(c);
                if (res.has_error()) {
                    return res.convert_error<types::logical_value_t>();
                } else {
                    result = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
                }
            }
        }
        result.set_alias(func_->name());
        return result;
    }

    core::result_wrapper_t<compute::datum_t>
    operator_func_t::aggregate_batch_impl(pipeline::context_t* pipeline_context) {
        auto& batch_chunks = left_->output()->chunks();
        std::vector<vector::data_chunk_t> arg_chunks;
        arg_chunks.reserve(batch_chunks.size());

        for (auto& chunk : batch_chunks) {
            std::vector<vector::vector_t> computed_vecs;
            if (!compute_expression_args(resource_, args_, chunk, *this, pipeline_context, computed_vecs)) {
                // error already set — return empty
                return compute::datum_t{std::pmr::vector<types::logical_value_t>(resource_)};
            }

            std::pmr::vector<columns_var> columns(resource_);
            columns.reserve(args_.size());
            resolve_columns(args_, chunk, pipeline_context, columns, computed_vecs);

            if (columns.size() == args_.size()) {
                auto c = build_arg_chunk(resource_, columns, chunk);
                auto types = c.types();
                if (distinct_) {
                    apply_distinct(resource_, c, types);
                }
                arg_chunks.push_back(std::move(c));
            }
        }

        auto res = func_->execute(arg_chunks);
        if (res.has_error()) {
            return res;
        }

        if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(res.value())) {
            auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(res.value());
            for (auto& v : vals) {
                v.set_alias(func_->name());
            }
        }
        return std::move(res.value());
    }

    std::string operator_func_t::key_impl() const { return func_->name(); }

} // namespace components::operators::aggregate
