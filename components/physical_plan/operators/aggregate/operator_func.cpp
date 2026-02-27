#include "operator_func.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/arithmetic_eval.hpp>

namespace components::operators::aggregate {

    operator_func_t::operator_func_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     compute::function* func,
                                     std::pmr::vector<expressions::param_storage> args)
        : operator_aggregate_t(resource, std::move(log))
        , args_(std::move(args))
        , func_(func) {
        assert(func);
    }

    types::logical_value_t operator_func_t::aggregate_impl(pipeline::context_t* pipeline_context) {
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA);
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();
            using column_it = decltype(vector::data_chunk_t::data)::const_iterator;
            using columns_var = std::variant<column_it, types::logical_value_t>;

            // Pre-compute any arithmetic expression arguments
            std::vector<vector::vector_t> computed_vecs;
            for (const auto& arg : args_) {
                if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                    auto& expr = std::get<expressions::expression_ptr>(arg);
                    if (expr->group() == expressions::expression_group::scalar) {
                        auto* scalar_expr =
                            static_cast<const expressions::scalar_expression_t*>(expr.get());
                        std::string arith_error;
                        auto computed = operators::evaluate_arithmetic(
                            left_->output()->resource(),
                            scalar_expr->type(),
                            scalar_expr->params(),
                            chunk,
                            pipeline_context->parameters,
                            &arith_error);
                        if (!arith_error.empty()) {
                            set_error(arith_error);
                            return result;
                        }
                        computed_vecs.emplace_back(std::move(computed));
                    }
                }
            }

            std::pmr::vector<columns_var> columns(left_->output()->resource());
            columns.reserve(args_.size());
            size_t computed_idx = 0;
            for (const auto& arg : args_) {
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                        return v.type().alias() == key.as_string();
                    });
                    if (it != chunk.data.end()) {
                        columns.emplace_back(it);
                    } else {
                        break;
                    }
                } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                    const auto& id = std::get<core::parameter_id_t>(arg);
                    columns.emplace_back(pipeline_context->parameters.parameters.at(id));
                } else if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                    // Use the pre-computed vector - add it to chunk data temporarily
                    if (computed_idx < computed_vecs.size()) {
                        chunk.data.emplace_back(std::move(computed_vecs[computed_idx]));
                        auto it = chunk.data.end() - 1;
                        columns.emplace_back(
                            static_cast<decltype(vector::data_chunk_t::data)::const_iterator>(it));
                        computed_idx++;
                    }
                }
            }
            if (columns.size() == args_.size()) {
                if (columns.empty()) {
                    // COUNT(*) — no specific columns, just need the cardinality
                    std::pmr::vector<types::complex_logical_type> dummy_types(left_->output()->resource());
                    dummy_types.emplace_back(types::logical_type::UBIGINT);
                    vector::data_chunk_t c(left_->output()->resource(), dummy_types, chunk.size());
                    c.set_cardinality(chunk.size());
                    auto res = func_->execute(c, c.size());
                    if (res.status() == compute::compute_status::ok()) {
                        result = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
                    }
                } else {
                    std::pmr::vector<types::complex_logical_type> types(left_->output()->resource());
                    types.reserve(columns.size());
                    for (const auto& it : columns) {
                        if (std::holds_alternative<column_it>(it)) {
                            types.emplace_back(std::get<column_it>(it)->type());
                        } else {
                            types.emplace_back(std::get<types::logical_value_t>(it).type());
                        }
                    }
                    vector::data_chunk_t c(left_->output()->resource(), types, chunk.size());
                    c.set_cardinality(chunk.size());
                    for (size_t i = 0; i < c.column_count(); i++) {
                        if (std::holds_alternative<column_it>(columns.at(i))) {
                            c.data[i].reference(*std::get<column_it>(columns.at(i)));
                        } else {
                            c.data[i].reference(std::get<types::logical_value_t>(columns.at(i)));
                            c.data[i].flatten(
                                vector::indexing_vector_t(left_->output()->resource(), chunk.size()),
                                chunk.size());
                        }
                    }
                    auto res = func_->execute(c, c.size());
                    if (res.status() == compute::compute_status::ok()) {
                        result = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
                    }
                }
            }
        }
        result.set_alias(func_->name());
        return result;
    }

    std::string operator_func_t::key_impl() const { return func_->name(); }

} // namespace components::operators::aggregate
