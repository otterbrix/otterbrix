#include "function_predicate.hpp"
#include "utils.hpp"

using namespace components;
using namespace components::operators::predicates;

namespace {
    // build a chunk of N rows where column c of row k holds the value returned
    // by arg_getter[c](left, right, left_indices[k], right_indices[k]), types are inferred from the first row.
    vector::data_chunk_t build_batch_chunk(std::pmr::memory_resource* resource,
                                           const std::pmr::vector<impl::value_getter>& getters,
                                           const vector::data_chunk_t& left,
                                           const vector::data_chunk_t& right,
                                           const vector::indexing_vector_t& left_indices,
                                           const vector::indexing_vector_t& right_indices,
                                           uint64_t count) {
        const size_t num_args = getters.size();

        std::pmr::vector<types::logical_value_t> first_vals(resource);
        first_vals.reserve(num_args);
        for (const auto& g : getters) {
            first_vals.emplace_back(g(left, right, left_indices.get_index(0), right_indices.get_index(0)));
        }

        std::pmr::vector<types::complex_logical_type> col_types(resource);
        col_types.reserve(num_args);
        for (const auto& v : first_vals) {
            col_types.emplace_back(v.type());
        }

        vector::data_chunk_t batch(resource, col_types, count);
        for (size_t c = 0; c < num_args; ++c) {
            batch.set_value(static_cast<uint64_t>(c), 0, first_vals[c]);
        }
        for (uint64_t k = 1; k < count; ++k) {
            for (size_t c = 0; c < num_args; ++c) {
                batch.set_value(static_cast<uint64_t>(c),
                                k,
                                getters[c](left, right, left_indices.get_index(k), right_indices.get_index(k)));
            }
        }
        batch.set_cardinality(count);
        return batch;
    }

    std::vector<bool>
    run_batch_and_extract_bools(const compute::function* function, vector::data_chunk_t& batch, size_t N) {
        auto res = function->execute(batch);
        if (res.status() != compute::compute_status::ok()) {
            throw std::runtime_error("batch function predicate failed: " + res.status().message());
        }
        std::vector<bool> results(N);
        if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(res.value())) {
            const auto& values = std::get<std::pmr::vector<types::logical_value_t>>(res.value());
            if (values.size() < N) {
                throw std::runtime_error("batch function predicate: function returned fewer results than inputs");
            }
            for (size_t k = 0; k < N; ++k) {
                results[k] = values[k].value<bool>();
            }
        } else {
            // vector_function returns data_chunk_t; result column is data[0]
            const auto& chunk = std::get<vector::data_chunk_t>(res.value());
            if (chunk.data.empty() || chunk.size() < N) {
                throw std::runtime_error("batch function predicate: function returned fewer results than inputs");
            }
            for (size_t k = 0; k < N; ++k) {
                results[k] = chunk.data.front().value(k).value<bool>();
            }
        }
        return results;
    }

    function_predicate::batch_check_fn_t make_batch_func(std::pmr::memory_resource* resource,
                                                         std::pmr::vector<impl::value_getter> getters,
                                                         const compute::function* function) {
        return [resource, getters = std::move(getters), function](const vector::data_chunk_t& left,
                                                                  const vector::data_chunk_t& right,
                                                                  const vector::indexing_vector_t& left_indices,
                                                                  const vector::indexing_vector_t& right_indices,
                                                                  uint64_t count) -> std::vector<bool> {
            if (count == 0) {
                return {};
            }
            auto batch = build_batch_chunk(resource, getters, left, right, left_indices, right_indices, count);
            return run_batch_and_extract_bools(function, batch, count);
        };
    }
} // namespace

namespace components::operators::predicates {
    function_predicate::function_predicate(row_check_fn_t func)
        : func_(std::move(func)) {}

    function_predicate::function_predicate(row_check_fn_t func, batch_check_fn_t batch_func)
        : func_(std::move(func))
        , batch_func_(std::move(batch_func)) {}

    bool function_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                        const vector::data_chunk_t& chunk_right,
                                        size_t index_left,
                                        size_t index_right) {
        return func_(chunk_left, chunk_right, index_left, index_right);
    }

    std::vector<bool> function_predicate::batch_check_impl(const vector::data_chunk_t& left,
                                                           const vector::data_chunk_t& right,
                                                           const vector::indexing_vector_t& left_indices,
                                                           const vector::indexing_vector_t& right_indices,
                                                           uint64_t count) {
        if (batch_func_) {
            return batch_func_(left, right, left_indices, right_indices, count);
        }

        std::vector<bool> results(count); // fallback: row-by-row via existing closure
        for (uint64_t k = 0; k < count; ++k) {
            results[k] = func_(left, right, left_indices.get_index(k), right_indices.get_index(k));
        }
        return results;
    }

    predicate_ptr create_complex_function_predicate(std::pmr::memory_resource* resource,
                                                    const compute::function_registry_t* function_registry,
                                                    const expressions::function_expression_ptr& expr,
                                                    const logical_plan::storage_parameters* parameters) {
        std::pmr::vector<impl::value_getter> arg_getters(resource);
        arg_getters.reserve(expr->args().size());
        for (const auto& arg : expr->args()) {
            arg_getters.emplace_back(impl::create_value_getter(resource, function_registry, arg, parameters));
        }
        const auto* function = function_registry->get_function(expr->function_uid());

        // copy for batch (original moved into row closure below)
        auto batch_func = make_batch_func(resource, arg_getters, function);

        function_predicate::row_check_fn_t row_func =
            [resource, arg_getters = std::move(arg_getters), function](const vector::data_chunk_t& left,
                                                                       const vector::data_chunk_t& right,
                                                                       size_t left_index,
                                                                       size_t right_index) {
                std::pmr::vector<types::logical_value_t> args(resource);
                args.reserve(arg_getters.size());
                for (const auto& getter : arg_getters) {
                    args.emplace_back(getter(left, right, left_index, right_index));
                }
                auto res = function->execute(args);
                if (!res) {
                    throw std::runtime_error(res.status().message());
                }
                return std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0].value<bool>();
            };
        return {new function_predicate(std::move(row_func), std::move(batch_func))};
    }

    predicate_ptr create_function_predicate(std::pmr::memory_resource* resource,
                                            const compute::function_registry_t* function_registry,
                                            const expressions::function_expression_ptr& expr,
                                            const logical_plan::storage_parameters* parameters) {
        // if any of the function arguments is a function call, we have to use
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                return create_complex_function_predicate(resource, function_registry, expr, parameters);
            }
        }

        const auto* function = function_registry->get_function(expr->function_uid());
        std::pmr::vector<impl::value_getter> arg_getters(resource);
        arg_getters.reserve(expr->args().size());
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::key_t>(arg)) {
                arg_getters.emplace_back(impl::create_value_getter(std::get<expressions::key_t>(arg)));
            } else {
                arg_getters.emplace_back(impl::create_value_getter(std::get<core::parameter_id_t>(arg), parameters));
            }
        }

        function_predicate::row_check_fn_t row_func =
            [resource, expr, function, parameters](const vector::data_chunk_t& left,
                                                   const vector::data_chunk_t& right,
                                                   size_t left_index,
                                                   size_t right_index) {
                std::pmr::vector<types::logical_value_t> args(resource);
                args.reserve(expr->args().size());
                for (const auto& arg : expr->args()) {
                    if (std::holds_alternative<expressions::key_t>(arg)) {
                        const auto& key = std::get<expressions::key_t>(arg);
                        args.emplace_back(key.side() == expressions::side_t::left
                                              ? left.at(key.path())->value(left_index)
                                              : right.at(key.path())->value(right_index));
                    } else {
                        args.emplace_back(parameters->parameters.at(std::get<core::parameter_id_t>(arg)));
                    }
                }
                auto res = function->execute(args);
                if (!res) {
                    throw std::runtime_error(res.status().message());
                }
                return std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0].value<bool>();
            };
        return {
            new function_predicate(std::move(row_func), make_batch_func(resource, std::move(arg_getters), function))};
    }

} // namespace components::operators::predicates