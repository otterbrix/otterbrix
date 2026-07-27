#include "function_predicate.hpp"
#include "utils.hpp"

using namespace components;
using namespace components::operators::predicates;

namespace {
    // build a chunk of N rows where column c of row k holds the value returned by
    // arg_getter[c](left, right, left_indices[k], right_indices[k]). Each column's type is inferred
    // from its first NON-NULL value across the batch (a NULL row 0 must not poison the column type),
    // falling back to whatever row 0 reports when the whole column is NULL. `any_arg_null[k]` is set
    // when any argument of row k is NULL, so the caller can force that row's predicate to UNKNOWN
    // (predicate functions -- LIKE / regex / comparisons -- are strict: NULL in, UNKNOWN out).
    core::result_wrapper_t<vector::data_chunk_t> build_batch_chunk(std::pmr::memory_resource* resource,
                                                                   const std::pmr::vector<impl::value_getter>& getters,
                                                                   const vector::data_chunk_t& left,
                                                                   const vector::data_chunk_t& right,
                                                                   const vector::indexing_vector_t& left_indices,
                                                                   const vector::indexing_vector_t& right_indices,
                                                                   uint64_t count,
                                                                   std::vector<bool>& any_arg_null) {
        const size_t num_args = getters.size();

        // Materialize every argument value first so a column can be typed from its first non-NULL.
        // The outer container is a plain std::vector (a pmr vector-of-pmr-vectors would drag the
        // allocator into each element's construction and fail uses-allocator resolution).
        std::vector<std::pmr::vector<types::logical_value_t>> cols;
        cols.reserve(num_args);
        for (size_t c = 0; c < num_args; ++c) {
            cols.emplace_back(resource);
            cols[c].reserve(count);
        }
        any_arg_null.assign(count, false);
        for (uint64_t k = 0; k < count; ++k) {
            for (size_t c = 0; c < num_args; ++c) {
                auto res = getters[c](left, right, left_indices.get_index(k), right_indices.get_index(k));
                if (res.has_error()) {
                    return res.convert_error<vector::data_chunk_t>();
                }
                if (res.value().is_null()) {
                    any_arg_null[k] = true;
                }
                cols[c].emplace_back(std::move(res.value()));
            }
        }

        std::pmr::vector<types::complex_logical_type> col_types(resource);
        col_types.reserve(num_args);
        for (size_t c = 0; c < num_args; ++c) {
            const types::logical_value_t* typed = nullptr;
            for (const auto& v : cols[c]) {
                if (!v.is_null()) {
                    typed = &v;
                    break;
                }
            }
            col_types.emplace_back(typed ? typed->type() : cols[c].front().type());
        }

        vector::data_chunk_t batch(resource, col_types, count);
        for (uint64_t k = 0; k < count; ++k) {
            for (size_t c = 0; c < num_args; ++c) {
                batch.set_value(static_cast<uint64_t>(c), k, cols[c][k]);
            }
        }
        batch.set_cardinality(count);
        return batch;
    }

    // Extract the function's boolean output as a tri-state, forcing UNKNOWN for rows whose input was
    // NULL (strict predicate) or whose result the function itself returned as NULL.
    core::result_wrapper_t<std::vector<types::tri_bool_t>>
    run_batch_and_extract_tri(const compute::function* function,
                              vector::data_chunk_t& batch,
                              size_t N,
                              const std::vector<bool>& any_arg_null) {
        auto res = function->execute(batch);
        if (res.has_error()) {
            return res.convert_error<std::vector<types::tri_bool_t>>();
        }
        std::vector<types::tri_bool_t> results(N);
        if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(res.value())) {
            const auto& values = std::get<std::pmr::vector<types::logical_value_t>>(res.value());
            if (values.size() < N) {
                return core::error_t(
                    core::error_code_t::incorrect_function_return_type,
                    std::pmr::string{"batch function predicate: function returned fewer results than inputs",
                                     batch.resource()});
            }
            for (size_t k = 0; k < N; ++k) {
                results[k] = types::tri_of(values[k].value<bool>(), any_arg_null[k] || values[k].is_null());
            }
        } else {
            // vector_function returns data_chunk_t; result column is data[0]
            const auto& chunk = std::get<vector::data_chunk_t>(res.value());
            if (chunk.data.empty() || chunk.size() < N) {
                return core::error_t(
                    core::error_code_t::incorrect_function_return_type,
                    std::pmr::string{"batch function predicate: function returned fewer results than inputs",
                                     batch.resource()});
            }
            const auto& out_col = chunk.data.front();
            for (size_t k = 0; k < N; ++k) {
                // Not the two-arg tri_of: an invalid row's slot may be uninitialized, so the
                // get_value read must stay behind the short-circuiting guard.
                const bool null_out = any_arg_null[k] || !out_col.validity().row_is_valid(k);
                results[k] = null_out ? types::tri_bool_t::unknown : types::tri_of(out_col.get_value<bool>(k));
            }
        }
        return results;
    }

    function_predicate::batch_check_fn_t make_batch_func(std::pmr::memory_resource* resource,
                                                         std::pmr::vector<impl::value_getter> getters,
                                                         const compute::function* function) {
        return [resource, getters = std::move(getters), function](
                   const vector::data_chunk_t& left,
                   const vector::data_chunk_t& right,
                   const vector::indexing_vector_t& left_indices,
                   const vector::indexing_vector_t& right_indices,
                   uint64_t count) -> core::result_wrapper_t<std::vector<types::tri_bool_t>> {
            if (count == 0) {
                return std::vector<types::tri_bool_t>{};
            }
            std::vector<bool> any_arg_null;
            auto batch =
                build_batch_chunk(resource, getters, left, right, left_indices, right_indices, count, any_arg_null);
            if (batch.has_error()) {
                return batch.convert_error<std::vector<types::tri_bool_t>>();
            } else {
                return run_batch_and_extract_tri(function, batch.value(), count, any_arg_null);
            }
        };
    }
} // namespace

namespace components::operators::predicates {
    function_predicate::function_predicate(row_check_fn_t func)
        : func_(std::move(func)) {}

    function_predicate::function_predicate(row_check_fn_t func, batch_check_fn_t batch_func)
        : func_(std::move(func))
        , batch_func_(std::move(batch_func)) {}

    core::result_wrapper_t<types::tri_bool_t> function_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                                                             const vector::data_chunk_t& chunk_right,
                                                                             size_t index_left,
                                                                             size_t index_right) {
        return func_(chunk_left, chunk_right, index_left, index_right);
    }

    core::result_wrapper_t<std::vector<types::tri_bool_t>>
    function_predicate::batch_check_impl(const vector::data_chunk_t& left,
                                         const vector::data_chunk_t& right,
                                         const vector::indexing_vector_t& left_indices,
                                         const vector::indexing_vector_t& right_indices,
                                         uint64_t count) {
        if (batch_func_) {
            return batch_func_(left, right, left_indices, right_indices, count);
        }

        std::vector<types::tri_bool_t> results(count); // fallback: row-by-row via existing closure
        for (uint64_t k = 0; k < count; ++k) {
            auto res = func_(left, right, left_indices.get_index(k), right_indices.get_index(k));
            if (res.has_error()) {
                return res.convert_error<std::vector<types::tri_bool_t>>();
            } else {
                results[k] = res.value();
            }
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
            [resource, arg_getters = std::move(arg_getters), function](
                const vector::data_chunk_t& left,
                const vector::data_chunk_t& right,
                size_t left_index,
                size_t right_index) -> core::result_wrapper_t<types::tri_bool_t> {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(arg_getters.size());
            for (const auto& getter : arg_getters) {
                auto res = getter(left, right, left_index, right_index);
                if (res.has_error()) {
                    return res.convert_error<types::tri_bool_t>();
                }
                // Strict predicate: a NULL argument makes the result UNKNOWN, without executing.
                if (res.value().is_null()) {
                    return types::tri_bool_t::unknown;
                }
                args.emplace_back(std::move(res.value()));
            }
            auto res = function->execute(args);
            if (res.has_error()) {
                return res.convert_error<types::tri_bool_t>();
            }
            const auto& out = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
            return types::tri_of(out.value<bool>(), out.is_null());
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
                arg_getters.emplace_back(impl::create_value_getter(resource, std::get<expressions::key_t>(arg)));
            } else {
                arg_getters.emplace_back(
                    impl::create_value_getter(resource, std::get<core::parameter_id_t>(arg), parameters));
            }
        }

        function_predicate::row_check_fn_t row_func =
            [resource, expr, function, parameters](const vector::data_chunk_t& left,
                                                   const vector::data_chunk_t& right,
                                                   size_t left_index,
                                                   size_t right_index) -> core::result_wrapper_t<types::tri_bool_t> {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(expr->args().size());
            for (const auto& arg : expr->args()) {
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    args.emplace_back(key.side() == expressions::side_t::left
                                          ? left.value(key.path(), left_index)
                                          : right.value(key.path(), right_index));
                } else {
                    args.emplace_back(parameters->parameters.at(std::get<core::parameter_id_t>(arg)));
                }
                // Strict predicate: a NULL argument makes the result UNKNOWN, without executing.
                if (args.back().is_null()) {
                    return types::tri_bool_t::unknown;
                }
            }
            auto res = function->execute(args);
            if (res.has_error()) {
                return res.convert_error<types::tri_bool_t>();
            }
            const auto& out = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
            return types::tri_of(out.value<bool>(), out.is_null());
        };
        return {
            new function_predicate(std::move(row_func), make_batch_func(resource, std::move(arg_getters), function))};
    }

} // namespace components::operators::predicates