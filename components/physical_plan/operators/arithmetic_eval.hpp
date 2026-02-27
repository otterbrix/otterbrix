#pragma once

#include <deque>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/arithmetic.hpp>

namespace components::operators {

    namespace detail {

        vector::arithmetic_op scalar_to_arithmetic_op(expressions::scalar_type t);

        struct resolved_operand {
            const vector::vector_t* vec = nullptr;
            std::optional<types::logical_value_t> scalar;
        };

        std::pair<resolved_operand, std::string> resolve_operand(
            const expressions::param_storage& param,
            vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            std::pmr::memory_resource* resource,
            std::deque<vector::vector_t>& temp_vecs);

        // Resolve a param_storage to logical_value_t for a specific row
        types::logical_value_t resolve_row_value(
            std::pmr::memory_resource* resource,
            const expressions::param_storage& param,
            const vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            size_t row_idx);

        // Evaluate a compare_expression for a specific row
        bool evaluate_row_condition(
            std::pmr::memory_resource* resource,
            const expressions::expression_ptr& condition,
            const vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params,
            size_t row_idx);

        // Evaluate a CASE expression per-row on a data_chunk
        vector::vector_t evaluate_case_expr(
            std::pmr::memory_resource* resource,
            const std::pmr::vector<expressions::param_storage>& operands,
            vector::data_chunk_t& chunk,
            const logical_plan::storage_parameters& params);

    } // namespace detail

    // Evaluate an arithmetic expression (or CASE) on a data chunk.
    // Returns {result_vector, error_message}. Empty error = success.
    std::pair<vector::vector_t, std::string> evaluate_arithmetic(
        std::pmr::memory_resource* resource,
        expressions::scalar_type op,
        const std::pmr::vector<expressions::param_storage>& operands,
        vector::data_chunk_t& chunk,
        const logical_plan::storage_parameters& params);

} // namespace components::operators