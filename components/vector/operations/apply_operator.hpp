#pragma once

#include <components/execution_context/graph_execution_context.hpp>
#include <components/operators/operator_code.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <compare>

// While we do not have proper operator registry, this is a place for default operators (comparison and arithmetics)
namespace components::vector::operations {

    // both have to be of the same type and comparable
    [[nodiscard]] std::partial_ordering
    compare_cells(const vector_t& left, uint64_t left_row, const vector_t& right, uint64_t right_row);

    [[nodiscard]] core::error_t apply_binary(operators::operator_code code,
                                             const vector_t& left,
                                             const vector_t& right,
                                             vector_t* output,
                                             const graph_execution_context& context,
                                             uint64_t count,
                                             // One bool per row, or null for "every row". A false
                                             // row is left untouched: the count never changes.
                                             const bool* active_rows = nullptr);

    [[nodiscard]] core::error_t apply_unary(operators::operator_code code,
                                            const vector_t& operand,
                                            vector_t* output,
                                            const graph_execution_context& context,
                                            uint64_t count);

    [[nodiscard]] bool is_comparison(operators::operator_code code) noexcept;

} // namespace components::vector::operations
