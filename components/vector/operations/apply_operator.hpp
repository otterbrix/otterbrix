#pragma once

#include <components/execution_context/graph_execution_context.hpp>
#include <components/operators/operator_code.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

// While we do not have proper operator registry, this is a place for default operators (comparison and arithmetics)
namespace components::vector::operations {

    [[nodiscard]] core::error_t apply_binary(operators::operator_code code,
                                             const vector_t& left,
                                             const vector_t& right,
                                             vector_t* output,
                                             const graph_execution_context& context,
                                             uint64_t count);

    [[nodiscard]] core::error_t apply_unary(operators::operator_code code,
                                            const vector_t& operand,
                                            vector_t* output,
                                            const graph_execution_context& context,
                                            uint64_t count);

    [[nodiscard]] bool is_comparison(operators::operator_code code) noexcept;

} // namespace components::vector::operations
