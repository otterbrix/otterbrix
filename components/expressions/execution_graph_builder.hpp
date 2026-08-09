#pragma once

#include <components/execution_graph/execution_graph.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/types/parameter_map.hpp>
#include <core/result_wrapper.hpp>
#include <core/span.hpp>

namespace components::expressions {

    [[nodiscard]] core::result_wrapper_t<execution_graph::slot_id_t>
    build_expression(execution_graph::execution_graph_t* graph,
                     const types::parameter_map_t& parameters,
                     const expression_i* expression,
                     const std::pmr::vector<types::complex_logical_type>& input_types,
                     size_t right_offset = 0);

    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_graph(std::pmr::memory_resource* resource,
                const types::parameter_map_t& parameters,
                core::span<const expression_i* const> expressions,
                const std::pmr::vector<types::complex_logical_type>& input_types,
                size_t right_offset = 0);

    // N+1 outputs (one per SET value, then a trailing bool 'is_modified')
    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_update_graph(std::pmr::memory_resource* resource,
                       const types::parameter_map_t& parameters,
                       core::span<const expression_i* const> values,
                       const std::pmr::vector<types::complex_logical_type>& input_types,
                       size_t right_offset = 0);

    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_condition_graph(std::pmr::memory_resource* resource,
                          const types::parameter_map_t& parameters,
                          const expression_i* expression,
                          const std::pmr::vector<types::complex_logical_type>& input_types,
                          size_t right_offset = 0);

    [[nodiscard]] core::result_wrapper_t<vector::data_chunk_t>
    run_graph(execution_graph::execution_graph_t* graph,
              const types::parameter_map_t& parameters,
              const vector::data_chunk_t& input,
              const components::graph_execution_context& context);

} // namespace components::expressions