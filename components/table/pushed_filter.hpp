#pragma once

#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/expression.hpp>
#include <components/table/column_state.hpp>
#include <components/vector/storage_parameters.hpp>
#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>

namespace components::table {

    // The storage's own filter of a WHERE it reads with: everything it holds lives on `resource` (the parameters
    // are copied there), and it is built for the column `types` of the rows it filters.
    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<table_filter_t>>
    build_table_filter(std::pmr::memory_resource* resource,
                       const expressions::expression_ptr& expression,
                       const vector::storage_parameters& parameters,
                       const graph_execution_context& context,
                       const std::pmr::vector<types::complex_logical_type>& types);

} // namespace components::table
