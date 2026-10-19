#pragma once

#include <components/logical_plan/param_storage.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>

// The parameter `id` of `node` holds `expected`: same type, same cell.
inline bool parameter_equals(const components::logical_plan::parameter_node_t& node,
                             core::parameter_id_t id,
                             const components::vector::vector_t& expected) {
    const auto& actual = node.parameter(id);
    return actual.type() == expected.type() && components::vector::cells_equal(actual, 0, expected, 0);
}

// Row `row` of `column` is the one-row `expected`: same type, same cell.
inline bool
cell_equals(const components::vector::vector_t& column, uint64_t row, const components::vector::vector_t& expected) {
    return column.type() == expected.type() && components::vector::cells_equal(column, row, expected, 0);
}
