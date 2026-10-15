#pragma once

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/vector.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <common/typedefs.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix { namespace util {

    // `declared` is the column's type: only a declared DOUBLE changes the answer (an integer or FLOAT cell
    // of such a column comes back as float). Errors are RuntimeErrors.
    py::object cell_to_python(const components::vector::vector_t& column,
                              uint64_t row,
                              const components::types::complex_logical_type& declared);

    py::dict cursor_row_to_python_dict(components::cursor::cursor_t_ptr& cursor,
                                       uint64_t row_idx,
                                       const std::vector<components::table::column_definition_t>& col_defs);
}} // namespace otterbrix::util
