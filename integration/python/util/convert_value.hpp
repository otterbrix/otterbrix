#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <components/types/logical_value.hpp>
#include <components/table/column_definition.hpp>
#include <components/cursor/cursor.hpp>
#include <components/vector/data_chunk.hpp>

#include <common/typedefs.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {
	namespace util {

		py::object logical_value_to_python(const components::types::logical_value_t& value,
				const components::types::complex_logical_type& type);

		py::dict cursor_row_to_python_dict(components::cursor::cursor_t_ptr& cursor,
				uint64_t row_idx,
				const std::vector<components::table::column_definition_t>& col_defs);

		py::dict data_chunk_row_to_python_dict(const components::vector::data_chunk_t& chunk,
				uint64_t row_idx,
				const std::vector<components::table::column_definition_t>& col_defs);
	}

} // namespace otterbrix
