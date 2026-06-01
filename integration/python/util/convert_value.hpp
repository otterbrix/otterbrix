#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <components/types/logical_value.hpp>
#include <components/table/column_definition.hpp>
#include <components/cursor/cursor.hpp>
#include <components/vector/data_chunk.hpp>

#include <core/types/vector.hpp>
#include <core/types/string.hpp>
#include <core/typedefs.hpp>
#include <memory_resource>

namespace otterbrix {
	namespace util {

		py::object LogicalValueToPython(const components::types::logical_value_t& value,
				const components::types::complex_logical_type& type);

		py::dict CursorRowToPythonDict(components::cursor::cursor_t_ptr& cursor,
				uint64_t row_idx,
				const vector<components::table::column_definition_t>& col_defs);

		py::dict DataChunkRowToPythonDict(const components::vector::data_chunk_t& chunk,
				uint64_t row_idx,
				const vector<components::table::column_definition_t>& col_defs);
	}

} // namespace otterbrix
