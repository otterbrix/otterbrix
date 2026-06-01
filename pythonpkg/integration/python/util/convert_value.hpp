#pragma once 

#include <pybind11/pybind_wrapper.hpp>
#include <components/types/logical_value.hpp>
#include <components/table/column_definition.hpp>
#include <components/document/document.hpp>
#include <components/cursor/cursor.hpp>
#include <components/vector/data_chunk.hpp>

#include <core/types/vector.hpp>
#include <core/types/string.hpp>
#include <core/typedefs.hpp>
#include <memory_resource>

namespace otterbrix {
	namespace util {
		components::document::value_t ToDocumentValue(components::document::impl::base_document* tape, 
				const components::types::logical_value_t& value);

		components::types::logical_value_t ToLogicalValue(const components::document::document_ptr& value,
				const components::table::column_definition_t& col_def);

		components::vector::data_chunk_t ToDataChunk(
				std::pmr::memory_resource* resource,
				components::cursor::cursor_t_ptr cursor,
				const vector<components::table::column_definition_t>& col_defs);

		std::pmr::vector<components::document::document_ptr> ToDocuments(std::pmr::memory_resource* resource, 
			    const components::vector::data_chunk_t& chunk, const vector<string>& names);


        py::dict DocumentToPythonDict(components::document::document_ptr doc,
                const vector<components::table::column_definition_t>& col_defs);
	}

} // namespace otterbrix
