#include "arrow_export_utils.hpp"

#include "arrow_array_stream.hpp"

#include <components/arrow/arrow.hpp>
#include <components/arrow/arrow_converter.hpp>

namespace otterbrix {

    void TransformOtterbrixToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, py::list &batches) {
	    py::gil_assert();
	    auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	    auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
	    batches.append(batch_import_func(reinterpret_cast<uint64_t>(&data), reinterpret_cast<uint64_t>(&arrow_schema)));
    }

    namespace pyarrow {
    
        py::object ToArrowTable(const vector<components::types::complex_logical_type> &types, 
                const vector<string> &names, const py::list &batches) {
        	py::gil_scoped_acquire acquire;
        
        	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
        	auto from_batches_func = pyarrow_lib_module.attr("Table").attr("from_batches");
        	auto schema_import_func = pyarrow_lib_module.attr("Schema").attr("_import_from_c");
        	ArrowSchema schema;
            components::arrow::ArrowConverter::ToArrowSchema(&schema, types, names);
        	auto schema_obj = schema_import_func(reinterpret_cast<uint64_t>(&schema));
        
        	return py::cast<otterbrix::pyarrow::Table>(from_batches_func(batches, schema_obj));
        }
    
    } // namespace pyarrow

} // namespace otterbrix
