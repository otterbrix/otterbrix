#include "python_replacement_scan.hpp"

#include <connection_environment/framework_object_detection.hpp>
#include <connection_environment/connection_environment.hpp>
#include <otterbrix_wrapper/python_dependency.hpp>
#include <pandas/pandas_scan.hpp>
#include <components/tableref/tableref.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/types/string.hpp>
#include <core/types/memory.hpp>
#include <core/types/vector.hpp>
#include <core/string_util/string_util.hpp>
#include <core/typedefs.hpp>

#include <stdexcept>

using components::types::logical_type;
using namespace components;

namespace otterbrix {

    void ThrowScanFailureError(const py::object &entry, const string &name) {
        auto py_object_type = string(py::str(entry.get_type().attr("__name__")));
        string error =
           "Python object " + name + " of type " + py_object_type;
       error += " not suitable for replacement scans. ";
       throw std::runtime_error(error);
    }

    unique_ptr<components::tableref::TableRef>
        Scan::TryReplacementObject(const py::object &entry, const string & /*name*/) {
        auto table_function = make_unique<components::tableref::TableRef>();
        vector<components::types::logical_value_t> children;
        NumpyObjectType numpy_type;
        if (FrameworkObjectDetection::IsPandasDataframe(entry)) {
                auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
                table_function->external_dependency = make_shared<ExternalDependency>();
                children.emplace_back(std::pmr::get_default_resource(), static_cast<void*>(new_df.ptr()));
                table_function->function = make_unique<PandasScanFunction>();
                table_function->children = std::move(children);
                table_function->external_dependency->AddDependency("data", PythonDependencyItem::Create(new_df));
        } else
        if (FrameworkObjectDetection::IsPolarsDataframe(entry)) {
            auto& import_cache_py = ConnectionEnvironment::ImportCache();
            py::object as_dict = entry.attr("to_dict")(py::arg("as_series") = false);
            py::object pandas_df = import_cache_py.pandas.DataFrame()(as_dict);
            return TryReplacementObject(pandas_df, "");
        } else
        if ((numpy_type = FrameworkObjectDetection::GetNumpyObjectType(entry)) != NumpyObjectType::INVALID) {
		    py::dict data; // we will convert all the supported format to dict{"key": np.array(value)}.
		    idx_t idx = 0;
		    switch (numpy_type) {
		    case NumpyObjectType::NDARRAY1D:
			    data["column0"] = entry;
			    break;
		    case NumpyObjectType::NDARRAY2D:
			    idx = 0;
			    for (auto item : py::cast<py::array>(entry)) {
				    data[("column" + std::to_string(idx)).c_str()] = item;
				    idx++;
			    }
			    break;
		    case NumpyObjectType::LIST:
			    idx = 0;
			    for (auto item : py::cast<py::list>(entry)) {
				    data[("column" + to_string(idx)).c_str()] = item;
				    idx++;
			    }
			    break;
		    case NumpyObjectType::DICT:
			    data = py::cast<py::dict>(entry);
			    break;
		    default:
			    throw std::runtime_error("Unsupported Numpy object");
			    break;
		    }
		    children.emplace_back(std::pmr::get_default_resource(), static_cast<void*>(data.ptr()));
		    table_function->function = make_unique<PandasScanFunction>();//make_unique<FunctionExpression>("pandas_scan", std::move(children));
            table_function->children = std::move(children);
            shared_ptr<ExternalDependency> dependency = make_shared<ExternalDependency>();
            dependency->AddDependency("data", PythonDependencyItem::Create(data));
            dependency->AddDependency("replacement_cache", PythonDependencyItem::Create(entry));
            table_function->external_dependency = dependency;
	    } else {
		    // This throws an error later on!
		    return nullptr;

        }
        return table_function;
    }


    unique_ptr<components::tableref::TableRef> 
        Scan::ReplacementObject(const py::object &entry, const string &name) {
            auto ref = TryReplacementObject(entry, name);
            if (!ref) {
                ThrowScanFailureError(entry, name);
            }
            return ref;
    }


    std::pair<logical_plan::node_data_ptr, unique_ptr<vector<components::table::column_definition_t>>>
            Scan::FetchObjectData(std::pmr::memory_resource* resource, unique_ptr<components::tableref::TableRef> ref) {
        function::TableFunctionBindInput bind_input(ref->children, *ref);
        vector<types::complex_logical_type> return_types;
        vector<string> names;
        auto function_data = ref->function->bind(bind_input, return_types, names);

        std::vector<components::table::column_definition_t> col_defs;
        for (std::size_t i = 0; i < return_types.size(); i++) {
            col_defs.emplace_back(names[i], return_types[i]);
        }
        vector<uint64_t> column_ids;
        column_ids.reserve(return_types.size());
        for (uint64_t i = 0; i < return_types.size(); i++) {
            column_ids.push_back(i);
        }
        function::TableFunctionInitInput init_input(
                otterbrix::optional_ptr<function::FunctionData>(function_data), column_ids);

        py::gil_scoped_release release;
        auto global_state = ref->function->init_global(init_input);
        auto local_state = ref->function->init_local(init_input, global_state.get());

        function::TableFunctionInput input{
                otterbrix::optional_ptr<function::FunctionData>(function_data),
                otterbrix::optional_ptr<function::LocalTableFunctionState>(local_state),
                otterbrix::optional_ptr<function::GlobalTableFunctionState>(global_state)};


        // One merged data_chunk on PMR into the plan (previously: ToDocuments and a vector of document_ptr).
        std::pmr::vector<types::complex_logical_type> pmr_types(resource);
        for (size_t i = 0; i < return_types.size(); i++) {
            auto t = return_types[i];
            if (!t.has_alias() && i < names.size()) {
                t.set_alias(names[i]);
            }
            // pmr_types: PMR copies for data_chunk_t; aliases so validate_schema can resolve column names.
            pmr_types.push_back(t);
        }

        std::vector<components::vector::data_chunk_t> chunks;
        while (true) {
            components::vector::data_chunk_t chunk(resource, pmr_types);
            ref->function->function(input, chunk);
            if (chunk.size() == 0) {
                break;
            }
            chunks.push_back(std::move(chunk));
        }

        uint64_t total_rows = 0;
        for (const auto& c : chunks) {
            total_rows += c.size();
        }

        components::vector::data_chunk_t result_chunk(resource, pmr_types,
                                                      total_rows > 0 ? total_rows : 1);
        result_chunk.set_cardinality(total_rows);
        uint64_t row_offset = 0;
        for (auto& c : chunks) {
            for (uint64_t r = 0; r < c.size(); r++) {
                for (uint64_t col = 0; col < pmr_types.size(); col++) {
                    result_chunk.set_value(col, row_offset + r, c.value(col, r));
                }
            }
            row_offset += c.size();
        }

        return {logical_plan::make_node_raw_data(resource, std::move(result_chunk)),
            make_unique<vector<components::table::column_definition_t>>(std::move(col_defs))};
    }


    

} // namespace otterbrix
