#include "python_replacement_scan.hpp"

//#include <arrow/arrow_array_stream.hpp>

#include <connection_environment/framework_object_detection.hpp>
#include <otterbrix_wrapper/python_dependency.hpp>
#include <pandas/pandas_scan.hpp>
#include <util/convert_value.hpp>

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

    //using components::table::ArrowScanFunction;
    //using components::table::ArrowScanBind;
    //using components::table::ArrowScanInitGlobal;
    //using components::table::ArrowScanInitLocal;


    // static void CreateArrowScan(const string &name, py::object entry, 
    //         components::reference::TableFunctionRef &table_function,
    //         vector<unique_ptr<ParsedExpression>> &children, PyArrowObjectType type) {

    //     if (type == PyArrowObjectType::PyCapsuleInterface) {
    //         entry = entry.attr("__arrow_c_stream__")();
    //         type = PyArrowObjectType::PyCapsule;
    //     }
    
    //     auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(entry.ptr());
    //     auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
    //     auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;
    
    //     children.emplace_back(static_cast<void*>(stream_factory.get()));
    //     children.emplace_back(static_cast<void*>(stream_factory_produce));
    //     children.emplace_back(static_cast<void*>(stream_factory_get_schema));
    
    //     if (type == PyArrowObjectType::PyCapsule) {
    //         // Disable projection+filter pushdown
    //         table_function.function = make_uniq<FunctionExpression>("arrow_scan_dumb", std::move(children));
    //     } else {
    //         table_function.function = make_uniq<FunctionExpression>("arrow_scan", std::move(children));
    //     }
    //     table_function.children = std::move(children);
    //     table_function.function = make_unique<TableFunction>("arrow_scan_dumb", 
    //             {logical_type::POINTER, logical_type::POINTER, logical_type::POINTER},
    //             ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
    
    //     auto dependency = make_uniq<ExternalDependency>();
    //     auto dependency_item = PythonDependencyItem::Create(make_uniq<RegisteredArrow>(std::move(stream_factory), entry));
    //     dependency->AddDependency("replacement_cache", std::move(dependency_item));
    //     table_function.external_dependency = std::move(dependency);
    // }


    void ThrowScanFailureError(const py::object &entry, const string &name) {
        auto py_object_type = string(py::str(entry.get_type().attr("__name__")));
        string error =
           "Python object " + name + " of type " + py_object_type;
       error += " not suitable for replacement scans. ";
       throw std::runtime_error(error);
    }

    unique_ptr<components::tableref::TableRef> 
        Scan::TryReplacementObject(const py::object &entry, const string &name) {
        auto table_function = make_unique<components::tableref::TableRef>();
        vector<components::types::logical_value_t> children;
        NumpyObjectType numpy_type;
        //PyArrowObjectType arrow_type;
        if (FrameworkObjectDetection::IsPandasDataframe(entry)) {
           // if (PandasDataFrame::IsPyArrowBacked(entry)) { 
           //     auto table = PandasDataFrame::ToArrowTable(entry);
           //     CreateArrowScan(name, table, *table_function, children, client_properties, PyArrowObjectType::Table);
           // } else {
                auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
                table_function->external_dependency = make_shared<ExternalDependency>();
                children.emplace_back(static_cast<void*>(new_df.ptr()));
                table_function->function = make_unique<PandasScanFunction>();
                table_function->children = std::move(children);
                table_function->external_dependency->AddDependency("data", PythonDependencyItem::Create(new_df));
           // }
        } else if (false) {//(arrow_type = FrameworkObjectDetection::GetArrowType(entry)) != PyArrowObjectType::Invalid) {
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
		    children.emplace_back(static_cast<void*>(data.ptr()));
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
            return std::move(ref);
    }


    std::pair<logical_plan::node_data_ptr, unique_ptr<vector<components::table::column_definition_t>>>
            Scan::FetchObjectData(std::pmr::memory_resource* resource, unique_ptr<components::tableref::TableRef> ref) {
        std::pmr::vector<components::document::document_ptr> result;
        function::TableFunctionBindInput bind_input(ref->children, *ref);
        vector<types::complex_logical_type> return_types;
        vector<string> names;
        auto function_data = ref->function->bind(bind_input, return_types, names);
        // coluns_definition
        std::vector<components::table::column_definition_t> col_defs;
        for (int i = 0; i < return_types.size(); i++) {
            col_defs.emplace_back(names[i], return_types[i]);
        }
        vector<uint64_t> column_ids;
        column_ids.reserve(return_types.size());
        for (int i = 0; i < return_types.size(); i++) {
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

        
        bool has_input;
        do {
            components::vector::data_chunk_t chunk(resource, return_types);
            ref->function->function(input, chunk);
            if (chunk.size() > 0) {
                has_input = true;
                auto result_chunk = util::ToDocuments(resource, chunk, names);
                result.reserve(result.size() + result_chunk.size());
                result.insert(result.end(), result_chunk.begin(), result_chunk.end());
                
            } else {
                has_input = false;
            }
        } while (has_input);
        return {logical_plan::make_node_raw_data(resource, std::move(result)), 
            make_unique<vector<components::table::column_definition_t>>(std::move(col_defs))};
    }


    

} // namespace otterbrix
