#include "python_replacement_scan.hpp"

#include <arrow/arrow_array_stream.hpp>
#include <arrow/arrow_scan_function.hpp>
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

    namespace {
        //! Does this python object (or a pyarrow-backed pandas DataFrame) expose the Arrow C-stream
        //! PyCapsule interface that core data_chunk_from_arrow can consume?
        bool HasArrowCStream(const py::object &entry) {
            return py::hasattr(entry, "__arrow_c_stream__");
        }

        //! Detect a pandas DataFrame whose columns are pyarrow-backed (dtype_backend="pyarrow"),
        //! i.e. every column dtype is an ArrowDtype. Such frames can be ingested through Arrow.
        bool IsPyarrowBackedPandas(const py::object &entry) {
            if (!FrameworkObjectDetection::IsPandasDataframe(entry)) {
                return false;
            }
            if (HasArrowCStream(entry)) {
                return true;
            }
            try {
                py::object dtypes = entry.attr("dtypes");
                py::object pandas = py::module_::import("pandas");
                if (!py::hasattr(pandas, "ArrowDtype")) {
                    return false;
                }
                py::object arrow_dtype = pandas.attr("ArrowDtype");
                bool any = false;
                for (auto dt : dtypes) {
                    any = true;
                    if (!py::isinstance(py::reinterpret_borrow<py::object>(dt), arrow_dtype)) {
                        return false;
                    }
                }
                return any;
            } catch (const py::error_already_set &) {
                return false;
            }
        }

        //! Build a TableRef that routes `arrow_source` through the core arrow path via ArrowScanFunction.
        //! `arrow_source` must be either an arrow object understood by GetArrowType (Table / Dataset /
        //! Scanner / RecordBatchReader / PyCapsule) or expose __arrow_c_stream__.
        unique_ptr<components::tableref::TableRef> BuildArrowTableRef(const py::object &arrow_source) {
            auto table_function = make_unique<components::tableref::TableRef>();
            auto dependency = make_shared<ExternalDependency>();
            auto factory_dep = make_unique<ArrowStreamFactoryDependency>(arrow_source);
            auto *factory_ptr = factory_dep->get();

            vector<components::types::logical_value_t> children;
            children.emplace_back(std::pmr::get_default_resource(), static_cast<void *>(factory_ptr));

            table_function->function = make_unique<ArrowScanFunction>();
            table_function->children = std::move(children);
            // ArrowScanBind looks up replacement_cache to keep the factory + python object alive.
            dependency->AddDependency(dependency_kind_t::replacement_cache, std::move(factory_dep));
            table_function->external_dependency = dependency;
            return table_function;
        }
    } // namespace

    unique_ptr<components::tableref::TableRef>
        Scan::TryReplacementObject(const py::object &entry, const string & /*name*/) {
        auto table_function = make_unique<components::tableref::TableRef>();
        vector<components::types::logical_value_t> children;
        NumpyObjectType numpy_type;
        if (FrameworkObjectDetection::IsPolarsDataframe(entry)) {
            // Polars exposes the Arrow C-stream PyCapsule interface; route the whole frame through
            // the core arrow path so data lands via components::vector::arrow::data_chunk_from_arrow.
            return BuildArrowTableRef(entry);
        } else
        if (IsPyarrowBackedPandas(entry)) {
            // pandas with dtype_backend="pyarrow" (or otherwise exposing __arrow_c_stream__):
            // ingest through the same arrow path instead of the numpy column scanner.
            return BuildArrowTableRef(entry);
        } else
        if (!FrameworkObjectDetection::IsPandasDataframe(entry) &&
            (GetArrowType(entry) != PyArrowObjectType::Invalid || HasArrowCStream(entry))) {
            // Native Arrow objects (pyarrow Table / Dataset / Scanner / RecordBatchReader /
            // Arrow C-stream capsule) ingest straight through the core arrow path.
            return BuildArrowTableRef(entry);
        } else
        if (FrameworkObjectDetection::IsPandasDataframe(entry)) {
                auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
                table_function->external_dependency = make_shared<ExternalDependency>();
                children.emplace_back(std::pmr::get_default_resource(), static_cast<void*>(new_df.ptr()));
                table_function->function = make_unique<PandasScanFunction>();
                table_function->children = std::move(children);
                table_function->external_dependency->AddDependency(dependency_kind_t::data, PythonDependencyItem::Create(new_df));
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
            dependency->AddDependency(dependency_kind_t::data, PythonDependencyItem::Create(data));
            dependency->AddDependency(dependency_kind_t::replacement_cache, PythonDependencyItem::Create(entry));
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

        // R8: engine-data container on the threaded memory_resource instead of the default heap.
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
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
