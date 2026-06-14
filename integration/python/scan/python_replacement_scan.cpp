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
#include <memory>
#include <common/string_util/string_util.hpp>
#include <common/typedefs.hpp>

#include <stdexcept>
#include <string>
#include <vector>

using components::types::logical_type;
using namespace components;

namespace otterbrix {

    void throw_scan_failure_error(const py::object &entry, const std::string &name) {
        auto py_object_type = std::string(py::str(entry.get_type().attr("__name__")));
        std::string error =
           "Python object " + name + " of type " + py_object_type;
       error += " not suitable for replacement scans. ";
       throw std::runtime_error(error);
    }

    namespace {
        //! Does this python object (or a pyarrow-backed pandas data_frame) expose the Arrow C-stream
        //! PyCapsule interface that core data_chunk_from_arrow can consume?
        bool has_arrow_c_stream(const py::object &entry) {
            return py::hasattr(entry, "__arrow_c_stream__");
        }

        //! Detect a pandas data_frame whose columns are pyarrow-backed (dtype_backend="pyarrow"),
        //! i.e. every column dtype is an ArrowDtype. Such frames can be ingested through Arrow.
        bool is_pyarrow_backed_pandas(const py::object &entry) {
            if (!framework_object_detection_t::is_pandas_dataframe(entry)) {
                return false;
            }
            if (has_arrow_c_stream(entry)) {
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

        //! Build a table_ref_t that routes `arrow_source` through the core arrow path via arrow_scan_function_t.
        //! `arrow_source` must be either an arrow object understood by get_arrow_type (arrow_table_t / Dataset /
        //! Scanner / arrow_record_batch_reader_t / PyCapsule) or expose __arrow_c_stream__.
        std::unique_ptr<components::tableref::table_ref_t> build_arrow_table_ref(const py::object &arrow_source) {
            auto table_function = std::make_unique<components::tableref::table_ref_t>();
            auto dependency = std::make_shared<external_dependency_t>();
            auto factory_dep = std::make_unique<arrow_stream_factory_dependency_t>(arrow_source);
            auto *factory_ptr = factory_dep->get();

            std::vector<components::types::logical_value_t> children;
            children.emplace_back(std::pmr::get_default_resource(), static_cast<void *>(factory_ptr));

            table_function->function = std::make_unique<arrow_scan_function_t>();
            table_function->children = std::move(children);
            // ArrowScanBind looks up replacement_cache to keep the factory + python object alive.
            dependency->add_dependency(dependency_kind_t::replacement_cache, std::move(factory_dep));
            table_function->external_dependency = dependency;
            return table_function;
        }
    } // namespace

    std::unique_ptr<components::tableref::table_ref_t>
        scan_t::try_replacement_object(const py::object &entry, const std::string & /*name*/) {
        auto table_function = std::make_unique<components::tableref::table_ref_t>();
        std::vector<components::types::logical_value_t> children;
        numpy_object_type_t numpy_type;
        if (framework_object_detection_t::is_polars_dataframe(entry)) {
            // Polars exposes the Arrow C-stream PyCapsule interface; route the whole frame through
            // the core arrow path so data lands via components::vector::arrow::data_chunk_from_arrow.
            return build_arrow_table_ref(entry);
        } else
        if (is_pyarrow_backed_pandas(entry)) {
            // pandas with dtype_backend="pyarrow" (or otherwise exposing __arrow_c_stream__):
            // ingest through the same arrow path instead of the numpy column scanner.
            return build_arrow_table_ref(entry);
        } else
        if (!framework_object_detection_t::is_pandas_dataframe(entry) &&
            (get_arrow_type(entry) != py_arrow_object_type_t::Invalid || has_arrow_c_stream(entry))) {
            // Native Arrow objects (pyarrow arrow_table_t / Dataset / Scanner / arrow_record_batch_reader_t /
            // Arrow C-stream capsule) ingest straight through the core arrow path.
            return build_arrow_table_ref(entry);
        } else
        if (framework_object_detection_t::is_pandas_dataframe(entry)) {
                auto new_df = pandas_scan_function_t::pandas_replace_copied_names(entry);
                table_function->external_dependency = std::make_shared<external_dependency_t>();
                children.emplace_back(std::pmr::get_default_resource(), static_cast<void*>(new_df.ptr()));
                table_function->function = std::make_unique<pandas_scan_function_t>();
                table_function->children = std::move(children);
                table_function->external_dependency->add_dependency(dependency_kind_t::data, python_dependency_item_t::create(new_df));
        } else
        if ((numpy_type = framework_object_detection_t::get_numpy_object_type(entry)) != numpy_object_type_t::INVALID) {
		    py::dict data; // we will convert all the supported format to dict{"key": np.array(value)}.
		    idx_t idx = 0;
		    switch (numpy_type) {
		    case numpy_object_type_t::NDARRAY1D:
			    data["column0"] = entry;
			    break;
		    case numpy_object_type_t::NDARRAY2D:
			    idx = 0;
			    for (auto item : py::cast<py::array>(entry)) {
				    data[("column" + std::to_string(idx)).c_str()] = item;
				    idx++;
			    }
			    break;
		    case numpy_object_type_t::LIST:
			    idx = 0;
			    for (auto item : py::cast<py::list>(entry)) {
				    data[("column" + std::to_string(idx)).c_str()] = item;
				    idx++;
			    }
			    break;
		    case numpy_object_type_t::DICT:
			    data = py::cast<py::dict>(entry);
			    break;
		    default:
			    throw std::runtime_error("Unsupported Numpy object");
			    break;
		    }
		    children.emplace_back(std::pmr::get_default_resource(), static_cast<void*>(data.ptr()));
		    table_function->function = std::make_unique<pandas_scan_function_t>();//std::make_unique<FunctionExpression>("pandas_scan", std::move(children));
            table_function->children = std::move(children);
            std::shared_ptr<external_dependency_t> dependency = std::make_shared<external_dependency_t>();
            dependency->add_dependency(dependency_kind_t::data, python_dependency_item_t::create(data));
            dependency->add_dependency(dependency_kind_t::replacement_cache, python_dependency_item_t::create(entry));
            table_function->external_dependency = dependency;
	    } else {
		    // This throws an error later on!
		    return nullptr;

        }
        return table_function;
    }


    std::unique_ptr<components::tableref::table_ref_t> 
        scan_t::replacement_object(const py::object &entry, const std::string &name) {
            auto ref = try_replacement_object(entry, name);
            if (!ref) {
                throw_scan_failure_error(entry, name);
            }
            return ref;
    }


    std::pair<logical_plan::node_data_ptr, std::unique_ptr<std::vector<components::table::column_definition_t>>>
            scan_t::fetch_object_data(std::pmr::memory_resource* resource, std::unique_ptr<components::tableref::table_ref_t> ref) {
        function::table_function_bind_input_t bind_input(ref->children, *ref);
        std::vector<types::complex_logical_type> return_types;
        std::vector<std::string> names;
        auto function_data = ref->function->bind(bind_input, return_types, names);

        std::vector<components::table::column_definition_t> col_defs;
        for (std::size_t i = 0; i < return_types.size(); i++) {
            col_defs.emplace_back(names[i], return_types[i]);
        }
        std::vector<uint64_t> column_ids;
        column_ids.reserve(return_types.size());
        for (uint64_t i = 0; i < return_types.size(); i++) {
            column_ids.push_back(i);
        }
        function::table_function_init_input_t init_input(
                otterbrix::optional_ptr<function::function_data_t>(function_data), column_ids);

        py::gil_scoped_release release;
        auto global_state = ref->function->init_global(init_input);
        auto local_state = ref->function->init_local(init_input, global_state.get());

        function::table_function_input_t input{
                otterbrix::optional_ptr<function::function_data_t>(function_data),
                otterbrix::optional_ptr<function::local_table_function_state_t>(local_state),
                otterbrix::optional_ptr<function::global_table_function_state_t>(global_state)};


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
            std::make_unique<std::vector<components::table::column_definition_t>>(std::move(col_defs))};
    }


    

} // namespace otterbrix
