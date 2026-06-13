#include "arrow_array_stream.hpp"

#include <connection_environment/connection_environment.hpp>

#include <cassert>
#include <stdexcept>
#include <string_view>

namespace otterbrix {

    using components::types::logical_type;


void VerifyArrowDatasetLoaded() {
	auto &import_cache = ConnectionEnvironment::ImportCache();
	if (!import_cache.pyarrow.dataset() || !ModuleIsLoaded<PyarrowDatasetCacheItem>()) {
		throw std::runtime_error("Optional module 'pyarrow.dataset' is required to perform this action");
	}
}

py::object PythonTableArrowArrayStreamFactory::ProduceScanner(py::object &arrow_scanner, py::handle &arrow_obj_handle,
                                                              ArrowStreamParameters &parameters) {
	/*assert(!py::isinstance<py::capsule>(arrow_obj_handle));
	ArrowSchemaWrapper schema;
	PythonTableArrowArrayStreamFactory::GetSchemaInternal(arrow_obj_handle, schema);
	vector<string> unused_names;
	vector<components::types::complex_logical_type> unused_types;
	ArrowTableType arrow_table;
	ArrowTableFunction::PopulateArrowTableType(arrow_table, schema, unused_names, unused_types);

	auto filters = parameters.filters;
	auto &column_list = parameters.projected_columns.columns;
	auto &filter_to_col = parameters.projected_columns.filter_to_col;
	bool has_filter = filters && !filters->filters.empty();
	py::list projection_list = py::cast(column_list);
	if (has_filter) {
		auto filter = TransformFilter(*filters, parameters.projected_columns.projection_map, 
                filter_to_col, arrow_table);
		if (column_list.empty()) {
			return arrow_scanner(arrow_obj_handle, py::arg("filter") = filter);
		} else {
			return arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list, py::arg("filter") = filter);
		}
	} else {
		if (column_list.empty()) {
			return arrow_scanner(arrow_obj_handle);
		} else {
			return arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list);
		}
	}*/
    return arrow_scanner(arrow_obj_handle);
}

unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr,
                                                                                ArrowStreamParameters &parameters) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr)); // NOLINT
	assert(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	auto arrow_object_type = GetArrowType(arrow_obj_handle);

	if (arrow_object_type == PyArrowObjectType::PyCapsule) {
		auto res = make_uniq<ArrowArrayStreamWrapper>();
		auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
		auto stream = capsule.get_pointer<struct ArrowArrayStream>();
		if (!stream->release) {
			throw std::runtime_error("ArrowArrayStream was released by another thread/library");
		}
		res->arrow_array_stream = *stream;
		stream->release = nullptr;
		return res;
	}

	auto &import_cache = ConnectionEnvironment::ImportCache();
	py::object scanner;
	py::object arrow_batch_scanner = import_cache.pyarrow.dataset.Scanner().attr("from_batches");
	switch (arrow_object_type) {
	case PyArrowObjectType::Table: {
		auto arrow_dataset = import_cache.pyarrow.dataset().attr("dataset");
		auto dataset = arrow_dataset(arrow_obj_handle);
		py::object arrow_scanner = dataset.attr("__class__").attr("scanner");
		scanner = ProduceScanner(arrow_scanner, dataset, parameters);
		break;
	}
	case PyArrowObjectType::RecordBatchReader: {
		scanner = ProduceScanner(arrow_batch_scanner, arrow_obj_handle, parameters); 
		break;
	}
	case PyArrowObjectType::Scanner: {
		// If it's a scanner we have to turn it to a record batch reader, and then a scanner again since we can't stack
		// scanners on arrow Otherwise pushed-down projections and filters will disappear like tears in the rain
		auto record_batches = arrow_obj_handle.attr("to_reader")();
		scanner = ProduceScanner(arrow_batch_scanner, record_batches, parameters);
		break;
	}
	case PyArrowObjectType::Dataset: {
		py::object arrow_scanner = arrow_obj_handle.attr("__class__").attr("scanner");
		scanner = ProduceScanner(arrow_scanner, arrow_obj_handle, parameters);
		break;
	}
	default: {
		auto py_object_type = string(py::str(arrow_obj_handle.get_type().attr("__name__")));
		throw std::runtime_error("Object of type "+py_object_type+" is not a recognized Arrow object");
	}
	}

	auto record_batches = scanner.attr("to_reader")();
	auto res = make_uniq<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c(reinterpret_cast<uint64_t>(&res->arrow_array_stream));
	return res;
}

void PythonTableArrowArrayStreamFactory::GetSchemaInternal(py::handle arrow_obj_handle, ArrowSchemaWrapper &schema) {
	if (py::isinstance<py::capsule>(arrow_obj_handle)) {
		auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
		auto stream = capsule.get_pointer<struct ArrowArrayStream>();
		if (!stream->release) {
			throw std::runtime_error("ArrowArrayStream was released by another thread/library");
		}
		stream->get_schema(stream, &schema.arrow_schema);
		return;
	}

	auto table_class = py::module::import("pyarrow").attr("Table");
	if (py::isinstance(arrow_obj_handle, table_class)) {
		auto obj_schema = arrow_obj_handle.attr("schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));
		return;
	}

	VerifyArrowDatasetLoaded();

	auto &import_cache = ConnectionEnvironment::ImportCache();
	auto scanner_class = import_cache.pyarrow.dataset.Scanner();

	if (py::isinstance(arrow_obj_handle, scanner_class)) {
		auto obj_schema = arrow_obj_handle.attr("projected_schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema));
	} else {
		auto obj_schema = arrow_obj_handle.attr("schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema));
	}
}

void PythonTableArrowArrayStreamFactory::GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr)); // NOLINT
	assert(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	GetSchemaInternal(arrow_obj_handle, schema);
}

string ConvertTimestampUnit(ArrowDateTimeType unit) {
	switch (unit) {
	case ArrowDateTimeType::MICROSECONDS:
		return "us";
	case ArrowDateTimeType::MILLISECONDS:
		return "ms";
	case ArrowDateTimeType::NANOSECONDS:
		return "ns";
	case ArrowDateTimeType::SECONDS:
		return "s";
	default:
		throw std::runtime_error("DatetimeType not recognized in ConvertTimestampUnit: "+std::to_string((int)unit));
	}
}

py::object GetScalar(components::types::logical_value_t &constant, const ArrowType &type) {
	py::object scalar = py::module_::import("pyarrow").attr("scalar");
	auto &import_cache = ConnectionEnvironment::ImportCache();
	py::object dataset_scalar = import_cache.pyarrow.dataset().attr("scalar");
	py::object scalar_value;
	switch (constant.type()) {
    case logical_type::BOOLEAN:
		return dataset_scalar(constant.value<bool>());
    case logical_type::TINYINT:
		return dataset_scalar(constant.value<int8_t>());
	case logical_type::SMALLINT:
		return dataset_scalar(constant.value<int16_t>());
	case logical_type::INTEGER:
		return dataset_scalar(constant.value<int32_t>());
	case logical_type::BIGINT:
		return dataset_scalar(constant.value<int64_t>());
	case logical_type::DATE: {
		py::object date_type = py::module_::import("pyarrow").attr("date32");
		return dataset_scalar(scalar(constant.value<int32_t>(), date_type()));
	}
	case logical_type::TIMESTAMP_US: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.value<int64_t>(), date_type("us")));
	}
	case logical_type::TIMESTAMP_MS: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.value<int64_t>(), date_type("ms")));
	}
	case logical_type::TIMESTAMP_NS: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.value<int64_t>(), date_type("ns")));
	}
	case logical_type::TIMESTAMP_SEC: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.value<int64_t>(), date_type("s")));
	}
	case logical_type::UTINYINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint8");
		return dataset_scalar(scalar(constant.value<uint8_t>(), integer_type()));
	}
	case logical_type::USMALLINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint16");
		return dataset_scalar(scalar(constant.value<uint16_t>(), integer_type()));
	}
	case logical_type::UINTEGER: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint32");
		return dataset_scalar(scalar(constant.value<uint32_t>(), integer_type()));
	}
	case logical_type::UBIGINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint64");
		return dataset_scalar(scalar(constant.value<uint64_t>(), integer_type()));
	}
	case logical_type::FLOAT:
		return dataset_scalar(constant.value<float>());
	case logical_type::DOUBLE:
		return dataset_scalar(constant.value<double>());
	case logical_type::STRING_LITERAL:
		return dataset_scalar(constant.value<const string&>());
	case logical_type::BLOB:
		return dataset_scalar(constant.value<std::string_view>());
	case logical_type::DECIMAL: {
		py::object date_type = py::module_::import("pyarrow").attr("decimal128");
        auto* decimal_extention = 
            static_cast<components::types::decimal_logical_type_extention*>(constant.type()->extention());
		uint8_t width = decimal_extention->width();
		uint8_t scale = decimal_extention->scale();
		return dataset_scalar(scalar(constant.value<int64_t>(), date_type(width, scale)));
	}
	default:
		throw std::runtime_error("Unimplemented type "+constant.type().ToString()+" for Arrow Filter Pushdown");
	}
}

} // namespace otterbrix
