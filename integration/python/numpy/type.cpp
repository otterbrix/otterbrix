#include "numpy_type.hpp"

#include <components/types/types.hpp>
#include <core/types/string.hpp>

#include <memory_resource>

namespace otterbrix {

using components::types::logical_type;

static core::error_t make_error(std::pmr::memory_resource *resource, const string &what) {
	return core::error_t{core::error_code_t::other_error, std::pmr::string{what, resource}};
}

static bool IsDateTime(NumpyNullableType type) {
	switch (type) {
	case NumpyNullableType::DATETIME_NS:
	case NumpyNullableType::DATETIME_S:
	case NumpyNullableType::DATETIME_MS:
	case NumpyNullableType::DATETIME_US:
		return true;
	default:
		return false;
	};
}

static core::result_wrapper_t<NumpyNullableType> ConvertNumpyTypeInternal(std::pmr::memory_resource *resource, const string &col_type_str) {
	if (col_type_str == "bool" || col_type_str == "boolean") {
		return NumpyNullableType::BOOL;
	}
	if (col_type_str == "uint8" || col_type_str == "UInt8") {
		return NumpyNullableType::UINT_8;
	}
	if (col_type_str == "uint16" || col_type_str == "UInt16") {
		return NumpyNullableType::UINT_16;
	}
	if (col_type_str == "uint32" || col_type_str == "UInt32") {
		return NumpyNullableType::UINT_32;
	}
	if (col_type_str == "uint64" || col_type_str == "UInt64") {
		return NumpyNullableType::UINT_64;
	}
	if (col_type_str == "int8" || col_type_str == "Int8") {
		return NumpyNullableType::INT_8;
	}
	if (col_type_str == "int16" || col_type_str == "Int16") {
		return NumpyNullableType::INT_16;
	}
	if (col_type_str == "int32" || col_type_str == "Int32") {
		return NumpyNullableType::INT_32;
	}
	if (col_type_str == "int64" || col_type_str == "Int64") {
		return NumpyNullableType::INT_64;
	}
	if (col_type_str == "float16" || col_type_str == "Float16") {
		return NumpyNullableType::FLOAT_16;
	}
	if (col_type_str == "float32" || col_type_str == "Float32") {
		return NumpyNullableType::FLOAT_32;
	}
	if (col_type_str == "float64" || col_type_str == "Float64") {
		return NumpyNullableType::FLOAT_64;
	}
	if (col_type_str == "string" || col_type_str == "str") {
		return NumpyNullableType::STRING;
	}
	if (col_type_str == "object") {
		return NumpyNullableType::OBJECT;
	}
	if (col_type_str == "timedelta64[ns]") {
		return NumpyNullableType::TIMEDELTA;
	}
	// We use 'starts_with' because it might have ', tz' at the end, indicating timezone
	if (col_type_str.starts_with("datetime64[ns")) {
		return NumpyNullableType::DATETIME_NS;
	}
	if (col_type_str.starts_with("datetime64[us")) {
		return NumpyNullableType::DATETIME_US;
	}
	if (col_type_str.starts_with("datetime64[ms")) {
		return NumpyNullableType::DATETIME_MS;
	}
	if (col_type_str.starts_with("datetime64[s")) {
		return NumpyNullableType::DATETIME_S;
	}
	// Legacy datetime type indicators
	if (col_type_str.starts_with("<M8[ns")) {
		return NumpyNullableType::DATETIME_NS;
	}
	if (col_type_str.starts_with("<M8[s")) {
		return NumpyNullableType::DATETIME_S;
	}
	if (col_type_str.starts_with("<M8[us")) {
		return NumpyNullableType::DATETIME_US;
	}
	if (col_type_str.starts_with("<M8[ms")) {
		return NumpyNullableType::DATETIME_MS;
	}
	if (col_type_str == "category") {
		return NumpyNullableType::CATEGORY;
	}
	return make_error(resource, "Data type "+col_type_str+" not recognized");
}

core::result_wrapper_t<NumpyType> ConvertNumpyType(std::pmr::memory_resource *resource, const py::handle &col_type) {
	auto col_type_str = string(py::str(col_type));
	NumpyType numpy_type;

	auto internal = ConvertNumpyTypeInternal(resource, col_type_str);
	if (internal.has_error()) {
		return internal.convert_error<NumpyType>();
	}
	numpy_type.type = internal.value();
	if (IsDateTime(numpy_type.type)) {
		if (hasattr(col_type, "tz")) {
			// The datetime has timezone information.
			numpy_type.has_timezone = true;
		}
	}
	return numpy_type;
}

core::result_wrapper_t<components::types::complex_logical_type> NumpyToLogicalType(std::pmr::memory_resource *resource, const NumpyType &col_type) {
	switch (col_type.type) {
	case NumpyNullableType::BOOL:
		return logical_type::BOOLEAN;
	case NumpyNullableType::INT_8:
		return logical_type::TINYINT;
	case NumpyNullableType::UINT_8:
		return logical_type::UTINYINT;
	case NumpyNullableType::INT_16:
		return logical_type::SMALLINT;
	case NumpyNullableType::UINT_16:
		return logical_type::USMALLINT;
	case NumpyNullableType::INT_32:
		return logical_type::INTEGER;
	case NumpyNullableType::UINT_32:
		return logical_type::UINTEGER;
	case NumpyNullableType::INT_64:
		return logical_type::BIGINT;
	case NumpyNullableType::UINT_64:
		return logical_type::UBIGINT;
	case NumpyNullableType::FLOAT_16:
		return logical_type::FLOAT;
	case NumpyNullableType::FLOAT_32:
		return logical_type::FLOAT;
	case NumpyNullableType::FLOAT_64:
		return logical_type::DOUBLE;
	case NumpyNullableType::STRING:
		return logical_type::STRING_LITERAL;
	case NumpyNullableType::OBJECT:
		return logical_type::STRING_LITERAL;
	case NumpyNullableType::DATETIME_MS:
	case NumpyNullableType::DATETIME_NS:
	case NumpyNullableType::DATETIME_S:
	case NumpyNullableType::DATETIME_US: {
		return logical_type::TIMESTAMP;
	}
	default:
		return make_error(resource, "No known conversion for NumpyNullableType "+to_string(static_cast<unsigned int>(col_type.type))+" to logical_type");
	}
}

} // namespace otterbrix
