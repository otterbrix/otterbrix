#include "numpy_type.hpp"

#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {

using components::types::logical_type;

static core::error_t make_error(std::pmr::memory_resource *resource, const std::string &what) {
	return core::error_t{core::error_code_t::other_error, std::pmr::string{what, resource}};
}

static bool is_date_time(numpy_nullable_type_t type) {
	switch (type) {
	case numpy_nullable_type_t::DATETIME_NS:
	case numpy_nullable_type_t::DATETIME_S:
	case numpy_nullable_type_t::DATETIME_MS:
	case numpy_nullable_type_t::DATETIME_US:
		return true;
	default:
		return false;
	};
}

static core::result_wrapper_t<numpy_nullable_type_t> convert_numpy_type_internal(std::pmr::memory_resource *resource, const std::string &col_type_str) {
	if (col_type_str == "bool" || col_type_str == "boolean") {
		return numpy_nullable_type_t::BOOL;
	}
	if (col_type_str == "uint8" || col_type_str == "UInt8") {
		return numpy_nullable_type_t::UINT_8;
	}
	if (col_type_str == "uint16" || col_type_str == "UInt16") {
		return numpy_nullable_type_t::UINT_16;
	}
	if (col_type_str == "uint32" || col_type_str == "UInt32") {
		return numpy_nullable_type_t::UINT_32;
	}
	if (col_type_str == "uint64" || col_type_str == "UInt64") {
		return numpy_nullable_type_t::UINT_64;
	}
	if (col_type_str == "int8" || col_type_str == "Int8") {
		return numpy_nullable_type_t::INT_8;
	}
	if (col_type_str == "int16" || col_type_str == "Int16") {
		return numpy_nullable_type_t::INT_16;
	}
	if (col_type_str == "int32" || col_type_str == "Int32") {
		return numpy_nullable_type_t::INT_32;
	}
	if (col_type_str == "int64" || col_type_str == "Int64") {
		return numpy_nullable_type_t::INT_64;
	}
	if (col_type_str == "float16" || col_type_str == "Float16") {
		return numpy_nullable_type_t::FLOAT_16;
	}
	if (col_type_str == "float32" || col_type_str == "Float32") {
		return numpy_nullable_type_t::FLOAT_32;
	}
	if (col_type_str == "float64" || col_type_str == "Float64") {
		return numpy_nullable_type_t::FLOAT_64;
	}
	if (col_type_str == "string" || col_type_str == "str") {
		return numpy_nullable_type_t::STRING;
	}
	if (col_type_str == "object") {
		return numpy_nullable_type_t::OBJECT;
	}
	if (col_type_str == "timedelta64[ns]") {
		return numpy_nullable_type_t::TIMEDELTA;
	}
	// We use 'starts_with' because it might have ', tz' at the end, indicating timezone
	if (col_type_str.starts_with("datetime64[ns")) {
		return numpy_nullable_type_t::DATETIME_NS;
	}
	if (col_type_str.starts_with("datetime64[us")) {
		return numpy_nullable_type_t::DATETIME_US;
	}
	if (col_type_str.starts_with("datetime64[ms")) {
		return numpy_nullable_type_t::DATETIME_MS;
	}
	if (col_type_str.starts_with("datetime64[s")) {
		return numpy_nullable_type_t::DATETIME_S;
	}
	// Legacy datetime type indicators
	if (col_type_str.starts_with("<M8[ns")) {
		return numpy_nullable_type_t::DATETIME_NS;
	}
	if (col_type_str.starts_with("<M8[s")) {
		return numpy_nullable_type_t::DATETIME_S;
	}
	if (col_type_str.starts_with("<M8[us")) {
		return numpy_nullable_type_t::DATETIME_US;
	}
	if (col_type_str.starts_with("<M8[ms")) {
		return numpy_nullable_type_t::DATETIME_MS;
	}
	if (col_type_str == "category") {
		return numpy_nullable_type_t::CATEGORY;
	}
	return make_error(resource, "Data type "+col_type_str+" not recognized");
}

core::result_wrapper_t<numpy_type_t> convert_numpy_type(std::pmr::memory_resource *resource, const py::handle &col_type) {
	auto col_type_str = std::string(py::str(col_type));
	numpy_type_t numpy_type;

	auto internal = convert_numpy_type_internal(resource, col_type_str);
	if (internal.has_error()) {
		return internal.convert_error<numpy_type_t>();
	}
	numpy_type.type = internal.value();
	if (is_date_time(numpy_type.type)) {
		if (hasattr(col_type, "tz")) {
			// The datetime has timezone information.
			numpy_type.has_timezone = true;
		}
	}
	return numpy_type;
}

core::result_wrapper_t<components::types::complex_logical_type> numpy_to_logical_type(std::pmr::memory_resource *resource, const numpy_type_t &col_type) {
	switch (col_type.type) {
	case numpy_nullable_type_t::BOOL:
		return logical_type::BOOLEAN;
	case numpy_nullable_type_t::INT_8:
		return logical_type::TINYINT;
	case numpy_nullable_type_t::UINT_8:
		return logical_type::UTINYINT;
	case numpy_nullable_type_t::INT_16:
		return logical_type::SMALLINT;
	case numpy_nullable_type_t::UINT_16:
		return logical_type::USMALLINT;
	case numpy_nullable_type_t::INT_32:
		return logical_type::INTEGER;
	case numpy_nullable_type_t::UINT_32:
		return logical_type::UINTEGER;
	case numpy_nullable_type_t::INT_64:
		return logical_type::BIGINT;
	case numpy_nullable_type_t::UINT_64:
		return logical_type::UBIGINT;
	case numpy_nullable_type_t::FLOAT_16:
		return logical_type::FLOAT;
	case numpy_nullable_type_t::FLOAT_32:
		return logical_type::FLOAT;
	case numpy_nullable_type_t::FLOAT_64:
		return logical_type::DOUBLE;
	case numpy_nullable_type_t::STRING:
		return logical_type::STRING_LITERAL;
	case numpy_nullable_type_t::OBJECT:
		return logical_type::STRING_LITERAL;
	case numpy_nullable_type_t::DATETIME_MS:
	case numpy_nullable_type_t::DATETIME_NS:
	case numpy_nullable_type_t::DATETIME_S:
	case numpy_nullable_type_t::DATETIME_US: {
		return logical_type::TIMESTAMP;
	}
	default:
		return make_error(resource, "No known conversion for NumpyNullableType "+std::to_string(static_cast<unsigned int>(col_type.type))+" to logical_type");
	}
}

} // namespace otterbrix
