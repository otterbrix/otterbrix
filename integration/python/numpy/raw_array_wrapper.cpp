#include "raw_array_wrapper.hpp"


#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

struct numpy_type_info {
	idx_t width;
	const char *dtype;
};

// Single source of truth mapping an OtterBrix logical_type to its NumPy memory width and dtype string.
// (Previously this was two separate switches: GetNumpyTypeWidth and OtterBrixToNumpyDtype.)
// Returns false for unsupported types; the human-readable error is composed by the caller that
// owns a memory_resource (OtterBrixToNumpyDtype / Initialize).
bool GetNumpyTypeInfo(const complex_logical_type &type, numpy_type_info &out) {
	switch (type.type()) {
	case logical_type::BOOLEAN:
		out = numpy_type_info{sizeof(bool), "bool"};
		return true;
	case logical_type::TINYINT:
		out = numpy_type_info{sizeof(int8_t), "int8"};
		return true;
	case logical_type::SMALLINT:
		out = numpy_type_info{sizeof(int16_t), "int16"};
		return true;
	case logical_type::INTEGER:
		out = numpy_type_info{sizeof(int32_t), "int32"};
		return true;
	case logical_type::BIGINT:
		out = numpy_type_info{sizeof(int64_t), "int64"};
		return true;
	case logical_type::UTINYINT:
		out = numpy_type_info{sizeof(uint8_t), "uint8"};
		return true;
	case logical_type::USMALLINT:
		out = numpy_type_info{sizeof(uint16_t), "uint16"};
		return true;
	case logical_type::UINTEGER:
		out = numpy_type_info{sizeof(uint32_t), "uint32"};
		return true;
	case logical_type::UBIGINT:
		out = numpy_type_info{sizeof(uint64_t), "uint64"};
		return true;
	case logical_type::FLOAT:
		out = numpy_type_info{sizeof(float), "float32"};
		return true;
	case logical_type::HUGEINT:
	case logical_type::DOUBLE:
	case logical_type::DECIMAL:
		out = numpy_type_info{sizeof(double), "float64"};
		return true;
	case logical_type::TIMESTAMP:
		out = numpy_type_info{sizeof(int64_t), "datetime64[us]"};
		return true;
	case logical_type::STRING_LITERAL:
	case logical_type::BIT:
	case logical_type::BLOB:
	case logical_type::ENUM:
	case logical_type::LIST:
	case logical_type::MAP:
	case logical_type::STRUCT:
	case logical_type::UNION:
	case logical_type::UUID:
	case logical_type::ARRAY:
		out = numpy_type_info{sizeof(PyObject *), "object"};
		return true;
	default:
		return false;
	}
}

} // namespace

RawArrayWrapper::RawArrayWrapper(const complex_logical_type &type) : data(nullptr), type(type), count(0) {
	// Width resolution is deferred to Initialize() for unsupported types so the constructor stays
	// non-throwing; the unsupported case is reported there via core::error_t.
	numpy_type_info info{};
	type_width = GetNumpyTypeInfo(type, info) ? info.width : 0;
}

core::result_wrapper_t<std::string> RawArrayWrapper::OtterBrixToNumpyDtype(std::pmr::memory_resource *resource, const complex_logical_type &type) {
	numpy_type_info info{};
	if (!GetNumpyTypeInfo(type, info)) {
		return core::error_t{core::error_code_t::other_error,
		                     std::pmr::string{"Unsupported type "+std::to_string(int(type.type()))+" for OtterBrix -> NumPy conversion", resource}};
	}
	return std::string(info.dtype);
}

core::error_t RawArrayWrapper::Initialize(std::pmr::memory_resource *resource, idx_t capacity) {
	auto dtype = OtterBrixToNumpyDtype(resource, type);
	if (dtype.has_error()) {
		return dtype.error();
	}

	array = py::array(py::dtype(dtype.value()), capacity);
	data = data_ptr_cast(array.mutable_data());
	return core::error_t::no_error();
}

void RawArrayWrapper::Resize(idx_t new_capacity) {
	std::vector<py::ssize_t> new_shape {py::ssize_t(new_capacity)};
	array.resize(new_shape, false);
	data = data_ptr_cast(array.mutable_data());
}

} // namespace otterbrix
