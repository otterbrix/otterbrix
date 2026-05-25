#include "raw_array_wrapper.hpp"

#include <core/types/vector.hpp>
#include <core/types/string.hpp>

#include <stdexcept>

namespace otterbrix {

using components::types::complex_logical_type;
using components::types::logical_type;

static idx_t GetNumpyTypeWidth(const complex_logical_type &type) {
	switch (type.type()) {
	case logical_type::BOOLEAN:
		return sizeof(bool);
	case logical_type::UTINYINT:
		return sizeof(uint8_t);
	case logical_type::USMALLINT:
		return sizeof(uint16_t);
	case logical_type::UINTEGER:
		return sizeof(uint32_t);
	case logical_type::UBIGINT:
		return sizeof(uint64_t);
	case logical_type::TINYINT:
		return sizeof(int8_t);
	case logical_type::SMALLINT:
		return sizeof(int16_t);
	case logical_type::INTEGER:
		return sizeof(int32_t);
	case logical_type::BIGINT:
		return sizeof(int64_t);
	case logical_type::FLOAT:
		return sizeof(float);
	case logical_type::HUGEINT:
	case logical_type::DOUBLE:
	case logical_type::DECIMAL:
		return sizeof(double);
	case logical_type::TIMESTAMP_SEC:
	case logical_type::TIMESTAMP_MS:
	case logical_type::TIMESTAMP_US:
	case logical_type::TIMESTAMP_NS:
		return sizeof(int64_t);
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
		return sizeof(PyObject *);
	default:
		throw std::runtime_error("Unsupported type "+to_string(int(type.type()))+" for OtterBrix -> NumPy conversion");
	}
}

RawArrayWrapper::RawArrayWrapper(const complex_logical_type &type) : data(nullptr), type(type), count(0) {
	type_width = GetNumpyTypeWidth(type);
}

string RawArrayWrapper::OtterBrixToNumpyDtype(const complex_logical_type &type) {
	switch (type.type()) {
	case logical_type::BOOLEAN:
		return "bool";
	case logical_type::TINYINT:
		return "int8";
	case logical_type::SMALLINT:
		return "int16";
	case logical_type::INTEGER:
		return "int32";
	case logical_type::BIGINT:
		return "int64";
	case logical_type::UTINYINT:
		return "uint8";
	case logical_type::USMALLINT:
		return "uint16";
	case logical_type::UINTEGER:
		return "uint32";
	case logical_type::UBIGINT:
		return "uint64";
	case logical_type::FLOAT:
		return "float32";
	case logical_type::HUGEINT:
	case logical_type::DOUBLE:
	case logical_type::DECIMAL:
		return "float64";
	case logical_type::TIMESTAMP_US:
		return "datetime64[us]";
	case logical_type::TIMESTAMP_NS:
		return "datetime64[ns]";
	case logical_type::TIMESTAMP_MS:
		return "datetime64[ms]";
	case logical_type::TIMESTAMP_SEC:
		return "datetime64[s]";
	case logical_type::STRING_LITERAL:
	case logical_type::BIT:
	case logical_type::BLOB:
	case logical_type::LIST:
	case logical_type::MAP:
	case logical_type::STRUCT:
	case logical_type::UNION:
	case logical_type::UUID:
	case logical_type::ARRAY:
		return "object";
	default:
		throw std::runtime_error("Unsupported type "+to_string(int(type.type())));
	}
}

void RawArrayWrapper::Initialize(idx_t capacity) {
	string dtype = OtterBrixToNumpyDtype(type);

	array = py::array(py::dtype(dtype), capacity);
	data = data_ptr_cast(array.mutable_data());
}

void RawArrayWrapper::Resize(idx_t new_capacity) {
	vector<py::ssize_t> new_shape {py::ssize_t(new_capacity)};
	array.resize(new_shape, false);
	data = data_ptr_cast(array.mutable_data());
}

} // namespace otterbrix
