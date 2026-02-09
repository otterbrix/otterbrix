#include "python_objects.hpp"

#include <connection_environment/connection_environment.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <boost/lexical_cast.hpp>

#include <ctime>
#include <sstream>
#include <stdexcept>
#include <limits>

// #include "datetime.h" // Python datetime initialize #1

using components::types::logical_type;
using components::types::logical_value_t;
using components::types::complex_logical_type;

namespace otterbrix {

PyDictionary::PyDictionary(py::object dict) {
	keys = py::list(dict.attr("keys")());
	values = py::list(dict.attr("values")());
	len = py::len(keys);
	this->dict = std::move(dict);
}


PyDecimal::PyDecimal(py::handle &obj) : obj(obj) {
	auto as_tuple = obj.attr("as_tuple")();

	py::object exponent = as_tuple.attr("exponent");
	SetExponent(exponent);

	auto sign = py::cast<int8_t>(as_tuple.attr("sign"));
	signed_value = sign != 0;

	auto decimal_digits = as_tuple.attr("digits");
	auto width = py::len(decimal_digits);
	digits.reserve(width);
	for (auto digit : decimal_digits) {
		digits.push_back(py::cast<uint8_t>(digit));
	}
}

bool PyDecimal::TryGetType(complex_logical_type &type) {
	int32_t width = digits.size();

	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
	case PyDecimalExponentType::EXPONENT_POWER: {
		auto scale = exponent_value;
		if (exponent_type == PyDecimalExponentType::EXPONENT_POWER) {
			width += scale;
		}
		if (scale > width) {
			// The value starts with 1 or more zeros, which are optimized out of the 'digits' array
			// 0.001; width=1, exponent=-3
			width = scale + 1; // DECIMAL(4,3) - add 1 for the non-decimal values
		}
		if (width > std::numeric_limits<int64_t>::digits10) {
			type = logical_type::DOUBLE;
			return true;
		}
		type = complex_logical_type::create_decimal(width, scale);
		return true;
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		type = logical_type::FLOAT;
		return true;
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		type = logical_type::FLOAT;
		return true;
	}
	default: // LCOV_EXCL_START
		throw std::runtime_error("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
	}
	return true;
}
// LCOV_EXCL_START
static void ExponentNotRecognized() {
    // no implementation
	throw std::runtime_error("Failed to convert decimal.Decimal value, exponent type is unknown");
}
// LCOV_EXCL_STOP

void PyDecimal::SetExponent(py::handle &exponent) {
	if (py::isinstance<py::int_>(exponent)) {
		this->exponent_value = py::cast<int32_t>(exponent);
		if (this->exponent_value >= 0) {
			exponent_type = PyDecimalExponentType::EXPONENT_POWER;
			return;
		}
		exponent_value *= -1;
		exponent_type = PyDecimalExponentType::EXPONENT_SCALE;
		return;
	}
	if (py::isinstance<py::str>(exponent)) {
		string exponent_string = py::str(exponent);
		if (exponent_string == "n") {
			exponent_type = PyDecimalExponentType::EXPONENT_NAN;
			return;
		}
		if (exponent_string == "F") {
			exponent_type = PyDecimalExponentType::EXPONENT_INFINITY;
			return;
		}
	}
	// LCOV_EXCL_START
	ExponentNotRecognized();
	// LCOV_EXCL_STOP
}

static bool WidthFitsInDecimal(int32_t width) {
	return width >= 0 && width <= std::numeric_limits<int64_t>::digits10; 
}

template <class OP>
logical_value_t PyDecimalCastSwitch(PyDecimal &decimal, uint8_t width, uint8_t scale) {
	/*if (width > DecimalWidth<int64_t>::max) {
		return OP::template Operation<int128_t>(decimal.signed_value, decimal.digits, width, scale);
	}*/
	//if (width > DecimalWidth<int32_t>::max) {
		return OP::template Operation<int64_t>(decimal.signed_value, decimal.digits, width, scale);
	//}
	/*if (width > DecimalWidth<int16_t>::max) {
		return OP::template Operation<int32_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	return OP::template Operation<int16_t>(decimal.signed_value, decimal.digits, width, scale);
    */
}

// Wont fit in a DECIMAL, fall back to DOUBLE
static logical_value_t CastToDouble(py::handle &obj) {
	return logical_value_t(py::cast<double>(obj));
}

logical_value_t PyDecimal::to_logical_value() {
	int32_t width = digits.size();
	if (!WidthFitsInDecimal(width)) {
		return CastToDouble(obj);
	}
	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
		uint8_t scale = exponent_value;
		assert(WidthFitsInDecimal(width));
		if (scale > width) {
			// Values like '0.001'
			width = scale + 1; // leave 1 room for the non-decimal value
		}
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalScaleConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_POWER: {
		uint8_t scale = exponent_value;
		width += scale;
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalPowerConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		return logical_value_t(NAN);
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		return logical_value_t(INFINITY);
	}
	// LCOV_EXCL_START
	default: {
		throw std::runtime_error("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
	}
}

enum class InfinityType : uint8_t { NONE, POSITIVE, NEGATIVE };

// OtterBrix timestamp
using timestamp_t = int64_t;

InfinityType GetTimestampInfinityType(timestamp_t &timestamp) {
	// constans for Unix timestamp
	if (timestamp == std::numeric_limits<timestamp_t>::max()) {
		return InfinityType::POSITIVE;
	}
	if (timestamp ==std::numeric_limits<timestamp_t>::min()) {
		return InfinityType::NEGATIVE;
	}
	return InfinityType::NONE;
}

py::object PythonObject::FromStruct(const logical_value_t &val, const complex_logical_type &type) {
	auto &struct_values = val.children();

	auto &child_types = type.child_types(); 
    // unnamed struct
	if (child_types.empty() || child_types[0].alias().empty()) {
		py::tuple py_tuple(struct_values.size());
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_type = child_types[i];
			assert(child_type.alias().empty());
			py_tuple[i] = FromValue(struct_values[i], child_type);
		}
		return std::move(py_tuple);
	} else {
		py::dict py_struct;
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_type = child_types[i];
			py_struct[child_type.alias().c_str()] = FromValue(struct_values[i], child_type); 
		}
		return std::move(py_struct);
	}
}

static bool KeyIsHashable(const complex_logical_type &type) {
	switch (type.type()) {
    case logical_type::BOOLEAN:
	case logical_type::TINYINT:
	case logical_type::SMALLINT:
	case logical_type::INTEGER:
	case logical_type::BIGINT:
	case logical_type::UTINYINT:
	case logical_type::USMALLINT:
	case logical_type::UINTEGER:
	case logical_type::UBIGINT:
	case logical_type::HUGEINT:
	case logical_type::UHUGEINT:
	case logical_type::FLOAT:
	case logical_type::DOUBLE:
	case logical_type::DECIMAL:
	case logical_type::ENUM:
	case logical_type::STRING_LITERAL:
	case logical_type::BLOB:
	case logical_type::BIT:
	case logical_type::TIMESTAMP_MS:
	case logical_type::TIMESTAMP_US:
	case logical_type::TIMESTAMP_NS:
	case logical_type::TIMESTAMP_SEC:
	case logical_type::UUID:
		return true;
	case logical_type::LIST:
	case logical_type::ARRAY:
	case logical_type::MAP:
		return false;
	case logical_type::UNION: {
		const auto& child_types = type.child_types();
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (!KeyIsHashable(child_types[i])) { /*UnionType::GetMemberType(type, i)) */
				return false;
			}
		}
		// Only if all the member types are hashable do we say the entire UNION is hashable
		return true;
	}
	case logical_type::STRUCT:
		return false;
	default:
		throw std::runtime_error("Unsupported type: "); //+ type.ToString());
	}
}

py::object PythonObject::FromValue(const logical_value_t &val, const complex_logical_type &type) {
	auto &import_cache = ConnectionEnvironment::ImportCache();
	if (val.is_null()) {
		return py::none();
	}
	switch (type.type()) {
	case logical_type::BOOLEAN:
		return py::cast(val.value<bool>());
	case logical_type::TINYINT:
		return py::cast(val.value<int8_t>());
	case logical_type::SMALLINT:
		return py::cast(val.value<int16_t>());
	case logical_type::INTEGER:
		return py::cast(val.value<int32_t>());
	case logical_type::BIGINT:
		return py::cast(val.value<int64_t>());
	case logical_type::UTINYINT:
		return py::cast(val.value<uint8_t>());
	case logical_type::USMALLINT:
		return py::cast(val.value<uint16_t>());
	case logical_type::UINTEGER:
		return py::cast(val.value<uint32_t>());
    case logical_type::UBIGINT:
		return py::cast(val.value<uint64_t>());
	case logical_type::HUGEINT: {
        std::stringstream ss;
        // ss << val.value<int128_t>();
        // auto res = ss.str();
		// return py::reinterpret_steal<py::object>(PyLong_FromString(res.c_str(), nullptr, 10));
		std::runtime_error("OtterBrix doen\'t support hugeint conversation to python object");
    }	
	case logical_type::UHUGEINT: {
        // ss << val.value<uint128_t>();
        // auto res = ss.str();
		// return py::reinterpret_steal<py::object>(PyLong_FromString(res.c_str(), nullptr, 10));
		std::runtime_error("OtterBrix doen\'t support uhugeint conversation to python object");
    }
	case logical_type::FLOAT:
		return py::cast(val.value<float>());
	case logical_type::DOUBLE:
		return py::cast(val.value<double>());
    case logical_type::DECIMAL: {
        int64_t value = val.value<int64_t>();
		auto* extention = static_cast<components::types::decimal_logical_type_extention*>(val.type().extention());
		auto scale = extention->scale();
        std::string digits = std::to_string(value);
        auto pydigits = import_cache.decimal.Decimal()(digits);
		return pydigits.attr("__truediv__")(py::cast<int>(scale));
	}
	case logical_type::ENUM: {
		std::runtime_error("OtterBrix doen\'t support enum type");
	}
	case logical_type::UNION: {
		std::runtime_error("OtterBrix doen\'t support union type");
	}
	case logical_type::STRING_LITERAL:
		return py::str(val.value<const std::string&>());
	case logical_type::BLOB:
		return py::bytes(val.value<std::string_view>());
    case logical_type::BIT:
		return py::cast(val.value<bool>()?"1":"0");
	case logical_type::TIMESTAMP_US:
	case logical_type::TIMESTAMP_MS:
	case logical_type::TIMESTAMP_NS:
	case logical_type::TIMESTAMP_SEC: {
		auto timestamp = val.value<timestamp_t>();

		InfinityType infinity = GetTimestampInfinityType(timestamp);
		if (infinity == InfinityType::POSITIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.max());
		}
		if (infinity == InfinityType::NEGATIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.min());
		}
		return py::cast(timestamp);
	}
	case logical_type::LIST: {
		auto &list_values = val.children();

		py::list list;
		for (auto &list_elem : list_values) {
			list.append(FromValue(list_elem, type.child_type()));
		}
		return std::move(list);
	}
	case logical_type::ARRAY: {
		auto &array_values = val.children();
		auto* extention = static_cast<components::types::array_logical_type_extention*>(type.extention());
		auto array_size = extention->size();
		auto &child_type = type.child_type();

		// do not remove the static cast here, it's required for building
		// otterbrix-python with Emscripten.
		//
		// without this cast, a static_assert fails in pybind11
		// because the return type of ArrayType::GetSize is idx_t,
		// which is typedef'd to uint64_t and ssize_t is 4 bytes with Emscripten
		// and pybind11 requires that the input be castable to ssize_t
		py::tuple arr(static_cast<py::ssize_t>(array_size));

		for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
			arr[elem_idx] = FromValue(array_values[elem_idx], child_type);
		}
		return std::move(arr);
	}
	case logical_type::MAP: {
		auto &list_values = val.children(); 

        auto* map_extention = static_cast<components::types::map_logical_type_extention*>(type.extention()); 
		auto &key_type = map_extention->key();
		auto &val_type = map_extention->value();

		py::dict py_struct;
		if (KeyIsHashable(key_type)) {
			for (auto &list_elem : list_values) {
				auto &struct_children = list_elem.children(); 
				auto key = PythonObject::FromValue(struct_children[0], key_type);
				auto value = PythonObject::FromValue(struct_children[1], val_type);
				py_struct[std::move(key)] = std::move(value);
			}
		} else {
			py::list keys;
			py::list values;
			for (auto &list_elem : list_values) {
				auto &struct_children = list_elem.children();
				keys.append(PythonObject::FromValue(struct_children[0], key_type));
				values.append(PythonObject::FromValue(struct_children[1], val_type));
			}
			py_struct["key"] = std::move(keys);
			py_struct["value"] = std::move(values);
		}
		return std::move(py_struct);
	}
	case logical_type::STRUCT: {
		return FromStruct(val, type);
	}
	case logical_type::UUID: {
		// auto uuid_value = val.GetValueUnsafe<int128_t>();
		// return import_cache.uuid.UUID()(UUID::ToString(uuid_value));
		std::runtime_error("OtterBrix doen\'t support uhugeint conversation to python object");

	}

	default:
		throw std::runtime_error("Unsupported type: "); //+ type.ToString());
	}
}

} // namespace otterbrix
