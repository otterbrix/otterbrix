#include "python_objects.hpp"

#include <connection_environment/connection_environment.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <boost/lexical_cast.hpp>

#include <ctime>
#include <memory_resource>
#include <sstream>
#include <limits>

using components::types::logical_type;
using components::types::logical_value_t;
using components::types::complex_logical_type;

namespace otterbrix {

static core::error_t make_error(std::pmr::memory_resource* r, const std::string& what) {
	return core::error_t{core::error_code_t::other_error, std::pmr::string{what, r}};
}

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

	if (!exponent_recognized) {
		// Failed to convert decimal.Decimal value, exponent type is unknown
		return false;
	}

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
		return false;
	} // LCOV_EXCL_STOP
	}
	return true;
}

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
	// Failed to convert decimal.Decimal value, exponent type is unknown.
	// Recorded here without throwing; surfaced by TryGetType / to_logical_value.
	exponent_recognized = false;
	// LCOV_EXCL_STOP
}

static bool WidthFitsInDecimal(int32_t width) {
	return width >= 0 && width <= std::numeric_limits<int64_t>::digits10; 
}

template <class OP>
logical_value_t PyDecimalCastSwitch(std::pmr::memory_resource* r, PyDecimal &decimal, uint8_t width, uint8_t scale) {
		return OP::template Operation<int64_t>(r, decimal.signed_value, decimal.digits, width, scale);
}

// Wont fit in a DECIMAL, fall back to DOUBLE
static logical_value_t CastToDouble(std::pmr::memory_resource* r, py::handle &obj) {
	return logical_value_t(r, py::cast<double>(obj));
}

core::result_wrapper_t<logical_value_t> PyDecimal::to_logical_value(std::pmr::memory_resource* r) {
	if (!exponent_recognized) {
		return make_error(r, "Failed to convert decimal.Decimal value, exponent type is unknown");
	}
	int32_t width = digits.size();
	if (!WidthFitsInDecimal(width)) {
		return CastToDouble(r, obj);
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
			return CastToDouble(r, obj);
		}
		return PyDecimalCastSwitch<PyDecimalScaleConverter>(r, *this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_POWER: {
		uint8_t scale = exponent_value;
		width += scale;
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(r, obj);
		}
		return PyDecimalCastSwitch<PyDecimalPowerConverter>(r, *this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		return logical_value_t(r, NAN);
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		return logical_value_t(r, INFINITY);
	}
	// LCOV_EXCL_START
	default: {
		return make_error(r, "case not implemented for type PyDecimalExponentType");
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

core::result_wrapper_t<py::object> PythonObject::FromStruct(std::pmr::memory_resource* r, const logical_value_t &val, const complex_logical_type &type) {
	auto &struct_values = val.children();

	auto &child_types = type.child_types();
    // unnamed struct
	if (child_types.empty() || child_types[0].alias().empty()) {
		py::tuple py_tuple(struct_values.size());
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_type = child_types[i];
			assert(child_type.alias().empty());
			auto field = FromValue(r, struct_values[i], child_type);
			if (field.has_error()) {
				return field.error();
			}
			py_tuple[i] = field.value();
		}
		return py::object(std::move(py_tuple));
	} else {
		py::dict py_struct;
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_type = child_types[i];
			auto field = FromValue(r, struct_values[i], child_type);
			if (field.has_error()) {
				return field.error();
			}
			py_struct[child_type.alias().c_str()] = field.value();
		}
		return py::object(std::move(py_struct));
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
	case logical_type::TIMESTAMP:
	case logical_type::UUID:
		return true;
	case logical_type::LIST:
	case logical_type::ARRAY:
	case logical_type::MAP:
		return false;
	case logical_type::UNION: {
		const auto& child_types = type.child_types();
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (!KeyIsHashable(child_types[i])) {
				return false;
			}
		}
		// Only if all the member types are hashable do we say the entire UNION is hashable
		return true;
	}
	case logical_type::STRUCT:
		return false;
	default:
		// Unsupported key types are treated as non-hashable here; the genuine "unsupported type"
		// error is surfaced when FromValue recurses into the key/value below.
		return false;
	}
}

core::result_wrapper_t<py::object> PythonObject::FromValue(std::pmr::memory_resource* r, const logical_value_t &val, const complex_logical_type &type) {
	auto &import_cache = ConnectionEnvironment::ImportCache();
	if (val.is_null()) {
		return py::object(py::none());
	}
	switch (type.type()) {
	case logical_type::BOOLEAN:
		return py::object(py::cast(val.value<bool>()));
	case logical_type::TINYINT:
		return py::object(py::cast(val.value<int8_t>()));
	case logical_type::SMALLINT:
		return py::object(py::cast(val.value<int16_t>()));
	case logical_type::INTEGER:
		return py::object(py::cast(val.value<int32_t>()));
	case logical_type::BIGINT:
		return py::object(py::cast(val.value<int64_t>()));
	case logical_type::UTINYINT:
		return py::object(py::cast(val.value<uint8_t>()));
	case logical_type::USMALLINT:
		return py::object(py::cast(val.value<uint16_t>()));
	case logical_type::UINTEGER:
		return py::object(py::cast(val.value<uint32_t>()));
    case logical_type::UBIGINT:
		return py::object(py::cast(val.value<uint64_t>()));
	case logical_type::HUGEINT: {
		return make_error(r, "OtterBrix doen\'t support hugeint conversation to python object");
    }
	case logical_type::UHUGEINT: {
		return make_error(r, "OtterBrix doen\'t support uhugeint conversation to python object");
    }
	case logical_type::FLOAT:
		return py::object(py::cast(val.value<float>()));
	case logical_type::DOUBLE:
		return py::object(py::cast(val.value<double>()));
    case logical_type::DECIMAL: {
        int64_t value = val.value<int64_t>();
		auto* extension = static_cast<components::types::decimal_logical_type_extension*>(val.type().extension());
		auto scale = extension->scale();
        std::string digits = std::to_string(value);
        auto pydigits = import_cache.decimal.Decimal()(digits);
		return py::object(pydigits.attr("__truediv__")(py::cast<int>(scale)));
	}
	case logical_type::ENUM: {
		return make_error(r, "OtterBrix doen\'t support enum type");
	}
	case logical_type::UNION: {
		return make_error(r, "OtterBrix doen\'t support union type");
	}
	case logical_type::STRING_LITERAL:
		return py::object(py::str(val.value<const std::string&>()));
	case logical_type::BLOB:
		return py::object(py::bytes(val.value<std::string_view>()));
    case logical_type::BIT:
		return py::object(py::cast(val.value<bool>()?"1":"0"));
	case logical_type::TIMESTAMP: {
		auto timestamp = val.value<timestamp_t>();

		InfinityType infinity = GetTimestampInfinityType(timestamp);
		if (infinity == InfinityType::POSITIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.max());
		}
		if (infinity == InfinityType::NEGATIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.min());
		}
		return py::object(py::cast(timestamp));
	}
	case logical_type::LIST: {
		auto &list_values = val.children();

		py::list list;
		for (auto &list_elem : list_values) {
			auto elem = FromValue(r, list_elem, type.child_type());
			if (elem.has_error()) {
				return elem.error();
			}
			list.append(elem.value());
		}
		return py::object(std::move(list));
	}
	case logical_type::ARRAY: {
		auto &array_values = val.children();
		auto* extension = static_cast<components::types::array_logical_type_extension*>(type.extension());
		auto array_size = extension->size();
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
			auto elem = FromValue(r, array_values[elem_idx], child_type);
			if (elem.has_error()) {
				return elem.error();
			}
			arr[elem_idx] = elem.value();
		}
		return py::object(std::move(arr));
	}
	case logical_type::MAP: {
		auto &list_values = val.children();

        auto* map_extension = static_cast<components::types::map_logical_type_extension*>(type.extension());
		auto &key_type = map_extension->key();
		auto &val_type = map_extension->value();

		py::dict py_struct;
		if (KeyIsHashable(key_type)) {
			for (auto &list_elem : list_values) {
				auto &struct_children = list_elem.children();
				auto key = PythonObject::FromValue(r, struct_children[0], key_type);
				if (key.has_error()) {
					return key.error();
				}
				auto value = PythonObject::FromValue(r, struct_children[1], val_type);
				if (value.has_error()) {
					return value.error();
				}
				py_struct[key.value()] = value.value();
			}
		} else {
			py::list keys;
			py::list values;
			for (auto &list_elem : list_values) {
				auto &struct_children = list_elem.children();
				auto key = PythonObject::FromValue(r, struct_children[0], key_type);
				if (key.has_error()) {
					return key.error();
				}
				auto value = PythonObject::FromValue(r, struct_children[1], val_type);
				if (value.has_error()) {
					return value.error();
				}
				keys.append(key.value());
				values.append(value.value());
			}
			py_struct["key"] = std::move(keys);
			py_struct["value"] = std::move(values);
		}
		return py::object(std::move(py_struct));
	}
	case logical_type::STRUCT: {
		return FromStruct(r, val, type);
	}
	case logical_type::UUID: {
		return make_error(r, "OtterBrix doen\'t support uhugeint conversation to python object");
	}

	default:
		return make_error(r, "Unsupported type: ");
	}
}

} // namespace otterbrix
