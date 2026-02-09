#include "python_conversion.hpp"

#include "python_objects.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <connection_environment/connection_environment.hpp>

#include <core/string_util/case_insensitive.hpp>
#include <core/typedefs.hpp>

// #include "datetime.h" //From Python

#include <limits>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>
#include <stdexcept>

using components::types::logical_value_t;
using components::types::physical_type;
using components::types::logical_type;
using components::types::complex_logical_type;

namespace otterbrix {


    logical_value_t TransformListValue(py::handle ele);

    static logical_value_t EmptyMapValue() {
	    return logical_value_t::create_map(
                logical_type::NA, logical_type::NA, 
                vector<logical_value_t>(), vector<logical_value_t>()
                );
    }


    vector<string> TransformStructKeys(py::handle keys, idx_t size) {
        vector<string> res;
    	res.reserve(size);
    	for (idx_t i = 0; i < size; i++) {
    		res.emplace_back(py::str(keys.attr("__getitem__")(i)));
    	}
    	return res;
    }
    
    static bool IsValidMapComponent(const py::handle &component) {
    	// The component is either NULL
    	if (py::none().is(component)) {
    		return true;
    	}
    	if (!py::hasattr(component, "__getitem__")) {
    		return false;
    	}
    	if (!py::hasattr(component, "__len__")) {
    		return false;
    	}
    	return true;
    }
    
    bool DictionaryHasMapFormat(const PyDictionary &dict) {
    	if (dict.len != 2) {
    		return false;
    	}
    
    	//{ 'key': [ .. keys .. ], 'value': [ .. values .. ]}
    	auto keys_key = py::str("key");
    	auto values_key = py::str("value");
    	auto keys = dict[keys_key];
    	auto values = dict[values_key];
    	if (!keys || !values) {
    		return false;
    	}
    
    	if (!IsValidMapComponent(keys)) {
    		return false;
    	}
    	if (!IsValidMapComponent(values)) {
    		return false;
    	}
    
    	// If either of the components is NULL, return early
    	if (py::none().is(keys) || py::none().is(values)) {
    		return true;
    	}
    
    	// Verify that both the keys and values are of the same length
    	auto size = py::len(keys);
    	if (size != py::len(values)) {
    		return false;
    	}
    	return true;
    }

logical_value_t TransformDictionaryToStruct(
        const PyDictionary &dict, 
        const complex_logical_type &target_type = logical_type::UNKNOWN) {
	auto struct_keys = TransformStructKeys(dict.keys, dict.len);

	bool struct_target = target_type.type() == logical_type::STRUCT;
    components::types::struct_logical_type_extention* struct_extention = nullptr;
	if (struct_target) {
        struct_extention = 
            static_cast<components::types::struct_logical_type_extention*>(target_type.extention());
        if (dict.len != struct_extention->child_types().size()) {
		    throw std::runtime_error(
                    "We could not convert the object "+dict.ToString()+" to the target struct type with "+
                    to_string(struct_extention->child_types().size())+" fields"
                    );
        }
	} 

	case_insensitive_map_t<idx_t> key_mapping;
	for (idx_t i = 0; i < struct_keys.size(); i++) {
		key_mapping[struct_keys[i]] = i;
	}

    vector<logical_value_t> struct_values;
	for (idx_t i = 0; i < dict.len; i++) {
        if (struct_target) {
            auto& key = struct_extention->child_types().at(i).alias();
		    auto value_index = key_mapping[key];
            auto& child_type = target_type.child_types().at(i);
		    auto val = TransformPythonValue(dict.values.attr("__getitem__")(value_index), child_type);
            val.set_alias(key);
		    struct_values.emplace_back(std::move(val));
        } else {
            auto& key = struct_keys[i];
		    auto value_index = key_mapping[key];
            auto child_type = logical_type::UNKNOWN;
		    auto val = TransformPythonValue(dict.values.attr("__getitem__")(value_index), child_type);
            val.set_alias(key);
		    struct_values.emplace_back(std::move(val));
        }
	}
	return logical_value_t::create_struct(struct_values);
}

logical_value_t TransformStructFormatDictionaryToMap(const PyDictionary &dict, const complex_logical_type &target_type) {
	if (dict.len == 0) {
		return EmptyMapValue();
	}

	if (target_type.type() != logical_type::MAP) {
		throw std::runtime_error("Please provide a valid target type for transform from Python to Value");
	}

	if (py::none().is(dict.keys) || py::none().is(dict.values)) {
        // todo should create logical_value with field is_null=true, but the field was removed
		return EmptyMapValue(); 
	}

	auto size = py::len(dict.keys);
	assert(size == py::len(dict.values));

    auto* map_extention = static_cast<components::types::map_logical_type_extention*>(target_type.extention()); 
	auto key_target = map_extention->key();
	auto value_target = map_extention->value();

	complex_logical_type key_type = logical_type::NA;
	complex_logical_type value_type = logical_type::NA;

    vector<logical_value_t> key_values;
    vector<logical_value_t> value_values;

	for (idx_t i = 0; i < size; i++) {

		logical_value_t new_key = TransformPythonValue(dict.keys.attr("__getitem__")(i), key_target);
		logical_value_t new_value = TransformPythonValue(dict.values.attr("__getitem__")(i), value_target);

        //todo common type
		//key_type = logical_type::ForceMaxLogicalType(key_type, new_key.type());
		//value_type = logical_type::ForceMaxLogicalType(value_type, new_value.type());

        new_key.set_alias("key");
        new_value.set_alias("value");
        key_values.emplace_back(new_key);
        value_values.emplace_back(new_value);
    }

	if (key_type.type() == logical_type::NA) {
		key_type = key_target;
	}
	if (value_type.type() == logical_type::NA) {
		value_type = value_target;
	}

	return logical_value_t::create_map(key_type, value_type, key_values, value_values);

}

logical_value_t TransformDictionaryToMap(const PyDictionary &dict, const complex_logical_type &target_type = logical_type::UNKNOWN) {
	if (target_type.type() != logical_type::UNKNOWN && !DictionaryHasMapFormat(dict)) {
		// dict == { 'k1': v1, 'k2': v2, ..., 'kn': vn }
		return TransformStructFormatDictionaryToMap(dict, target_type);
	}

	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);

	if (py::none().is(keys) || py::none().is(values)) {
        // todo should create logical_value with field is_null=true, but the field was removed
		// Either 'key' or 'value' is None, return early with a NULL value
		//  return logical_value_t(std::move(complex_logical_type::create_map(logical_type::NA, logical_type::NA)));
        return EmptyMapValue();
	}

	auto key_size = py::len(keys);
	assert(key_size == py::len(values));
	if (key_size == 0) {
		// dict == { 'key': [], 'value': [] }
		return EmptyMapValue();
	}

	// dict == { 'key': [ ... ], 'value' : [ ... ] }
	complex_logical_type key_target = logical_type::UNKNOWN;
	complex_logical_type value_target = logical_type::UNKNOWN;

	if (target_type.type() != logical_type::UNKNOWN) {
        auto* map_extention = static_cast<components::types::map_logical_type_extention*>(target_type.extention()); 
	    key_target = complex_logical_type::create_list(map_extention->key());
	    value_target = complex_logical_type::create_list(map_extention->value());
	}

	auto key_list = TransformPythonValue(keys, key_target);
	auto value_list = TransformPythonValue(values, value_target);

	logical_type key_type = logical_type::NA;
	logical_type value_type = logical_type::NA;

    const auto& key_children = key_list.children();
    const auto& value_children = value_list.children();

    vector<logical_value_t> key_values;
    vector<logical_value_t> value_values;

	for (idx_t i = 0; i < key_size; i++) {

		logical_value_t new_key = key_children[i];
		logical_value_t new_value = value_children[i];

        //todo common type
		//key_type = logical_type::ForceMaxLogicalType(key_type, new_key.type());
		//value_type = logical_type::ForceMaxLogicalType(value_type, new_value.type());

        new_key.set_alias("key");
        new_value.set_alias("value");
        key_values.emplace_back(new_key);
        value_values.emplace_back(new_value);
    }

	return logical_value_t::create_map(key_type, value_type, key_values, value_values);
}

logical_value_t TransformTupleToStruct(py::handle ele, const complex_logical_type &target_type = logical_type::UNKNOWN) {
	auto tuple = py::cast<py::tuple>(ele);
	auto size = py::len(tuple);

	assert(target_type.type() == logical_type::STRUCT);
	auto child_types = target_type.child_types(); 
	auto child_count = child_types.size();
	if (size != child_count) {
		throw std::runtime_error("Tried to create a STRUCT value from a tuple containing "+std::to_string(size)+" elements, but the "+
		                            "STRUCT consists of "+std::to_string(child_count)+" children");
	}
    vector<logical_value_t> children;
	for (idx_t i = 0; i < child_count; i++) {
		auto &type = child_types[i];
		const auto &name = target_type.child_types()[i].alias(); 
		auto element = py::handle(tuple[i]);
		auto converted_value = TransformPythonValue(element, type);
        converted_value.set_alias(name);
		children.emplace_back(std::move(converted_value));
	}
	auto result = logical_value_t::create_struct(std::move(children));
	return result;
}

logical_value_t TransformListValue(py::handle ele, const complex_logical_type &target_type = logical_type::UNKNOWN) {
	auto size = py::len(ele);

	vector<logical_value_t> values;
	values.reserve(size);

	bool list_target = target_type.type() == logical_type::LIST;

	complex_logical_type element_type = logical_type::NA;
	for (idx_t i = 0; i < size; i++) {
		const auto &child_type = list_target ? target_type.child_type() : logical_type::UNKNOWN;
		logical_value_t new_value = TransformPythonValue(ele.attr("__getitem__")(i), child_type);
        //todo common type
		//element_type = logical_type::ForceMaxLogicalType(element_type, new_value.type());
		values.push_back(std::move(new_value));
        
        // todo remove if ForceMaxLogicalType exists
        element_type = child_type;
	}
	return logical_value_t::create_list(element_type, values);
}

logical_value_t TransformArrayValue(py::handle ele, const complex_logical_type &target_type = logical_type::UNKNOWN) {
	auto size = py::len(ele);

	vector<logical_value_t> values;
	values.reserve(size);

	bool array_target = target_type.type() == logical_type::ARRAY;
	const auto &child_type = array_target ? target_type.child_type() : logical_type::UNKNOWN;

	complex_logical_type element_type = logical_type::NA;
	for (idx_t i = 0; i < size; i++) {
		logical_value_t new_value = TransformPythonValue(ele.attr("__getitem__")(i), child_type);
        // todo common type
		//element_type = logical_type::ForceMaxLogicalType(element_type, new_value.type());
		values.push_back(std::move(new_value));
        
        // todo remove if ForceMaxLogicalType exists
        element_type = child_type;
	}

	return logical_value_t::create_array(element_type, std::move(values));
}

logical_value_t TransformDictionary(const PyDictionary &dict) {
	//! DICT -> MAP FORMAT
	// keys() = [key, value]
	// values() = [ [n keys] ], [ [n values] ]

	//! DICT -> STRUCT FORMAT
	// keys() = ['a', .., 'n']
	// values() = [ val1, .., valn]
	if (dict.len == 0) {
		// dict == {}
		return EmptyMapValue();
	}

	if (DictionaryHasMapFormat(dict)) {
		return TransformDictionaryToMap(dict);
	}
	return TransformDictionaryToStruct(dict);
}

bool TryTransformPythonIntegerToDouble(logical_value_t &res, py::handle ele) {
	double number = PyLong_AsDouble(ele.ptr());
	if (number == -1.0 && PyErr_Occurred()) {
		PyErr_Clear();
		return false;
	}
	res = logical_value_t(number);
	return true;
}

void TransformPythonUnsigned(uint64_t value, logical_value_t &res) {
	if (value > (uint64_t)std::numeric_limits<uint32_t>::max()) {
		res = logical_value_t(value);
	} else if (value > (int64_t)std::numeric_limits<uint16_t>::max()) {
		res = logical_value_t((uint32_t)value);
	} else if (value > (int64_t)std::numeric_limits<uint16_t>::max()) {
		res = logical_value_t((uint16_t)value);
	} else {
		res = logical_value_t((uint8_t)value);
	}
}

bool TrySniffPythonNumeric(logical_value_t &res, int64_t value) {
	if (value < (int64_t)std::numeric_limits<int32_t>::min() || value > (int64_t)std::numeric_limits<int32_t>::max()) {
		res = logical_value_t(value);
	} else {
		// To match default otterbrix behavior, numeric values without a specified type should not become a smaller type
		// than INT32
		res = logical_value_t((int32_t)value);
	}
	return true;
}


// TODO: add support for HUGEINT
bool TryTransformPythonNumeric(logical_value_t &res, py::handle ele, const complex_logical_type &target_type) {
	auto ptr = ele.ptr();

	auto numeric_string = string(py::str(ele));
	int overflow;
	int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
	if (overflow == -1) {
		PyErr_Clear();
		if (target_type.type() == logical_type::BIGINT) {
			throw std::runtime_error("Failed to cast value: Python value "+numeric_string+" to INT64");
		}
		
		switch (target_type.type()) {
			case logical_type::TINYINT:
				res = logical_value_t(int8_t(value));
				return true;
			case logical_type::UTINYINT:
				res = logical_value_t(uint8_t(value));
				return true;
			case logical_type::SMALLINT:
				res = logical_value_t(int16_t(value));
				return true;
			case logical_type::USMALLINT:
				res = logical_value_t(uint16_t(value));
				return true;
			case logical_type::INTEGER:
				res = logical_value_t(int32_t(value));
				return true;
			case logical_type::UINTEGER:
				res = logical_value_t(uint32_t(value));
				return true;
			case logical_type::BIGINT:
				res = logical_value_t(int64_t(value));
				return true;
			case logical_type::UBIGINT:
				res = logical_value_t(uint64_t(value));
				return true;
			case logical_type::UHUGEINT:
				// res = logical_value_t(ParseToNumeric<absl::uint128>(numeric_string));
				throw std::runtime_error("OtterBrix has no a logical_value_t constructor for hugeint/uhugeint");
				return true;
			case logical_type::HUGEINT:
			default:
				// res = logical_value_t(ParseToNumeric<absl::uint128>(numeric_string));
				throw std::runtime_error("OtterBrix has no a logical_value_t constructor for hugeint/uhugeint");

				return true;

		}
		return true;
	} else if (overflow == 1) {
		if (target_type.to_physical_type() == physical_type::INT64) {
			throw std::runtime_error("Failed to cast value: Python value "+numeric_string+" to INT64");
		}
		uint64_t unsigned_value = PyLong_AsUnsignedLongLong(ptr);
		if (PyErr_Occurred()) {
			PyErr_Clear();
			return TryTransformPythonIntegerToDouble(res, ele);
		} else {
			TransformPythonUnsigned(unsigned_value, res);
		}
		PyErr_Clear();
		return true;
	} else if (value == -1 && PyErr_Occurred()) {
		return false;
	}

	// The value is int64_t or smaller

	switch (target_type.type()) {
    case logical_type::UNKNOWN:
		return TrySniffPythonNumeric(res, value);
	case logical_type::HUGEINT: {
		// res = logical_value_t(ParseToNumeric<absl::int128>(numeric_string));
		throw std::runtime_error("OtterBrix has no a logical_value_t constructor for hugeint/uhugeint");

		return true;
	}
	case logical_type::UHUGEINT: {
		if (value < 0) {
			return false;
		}
		// res = logical_value_t(ParseToNumeric<absl::uint128>(numeric_string));
		throw std::runtime_error("OtterBrix has no a logical_value_t constructor for hugeint/uhugeint");

		return true;
	}
	case logical_type::BIGINT: {
		res = logical_value_t(int64_t(value));
		return true;
	}
	case logical_type::INTEGER: {
		if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max()) {
			return false;
		}
		res = logical_value_t(int32_t(value));
		return true;
	}
	case logical_type::SMALLINT: {
		if (value < std::numeric_limits<int16_t>::min() || value > std::numeric_limits<int16_t>::max()) {
			return false;
		}
		res = logical_value_t(int16_t(value));
		return true;
	}
	case logical_type::TINYINT: {
		if (value < std::numeric_limits<int8_t>::min() || value > std::numeric_limits<int8_t>::max()) {
			return false;
		}
		res = logical_value_t(int8_t(value));
		return true;
	}
	case logical_type::UBIGINT: {
		if (value < 0) {
			return false;
		}
		res = logical_value_t(uint64_t(value));
		return true;
	}
	case logical_type::UINTEGER: {
		if (value < 0 || value > (int64_t)std::numeric_limits<uint32_t>::max()) {
			return false;
		}
		res = logical_value_t(uint32_t(value));
		return true;
	}
	case logical_type::USMALLINT: {
		if (value < 0 || value > (int64_t)std::numeric_limits<uint16_t>::max()) {
			return false;
		}
		res = logical_value_t(uint16_t(value));
		return true;
	}
	case logical_type::UTINYINT: {
		if (value < 0 || value > (int64_t)std::numeric_limits<uint8_t>::max()) {
			return false;
		}
		res = logical_value_t(uint8_t(value));
		return true;
	}
	default: {
		if (!TrySniffPythonNumeric(res, value)) {
			return false;
		}
		// res = logical_value_t(ParseToNumeric<absl::int128>(numeric_string));
		throw std::runtime_error("OtterBrix has no a logical_value_t constructor for hugeint/uhugeint");
		return true;
	}
	}
}

PythonObjectType GetPythonObjectType(py::handle &ele) {
	auto &import_cache = ConnectionEnvironment::ImportCache();

	if (ele.is_none()) {
		return PythonObjectType::None;
	} else if (ele.is(import_cache.pandas.NaT())) {
		return PythonObjectType::None;
	} else if (ele.is(import_cache.pandas.NA())) {
		return PythonObjectType::None;
	} else if (py::isinstance<py::bool_>(ele)) {
		return PythonObjectType::Bool;
	} else if (py::isinstance<py::int_>(ele)) {
		return PythonObjectType::Integer;
	} else if (py::isinstance<py::float_>(ele)) {
		return PythonObjectType::Float;
	} else if (py::isinstance(ele, import_cache.decimal.Decimal())) {
		return PythonObjectType::Decimal;
	} else if (py::isinstance(ele, import_cache.uuid.UUID())) {
		return PythonObjectType::Uuid;
	} else if (py::isinstance(ele, import_cache.datetime.datetime())) {
		return PythonObjectType::Datetime;
	} else if (py::isinstance(ele, import_cache.datetime.time())) {
		return PythonObjectType::Time;
	} else if (py::isinstance(ele, import_cache.datetime.date())) {
		return PythonObjectType::Date;
	} else if (py::isinstance(ele, import_cache.datetime.timedelta())) {
		return PythonObjectType::Timedelta;
	} else if (py::isinstance<py::str>(ele)) {
		return PythonObjectType::String;
	} else if (py::isinstance<py::bytearray>(ele)) {
		return PythonObjectType::ByteArray;
	} else if (py::isinstance<py::memoryview>(ele)) {
		return PythonObjectType::MemoryView;
	} else if (py::isinstance<py::bytes>(ele)) {
		return PythonObjectType::Bytes;
	} else if (py::isinstance<py::list>(ele)) {
		return PythonObjectType::List;
	} else if (py::isinstance<py::tuple>(ele)) {
		return PythonObjectType::Tuple;
	} else if (py::isinstance<py::dict>(ele)) {
		return PythonObjectType::Dict;
	} else if (ele.is(import_cache.numpy.ma.masked())) {
		return PythonObjectType::None;
	} else if (py::isinstance(ele, import_cache.numpy.ndarray())) {
		return PythonObjectType::NdArray;
	} else if (py::isinstance(ele, import_cache.numpy.datetime64())) {
		return PythonObjectType::NdDatetime;
	/*} else if (py::isinstance(ele, import_cache.otterbrix.Value())) {
		return PythonObjectType::Value;*/
	} else {
		return PythonObjectType::Other;
	}
}

logical_value_t TransformPythonValue(py::handle ele, const complex_logical_type &target_type, bool nan_as_null) {
	auto object_type = GetPythonObjectType(ele);

	switch (object_type) {
	case PythonObjectType::None:
		return logical_value_t();
	case PythonObjectType::Bool:
		return logical_value_t(ele.cast<bool>());
	case PythonObjectType::Integer: {
		logical_value_t integer;
		if (!TryTransformPythonNumeric(integer, ele, target_type)) {
			throw std::runtime_error("An error occurred attempting to convert a python integer");
		}
		return integer;
	}
	case PythonObjectType::Float:
		if (nan_as_null && std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return logical_value_t();
		}
		switch (target_type.type()) {
		case logical_type::UNKNOWN:
		case logical_type::DOUBLE: {
			return logical_value_t(ele.cast<double>());
		}
		case logical_type::FLOAT: {
			return logical_value_t(ele.cast<float>());
		}
		case logical_type::DECIMAL: {
			throw std::runtime_error("Can't losslessly convert from object of float to type ");//+target_type.ToString());
		}
		default:
			throw std::runtime_error("Could not convert 'float' to type ");//+target_type.ToString());
		}
	case PythonObjectType::Decimal: {
		PyDecimal decimal(ele);
		return decimal.to_logical_value();
	}
	case PythonObjectType::Uuid: {
		// auto string_val = py::str(ele).cast<string>();
		// return logical_value_t::Uuid(string_val);
		throw std::runtime_error("OtterBrix doens\'t support UUID");
	}
	case PythonObjectType::String: {
		auto stringified = ele.cast<string>();
		if (target_type.type() == logical_type::UNKNOWN) {
			return logical_value_t(stringified);
		}
		// return logical_value_t(stringified).cast_as(target_type);

		throw std::runtime_error("OtterBrix doens\'t support string conversation");

	}
	case PythonObjectType::ByteArray: {
		auto byte_array = ele;
		const_data_ptr_t bytes = const_data_ptr_cast(PyByteArray_AsString(byte_array.ptr())); // NOLINT
		// idx_t byte_length = PyUtil::PyByteArrayGetSize(byte_array);                           // NOLINT
		// return logical_value_t::Blob(bytes, byte_length);
		throw std::runtime_error("OtterBrix doens\'t support byte array conversation");

	}
	case PythonObjectType::MemoryView: {
		py::memoryview py_view = ele.cast<py::memoryview>();
		// Py_buffer *py_buf = PyUtil::PyMemoryViewGetBuffer(py_view); // NOLINT
		// return logical_value_t::Blob(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
		throw std::runtime_error("OtterBrix doens\'t support memory view conversation");

	}
	case PythonObjectType::Bytes: {
		const string &ele_string = ele.cast<string>();
		switch (target_type.type()) {
		case logical_type::UNKNOWN:
		case logical_type::BLOB:
			// return logical_value_t::Blob(const_data_ptr_t(ele_string.data()), ele_string.size());
			throw std::runtime_error("OtterBrix doens\'t support blob conversation");

		case logical_type::BIT: {
			// return logical_value_t::Bit(ele_string);
			throw std::runtime_error("OtterBrix doens\'t support blob conversation");

		default:
			throw std::runtime_error("Could not convert 'bytes' to type"); //%s", target_type.ToString());
		}
		}
	}
	case PythonObjectType::List:
		if (target_type.type() == logical_type::ARRAY) {
			return TransformArrayValue(ele, target_type);
		} else {
			return TransformListValue(ele, target_type);
		}
	case PythonObjectType::Dict: {
		PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
		switch (target_type.type()) {
		case logical_type::STRUCT:
			return TransformDictionaryToStruct(dict, target_type);
		case logical_type::MAP:
			return TransformDictionaryToMap(dict, target_type);
		default:
			return TransformDictionary(dict);
		}
	}
	case PythonObjectType::Tuple: {
		switch (target_type.type()) {
		case logical_type::STRUCT:
			return TransformTupleToStruct(ele, target_type);
		case logical_type::UNKNOWN:
		case logical_type::LIST:
			return TransformListValue(ele, target_type);
		case logical_type::ARRAY:
			return TransformArrayValue(ele, target_type);
		default:
			throw std::runtime_error("Can't convert tuple to a Value of type ");//+target_type.ToString());
		}
	}
	case PythonObjectType::NdArray:
	case PythonObjectType::NdDatetime:
		return TransformPythonValue(ele.attr("tolist")(), target_type, nan_as_null);
	case PythonObjectType::Other:
		throw std::runtime_error("No implementation: Unable to transform python value of type " +
                py::str(ele.get_type()).cast<string>() +
                " to OtterBrix logical_type");
	default:
		throw std::runtime_error("Object type recognized but not implemented!");
	}
}

} // namespace otterbrix
