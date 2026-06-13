#include "pandas_analyzer.hpp"

#include <numpy/numpy_type.hpp>
#include <native/python_conversion.hpp>
#include <native/python_objects.hpp>

#include <components/types/types.hpp>
#include <core/typedefs.hpp>
#include <core/types/vector.hpp>
#include <core/string_util/case_insensitive.hpp>

#include <stdexcept>

namespace otterbrix {
    using components::types::logical_type;
    using components::types::complex_logical_type;
    using components::types::map_logical_type_extension;
    
static bool SameTypeRealm(const complex_logical_type &a, const complex_logical_type &b) {
	auto a_id = a.type();
	auto b_id = b.type();
	if (a_id == b_id) {
		return true;
	}
	if (a_id > b_id) {
		return SameTypeRealm(b, a);
	}
	assert(a_id < b_id);

	// anything ANY and under can transform to anything
	if (a_id <= components::types::logical_type::ANY) {
		return true;
	}

	auto a_is_nested = a.is_nested();
	auto b_is_nested = b.is_nested();
	// Both a and b are not nested
	if (!a_is_nested && !b_is_nested) {
		return true;
	}
	// Non-nested -> Nested is not possible
	if (!a_is_nested || !b_is_nested) {
		return false;
	}

	// From this point on, left and right are both nested
	assert(a_id != b_id);
	// STRUCT -> LIST is not possible
	if (b_id == logical_type::LIST || a_id == logical_type::LIST) {
		return false;
	}
	return true;
}

static bool UpgradeType(complex_logical_type &left, const complex_logical_type &right);

static bool CheckTypeCompatibility(const complex_logical_type &left, const complex_logical_type &right) {
	if (!SameTypeRealm(left, right)) {
		return false;
	}
	if (!left.is_nested() || !right.is_nested()) {
		return true;
	}

	// Nested type IDs between left and right have to match
	if (left.type() != right.type()) {
		return false;
	}
	return true;
}

static bool IsStructColumnValid(const complex_logical_type &left, const complex_logical_type &right) {
	assert(left.type() == logical_type::STRUCT && left.type() == right.type());

	//! Child types of the two structs
	auto &left_children = left.child_types();
	auto &right_children = right.child_types();

	if (left_children.size() != right_children.size()) {
		return false;
	}
	//! Compare keys of struct case-insensitively
	auto compare = CaseInsensitiveStringEquality();
	for (idx_t i = 0; i < left_children.size(); i++) {
		auto &left_child = left_children[i];
		auto &right_child = right_children[i];

		// keys in left and right don't match
		if (!compare(left_child.alias(), right_child.alias())) {
			return false;
		}
		// Types are not compatible with each other
		if (!CheckTypeCompatibility(left_child, right_child)) {
			return false;
		}
	}
	return true;
}

static bool CombineStructTypes(complex_logical_type &result, const complex_logical_type &input) {
	assert(input.type() == logical_type::STRUCT);
	auto &children = input.child_types();
	for (auto &type : children) {
		if (!UpgradeType(result, type)) {
			return false;
		}
	}
	return true;
}

static bool SatisfiesMapConstraints(const complex_logical_type &left, const complex_logical_type &right, complex_logical_type &map_value_type) {
	assert(left.type() == logical_type::STRUCT && left.type() == right.type());

	if (!CombineStructTypes(map_value_type, left)) {
		return false;
	}
	if (!CombineStructTypes(map_value_type, right)) {
		return false;
	}
	return true;
}

static complex_logical_type ConvertStructToMap(complex_logical_type &map_value_type) {
	return complex_logical_type::create_map(logical_type::STRING_LITERAL, map_value_type);
}

// This is similar to ForceMaxLogicalType but we have custom rules around combining STRUCT types
// And because of that we have to avoid ForceMaxLogicalType for every nested type
static bool UpgradeType(complex_logical_type &left, const complex_logical_type &right) {
	if (left.type() == logical_type::NA) {
		// Early out for upgrading null
		left = right;
		return true;
	}

	if (left.is_nested() && right.type() == logical_type::NA) {
		return true;
	}

	switch (left.type()) {
    case logical_type::LIST: {
		if (right.type() != left.type()) {
			// Not both sides are LIST, not compatible
			return false;
		}
		complex_logical_type child_type = logical_type::NA;
		if (!UpgradeType(child_type, left.child_type())) {
			return false;
		}
		if (!UpgradeType(child_type, right.child_type())) {
			return false;
		}
		left = complex_logical_type::create_list(child_type);
		return true;
	}
    case logical_type::ARRAY: {
		throw std::runtime_error("ARRAY types are not being detected yet, this should never happen");
	}
    case logical_type::STRUCT: {
		if (right.type() == logical_type::STRUCT) {
			bool valid_struct = IsStructColumnValid(left, right);
			if (valid_struct) {
                std::pmr::vector<complex_logical_type> children(std::pmr::get_default_resource());
				auto child_count = right.size();
				assert(child_count == left.size());

                auto& left_children = left.child_types();
                auto& right_children = right.child_types();
				// Combine all types from left and right
				for (idx_t i = 0; i < child_count; i++) {
					auto &right_child = right_children[i];
					auto new_child = left_children[i];

					auto child_name = new_child.alias(); 
					if (!UpgradeType(new_child, right_child)) {
						return false;
					}
                    new_child.set_alias(child_name);
					children.push_back(new_child);
				}
				left = complex_logical_type::create_struct("struct", std::move(children));
			} else {
				complex_logical_type value_type = logical_type::NA;
				if (SatisfiesMapConstraints(left, right, value_type)) {
					// Combine all the child types together, becoming the value_type for the resulting MAP
					left = ConvertStructToMap(value_type);
				} else {
					return false;
				}
			}
		} else if (right.type() == logical_type::MAP) {
			// Left: STRUCT, Right: MAP
			// Combine all the child types of the STRUCT into the value type of the MAP
            auto value_type = 
                static_cast<map_logical_type_extension*>(right.extension())->value();
			if (!CombineStructTypes(value_type, left)) {
				return false;
			}
			left = complex_logical_type::create_map(logical_type::STRING_LITERAL, value_type);
		} else {
			return false;
		}
		return true;
	}
    case logical_type::UNION: {
		throw std::runtime_error("UNION types are not being detected yet, this should never happen");
	}
    case logical_type::MAP: {
        auto left_map_extension =
            static_cast<map_logical_type_extension*>(left.extension());
		if (right.type() == logical_type::MAP) {
            auto right_map_extension =
                static_cast<map_logical_type_extension*>(right.extension());
			// Key Type
			complex_logical_type key_type = logical_type::NA;
			if (!UpgradeType(key_type, left_map_extension->key())) {
				return false;
			}
			if (!UpgradeType(key_type, right_map_extension->key())) {
				return false;
			}

			// Value Type
			complex_logical_type value_type = logical_type::NA;
			if (!UpgradeType(value_type, left_map_extension->value())) {
				return false;
			}
			if (!UpgradeType(value_type, right_map_extension->value())) {
				return false;
			}
			left = complex_logical_type::create_map(key_type, value_type);
		} else if (right.type() == logical_type::STRUCT) {
			auto value_type = left_map_extension->value();
			if (!CombineStructTypes(value_type, right)) {
				return false;
			}
			left = complex_logical_type::create_map(logical_type::STRING_LITERAL, value_type);
		} else {
			return false;
		}
		return true;
	}
	default: {
		if (!CheckTypeCompatibility(left, right)) {
			return false;
		}
		return true;
	}
	}
}

complex_logical_type PandasAnalyzer::GetListType(py::object &ele, bool &can_convert) {
	auto size = py::len(ele);

	if (size == 0) {
		return logical_type::NA;
	}

	idx_t i = 0;
	complex_logical_type list_type = logical_type::NA;
	for (auto py_val : ele) {
		auto object = py::reinterpret_borrow<py::object>(py_val);
		auto item_type = GetItemType(object, can_convert);
		if (!i) {
			list_type = item_type;
		} else {
			if (!UpgradeType(list_type, item_type)) {
				can_convert = false;
			}
		}
		if (!can_convert) {
			break;
		}
		i++;
	}
	return list_type;
}

static complex_logical_type EmptyMap() {
	return complex_logical_type::create_map(
            logical_type::NA, logical_type::NA);
}

//! Check if the keys match
static bool StructKeysAreEqual(const std::pmr::vector<complex_logical_type> &reference,
                               const std::pmr::vector<complex_logical_type> &compare) {
	assert(reference.size() == compare.size());
	for (idx_t i = 0; i < reference.size(); i++) {
		auto &ref = reference[i].alias();
		auto &comp = compare[i].alias();
		if (!otterbrix::CaseInsensitiveStringEquality()(ref, comp)) {
			return false;
		}
	}
	return true;
}

// Verify that all struct entries in a column have the same amount of fields and that keys are equal
static bool VerifyStructValidity(vector<complex_logical_type> &structs) {
	assert(!structs.empty());
	idx_t reference_entry = 0;
	// Get first non-null entry
	for (; reference_entry < structs.size(); reference_entry++) {
		if (structs[reference_entry].type() != logical_type::NA) {
			break;
		}
	}
	// All entries are NULL
	if (reference_entry == structs.size()) {
		return true;
	}
	auto reference_type = structs[reference_entry];
	auto reference_children = reference_type.child_types();

	for (idx_t i = reference_entry + 1; i < structs.size(); i++) {
		auto &entry = structs[i];
		if (entry.type() == logical_type::NA) {
			continue;
		}
		auto &entry_children = entry.child_types(); 
		if (entry_children.size() != reference_children.size()) {
			return false;
		}
		if (!StructKeysAreEqual(reference_children, entry_children)) {
			return false;
		}
	}
	return true;
}

complex_logical_type PandasAnalyzer::DictToMap(const PyDictionary &dict, bool &can_convert) {
	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);

	if (py::none().is(keys) || py::none().is(values)) {
		return complex_logical_type::create_map(
                logical_type::NA, 
                logical_type::NA);
	}

	auto key_type = GetListType(keys, can_convert);
	if (!can_convert) {
		return EmptyMap();
	}
	auto value_type = GetListType(values, can_convert);
	if (!can_convert) {
		return EmptyMap();
	}

	return complex_logical_type::create_map(key_type, value_type);
}

//! Python dictionaries don't allow duplicate keys, so we don't need to check this.
complex_logical_type PandasAnalyzer::DictToStruct(const PyDictionary &dict, bool &can_convert) {
    std::pmr::vector<complex_logical_type> struct_children(std::pmr::get_default_resource());

	for (idx_t i = 0; i < dict.len; i++) {
		auto dict_key = dict.keys.attr("__getitem__")(i);

		//! Have to already transform here because the child_list needs a string as key
		auto key = string(py::str(dict_key));

		auto dict_val = dict.values.attr("__getitem__")(i);
		auto val = GetItemType(dict_val, can_convert);
        val.set_alias(key);
		struct_children.push_back(val);
	}
	return complex_logical_type::create_struct("struct", struct_children);
}

//! 'can_convert' is used to communicate if internal structures encountered here are valid
//! e.g python lists can consist of multiple different types, which we cant communicate downwards through
//! complex_logical_type's alone

complex_logical_type PandasAnalyzer::GetItemType(py::object ele, bool &can_convert) {
	auto object_type = GetPythonObjectType(ele);

	switch (object_type) {
	case PythonObjectType::None:
		return logical_type::NA;
	case PythonObjectType::Bool:
		return logical_type::BOOLEAN;
	case PythonObjectType::Integer: {
		components::types::logical_value_t integer(std::pmr::get_default_resource(), components::types::logical_type::UNKNOWN);
		if (!TryTransformPythonNumeric(integer, ele)) {
			can_convert = false;
			return logical_type::NA;
		}
		return integer.type();
	}
	case PythonObjectType::Float:
		if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return logical_type::NA;
		}
		return logical_type::DOUBLE;
	case PythonObjectType::Decimal: {
		PyDecimal decimal(ele);
		complex_logical_type type;
		if (!decimal.TryGetType(type)) {
			can_convert = false;
		}
		return type;
	}
	case PythonObjectType::String:
		return logical_type::STRING_LITERAL;
	case PythonObjectType::Uuid:
		return logical_type::UUID;
	case PythonObjectType::ByteArray:
	case PythonObjectType::MemoryView:
	case PythonObjectType::Bytes:
		return logical_type::BLOB;
	case PythonObjectType::Tuple:
	case PythonObjectType::List:
		return complex_logical_type::create_list(GetListType(ele, can_convert));
	case PythonObjectType::Dict: {
		PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
		// Assuming keys and values are the same size

		if (dict.len == 0) {
			return EmptyMap();
		}
		if (DictionaryHasMapFormat(dict)) {
			return DictToMap(dict, can_convert);
		}
		return DictToStruct(dict, can_convert);
	}
	case PythonObjectType::NdDatetime: {
		return GetItemType(ele.attr("tolist")(), can_convert);
	}
	case PythonObjectType::NdArray: {
		auto extended_type = ConvertNumpyType(ele.attr("dtype"));
		complex_logical_type ltype;
		ltype = NumpyToLogicalType(extended_type);
		if (extended_type.type == NumpyNullableType::OBJECT) {
			complex_logical_type converted_type = InnerAnalyze(ele, can_convert, 1);
			if (can_convert) {
				ltype = converted_type;
			}
		}
		return complex_logical_type::create_list(ltype);
	}
	case PythonObjectType::Other:
		// Fall back to string for unknown types
		can_convert = false;
		return logical_type::STRING_LITERAL;
	default:
		throw std::runtime_error("Unsupported PythonObjectType");
	}
}

//! Get the increment for the given sample size
uint64_t PandasAnalyzer::GetSampleIncrement(idx_t rows) {
	//! Apply the maximum
	auto sample = sample_size;
	if (sample > rows) {
		sample = rows;
	}
	if (sample == 0) {
		return rows;
	}
	return rows / sample;
}

complex_logical_type PandasAnalyzer::InnerAnalyze(py::object column, bool &can_convert, idx_t increment) {
	idx_t rows = py::len(column);

	if (rows == 0) {
		return logical_type::NA;
	}

	// Keys are not guaranteed to start at 0 for Series, use the internal __array__ instead
	auto pandas_module = py::module::import("pandas");
	auto pandas_series = pandas_module.attr("core").attr("series").attr("Series");

	if (py::isinstance(column, pandas_series)) {
		column = column.attr("to_numpy")();
	}
	auto row = column.attr("__getitem__");

	complex_logical_type item_type = logical_type::NA;
	vector<complex_logical_type> types;
	for (idx_t i = 0; i < rows; i += increment) {
		auto obj = row(i);
		auto next_item_type = GetItemType(obj, can_convert);
		types.push_back(next_item_type);

		if (!can_convert || !UpgradeType(item_type, next_item_type)) {
			can_convert = false;
			return next_item_type;
		}
	}

	if (can_convert && item_type.type() == logical_type::STRUCT) {
		can_convert = VerifyStructValidity(types);
	}

	return item_type;
}

bool PandasAnalyzer::Analyze(py::object column) {
	// Disable analyze
	if (sample_size == 0) {
		return false;
	}
	bool can_convert = true;
	idx_t increment = GetSampleIncrement(py::len(column));
	complex_logical_type type = InnerAnalyze(column, can_convert, increment);

	if (type == logical_type::NA && increment > 1) {
		// We did not see the whole dataset, hence we are not sure if nulls are really nulls
		// as a fallback we try to identify this specific type
		auto first_valid_index = column.attr("first_valid_index")();
		if (GetPythonObjectType(first_valid_index) != PythonObjectType::None) {
			// This means we do have a value that is not null, figure out its type
			auto row = column.attr("__getitem__");
			auto obj = row(first_valid_index);
			type = GetItemType(obj, can_convert);
		}
	}
	if (can_convert) {
		analyzed_type = type;
	}
	return can_convert;
}

} // namespace otterbrix
