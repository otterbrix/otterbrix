#include "pandas_analyzer.hpp"

#include <numpy/numpy_type.hpp>
#include <native/python_conversion.hpp>
#include <native/python_objects.hpp>

#include <components/types/types.hpp>
#include <common/typedefs.hpp>
#include <common/string_util/case_insensitive.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {
    using components::types::logical_type;
    using components::types::complex_logical_type;
    using components::types::map_logical_type_extension;
    
static bool same_type_realm(const complex_logical_type &a, const complex_logical_type &b) {
	auto a_id = a.type();
	auto b_id = b.type();
	if (a_id == b_id) {
		return true;
	}
	if (a_id > b_id) {
		return same_type_realm(b, a);
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

static core::result_wrapper_t<bool> upgrade_type(complex_logical_type &left, const complex_logical_type &right,
                                                std::pmr::memory_resource *resource);

static bool check_type_compatibility(const complex_logical_type &left, const complex_logical_type &right) {
	if (!same_type_realm(left, right)) {
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

static bool is_struct_column_valid(const complex_logical_type &left, const complex_logical_type &right) {
	assert(left.type() == logical_type::STRUCT && left.type() == right.type());

	//! Child types of the two structs
	auto &left_children = left.child_types();
	auto &right_children = right.child_types();

	if (left_children.size() != right_children.size()) {
		return false;
	}
	//! Compare keys of struct case-insensitively
	auto compare = case_insensitive_string_equality_t();
	for (idx_t i = 0; i < left_children.size(); i++) {
		auto &left_child = left_children[i];
		auto &right_child = right_children[i];

		// keys in left and right don't match
		if (!compare(left_child.alias(), right_child.alias())) {
			return false;
		}
		// Types are not compatible with each other
		if (!check_type_compatibility(left_child, right_child)) {
			return false;
		}
	}
	return true;
}

static core::result_wrapper_t<bool> combine_struct_types(complex_logical_type &result, const complex_logical_type &input,
                                                       std::pmr::memory_resource *resource) {
	assert(input.type() == logical_type::STRUCT);
	auto &children = input.child_types();
	for (auto &type : children) {
		auto upgraded = upgrade_type(result, type, resource);
		if (upgraded.has_error()) {
			return upgraded;
		}
		if (!upgraded.value()) {
			return false;
		}
	}
	return true;
}

static core::result_wrapper_t<bool> satisfies_map_constraints(const complex_logical_type &left,
                                                            const complex_logical_type &right,
                                                            complex_logical_type &map_value_type,
                                                            std::pmr::memory_resource *resource) {
	assert(left.type() == logical_type::STRUCT && left.type() == right.type());

	auto combined_left = combine_struct_types(map_value_type, left, resource);
	if (combined_left.has_error()) {
		return combined_left;
	}
	if (!combined_left.value()) {
		return false;
	}
	auto combined_right = combine_struct_types(map_value_type, right, resource);
	if (combined_right.has_error()) {
		return combined_right;
	}
	if (!combined_right.value()) {
		return false;
	}
	return true;
}

static complex_logical_type convert_struct_to_map(complex_logical_type &map_value_type) {
	return complex_logical_type::create_map(logical_type::STRING_LITERAL, map_value_type);
}

// This is similar to ForceMaxLogicalType but we have custom rules around combining STRUCT types
// and_ because of that we have to avoid ForceMaxLogicalType for every nested type
static core::result_wrapper_t<bool> upgrade_type(complex_logical_type &left, const complex_logical_type &right,
                                                std::pmr::memory_resource *resource) {
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
			// not_ both sides are LIST, not compatible
			return false;
		}
		complex_logical_type child_type = logical_type::NA;
		auto upgraded_left = upgrade_type(child_type, left.child_type(), resource);
		if (upgraded_left.has_error()) {
			return upgraded_left;
		}
		if (!upgraded_left.value()) {
			return false;
		}
		auto upgraded_right = upgrade_type(child_type, right.child_type(), resource);
		if (upgraded_right.has_error()) {
			return upgraded_right;
		}
		if (!upgraded_right.value()) {
			return false;
		}
		left = complex_logical_type::create_list(child_type);
		return true;
	}
    case logical_type::ARRAY: {
		return core::error_t(core::error_code_t::unimplemented_yet,
		                     std::pmr::string("ARRAY types are not being detected yet, this should never happen",
		                                      resource));
	}
    case logical_type::STRUCT: {
		if (right.type() == logical_type::STRUCT) {
			bool valid_struct = is_struct_column_valid(left, right);
			if (valid_struct) {
                std::pmr::vector<complex_logical_type> children(resource);
				auto child_count = right.size();
				assert(child_count == left.size());

                auto& left_children = left.child_types();
                auto& right_children = right.child_types();
				// Combine all types from left and right
				for (idx_t i = 0; i < child_count; i++) {
					auto &right_child = right_children[i];
					auto new_child = left_children[i];

					auto child_name = new_child.alias();
					auto upgraded = upgrade_type(new_child, right_child, resource);
					if (upgraded.has_error()) {
						return upgraded;
					}
					if (!upgraded.value()) {
						return false;
					}
                    new_child.set_alias(child_name);
					children.push_back(new_child);
				}
				left = complex_logical_type::create_struct("struct", std::move(children));
			} else {
				complex_logical_type value_type = logical_type::NA;
				auto satisfies = satisfies_map_constraints(left, right, value_type, resource);
				if (satisfies.has_error()) {
					return satisfies;
				}
				if (satisfies.value()) {
					// Combine all the child types together, becoming the value_type for the resulting MAP
					left = convert_struct_to_map(value_type);
				} else {
					return false;
				}
			}
		} else if (right.type() == logical_type::MAP) {
			// Left: STRUCT, Right: MAP
			// Combine all the child types of the STRUCT into the value type of the MAP
            auto value_type =
                static_cast<map_logical_type_extension*>(right.extension())->value();
			auto combined = combine_struct_types(value_type, left, resource);
			if (combined.has_error()) {
				return combined;
			}
			if (!combined.value()) {
				return false;
			}
			left = complex_logical_type::create_map(logical_type::STRING_LITERAL, value_type);
		} else {
			return false;
		}
		return true;
	}
    case logical_type::UNION: {
		return core::error_t(core::error_code_t::unimplemented_yet,
		                     std::pmr::string("UNION types are not being detected yet, this should never happen",
		                                      resource));
	}
    case logical_type::MAP: {
        auto left_map_extension =
            static_cast<map_logical_type_extension*>(left.extension());
		if (right.type() == logical_type::MAP) {
            auto right_map_extension =
                static_cast<map_logical_type_extension*>(right.extension());
			// Key type
			complex_logical_type key_type = logical_type::NA;
			auto key_left = upgrade_type(key_type, left_map_extension->key(), resource);
			if (key_left.has_error()) {
				return key_left;
			}
			if (!key_left.value()) {
				return false;
			}
			auto key_right = upgrade_type(key_type, right_map_extension->key(), resource);
			if (key_right.has_error()) {
				return key_right;
			}
			if (!key_right.value()) {
				return false;
			}

			// Value type
			complex_logical_type value_type = logical_type::NA;
			auto val_left = upgrade_type(value_type, left_map_extension->value(), resource);
			if (val_left.has_error()) {
				return val_left;
			}
			if (!val_left.value()) {
				return false;
			}
			auto val_right = upgrade_type(value_type, right_map_extension->value(), resource);
			if (val_right.has_error()) {
				return val_right;
			}
			if (!val_right.value()) {
				return false;
			}
			left = complex_logical_type::create_map(key_type, value_type);
		} else if (right.type() == logical_type::STRUCT) {
			auto value_type = left_map_extension->value();
			auto combined = combine_struct_types(value_type, right, resource);
			if (combined.has_error()) {
				return combined;
			}
			if (!combined.value()) {
				return false;
			}
			left = complex_logical_type::create_map(logical_type::STRING_LITERAL, value_type);
		} else {
			return false;
		}
		return true;
	}
	default: {
		if (!check_type_compatibility(left, right)) {
			return false;
		}
		return true;
	}
	}
}

core::result_wrapper_t<complex_logical_type> pandas_analyzer_t::get_list_type(py::object &ele, bool &can_convert) {
	auto size = py::len(ele);

	if (size == 0) {
		return complex_logical_type(logical_type::NA);
	}

	idx_t i = 0;
	complex_logical_type list_type = logical_type::NA;
	for (auto py_val : ele) {
		auto object = py::reinterpret_borrow<py::object>(py_val);
		auto item_type = get_item_type(object, can_convert);
		if (item_type.has_error()) {
			return item_type;
		}
		if (!i) {
			list_type = item_type.value();
		} else {
			auto upgraded = upgrade_type(list_type, item_type.value(), resource_);
			if (upgraded.has_error()) {
				return upgraded.convert_error<complex_logical_type>();
			}
			if (!upgraded.value()) {
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

static complex_logical_type empty_map() {
	return complex_logical_type::create_map(
            logical_type::NA, logical_type::NA);
}

//! Check if the keys match
static bool struct_keys_are_equal(const std::pmr::vector<complex_logical_type> &reference,
                               const std::pmr::vector<complex_logical_type> &compare) {
	assert(reference.size() == compare.size());
	for (idx_t i = 0; i < reference.size(); i++) {
		auto &ref = reference[i].alias();
		auto &comp = compare[i].alias();
		if (!otterbrix::case_insensitive_string_equality_t()(ref, comp)) {
			return false;
		}
	}
	return true;
}

// Verify that all struct entries in a column have the same amount of fields and that keys are equal
static bool verify_struct_validity(std::pmr::vector<complex_logical_type> &structs) {
	assert(!structs.empty());
	idx_t reference_entry = 0;
	// get first non-null entry
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
		if (!struct_keys_are_equal(reference_children, entry_children)) {
			return false;
		}
	}
	return true;
}

core::result_wrapper_t<complex_logical_type> pandas_analyzer_t::dict_to_map(const py_dictionary_t &dict, bool &can_convert) {
	auto keys = dict.values.attr("__getitem__")(0);
	auto values = dict.values.attr("__getitem__")(1);

	if (py::none().is(keys) || py::none().is(values)) {
		return complex_logical_type::create_map(
                logical_type::NA,
                logical_type::NA);
	}

	auto key_type = get_list_type(keys, can_convert);
	if (key_type.has_error()) {
		return key_type;
	}
	if (!can_convert) {
		return empty_map();
	}
	auto value_type = get_list_type(values, can_convert);
	if (value_type.has_error()) {
		return value_type;
	}
	if (!can_convert) {
		return empty_map();
	}

	return complex_logical_type::create_map(key_type.value(), value_type.value());
}

//! Python dictionaries don't allow duplicate keys, so we don't need to check this.
core::result_wrapper_t<complex_logical_type> pandas_analyzer_t::dict_to_struct(const py_dictionary_t &dict, bool &can_convert) {
    std::pmr::vector<complex_logical_type> struct_children(resource_);

	for (idx_t i = 0; i < dict.len; i++) {
		auto dict_key = dict.keys.attr("__getitem__")(i);

		//! Have to already transform here because the child_list needs a string as key
		auto key = std::string(py::str(dict_key));

		auto dict_val = dict.values.attr("__getitem__")(i);
		auto val = get_item_type(dict_val, can_convert);
		if (val.has_error()) {
			return val;
		}
		auto child = val.value();
        child.set_alias(key);
		struct_children.push_back(child);
	}
	return complex_logical_type::create_struct("struct", struct_children);
}

//! 'can_convert' is used to communicate if internal structures encountered here are valid
//! e.g python lists can consist of multiple different types, which we cant communicate downwards through
//! complex_logical_type's alone

core::result_wrapper_t<complex_logical_type> pandas_analyzer_t::get_item_type(py::object ele, bool &can_convert) {
	auto object_type = get_python_object_type(ele);

	switch (object_type) {
	case python_object_type_t::None:
		return complex_logical_type(logical_type::NA);
	case python_object_type_t::Bool:
		return complex_logical_type(logical_type::BOOLEAN);
	case python_object_type_t::Integer: {
		components::types::logical_value_t integer(resource_, components::types::logical_type::UNKNOWN);
		if (!try_transform_python_numeric(integer, ele)) {
			can_convert = false;
			return complex_logical_type(logical_type::NA);
		}
		return integer.type();
	}
	case python_object_type_t::Float:
		if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
			return complex_logical_type(logical_type::NA);
		}
		return complex_logical_type(logical_type::DOUBLE);
	case python_object_type_t::Decimal: {
		py_decimal_t decimal(ele);
		complex_logical_type type;
		if (!decimal.try_get_type(type)) {
			can_convert = false;
		}
		return type;
	}
	case python_object_type_t::String:
		return complex_logical_type(logical_type::STRING_LITERAL);
	case python_object_type_t::Uuid:
		return complex_logical_type(logical_type::UUID);
	case python_object_type_t::ByteArray:
	case python_object_type_t::MemoryView:
	case python_object_type_t::Bytes:
		return complex_logical_type(logical_type::BLOB);
	case python_object_type_t::Tuple:
	case python_object_type_t::List: {
		auto list_type = get_list_type(ele, can_convert);
		if (list_type.has_error()) {
			return list_type;
		}
		return complex_logical_type::create_list(list_type.value());
	}
	case python_object_type_t::Dict: {
		py_dictionary_t dict = py_dictionary_t(py::reinterpret_borrow<py::object>(ele));
		// Assuming keys and values are the same size

		if (dict.len == 0) {
			return empty_map();
		}
		if (dictionary_has_map_format(dict)) {
			return dict_to_map(dict, can_convert);
		}
		return dict_to_struct(dict, can_convert);
	}
	case python_object_type_t::NdDatetime: {
		return get_item_type(ele.attr("tolist")(), can_convert);
	}
	case python_object_type_t::NdArray: {
		auto extended_type = convert_numpy_type(resource_, ele.attr("dtype"));
		if (extended_type.has_error()) {
			return extended_type.convert_error<complex_logical_type>();
		}
		auto ltype_result = numpy_to_logical_type(resource_, extended_type.value());
		if (ltype_result.has_error()) {
			return ltype_result;
		}
		complex_logical_type ltype = ltype_result.value();
		if (extended_type.value().type == numpy_nullable_type_t::OBJECT) {
			auto converted_type = inner_analyze(ele, can_convert, 1);
			if (converted_type.has_error()) {
				return converted_type;
			}
			if (can_convert) {
				ltype = converted_type.value();
			}
		}
		return complex_logical_type::create_list(ltype);
	}
	case python_object_type_t::Other:
		// Fall back to string for unknown types
		can_convert = false;
		return complex_logical_type(logical_type::STRING_LITERAL);
	default:
		return core::error_t(core::error_code_t::conversion_failure,
		                     std::pmr::string("Unsupported PythonObjectType", resource_));
	}
}

//! get the increment for the given sample size
uint64_t pandas_analyzer_t::get_sample_increment(idx_t rows) {
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

core::result_wrapper_t<complex_logical_type> pandas_analyzer_t::inner_analyze(py::object column, bool &can_convert, idx_t increment) {
	idx_t rows = py::len(column);

	if (rows == 0) {
		return complex_logical_type(logical_type::NA);
	}

	// Keys are not guaranteed to start at 0 for Series, use the internal __array__ instead
	auto pandas_module = py::module::import("pandas");
	auto pandas_series = pandas_module.attr("core").attr("series").attr("Series");

	if (py::isinstance(column, pandas_series)) {
		column = column.attr("to_numpy")();
	}
	auto row = column.attr("__getitem__");

	complex_logical_type item_type = logical_type::NA;
	std::pmr::vector<complex_logical_type> types(resource_);
	for (idx_t i = 0; i < rows; i += increment) {
		auto obj = row(i);
		auto next_item_type = get_item_type(obj, can_convert);
		if (next_item_type.has_error()) {
			return next_item_type;
		}
		types.push_back(next_item_type.value());

		if (!can_convert) {
			return next_item_type;
		}
		auto upgraded = upgrade_type(item_type, next_item_type.value(), resource_);
		if (upgraded.has_error()) {
			return upgraded.convert_error<complex_logical_type>();
		}
		if (!upgraded.value()) {
			can_convert = false;
			return next_item_type;
		}
	}

	if (can_convert && item_type.type() == logical_type::STRUCT) {
		can_convert = verify_struct_validity(types);
	}

	return item_type;
}

core::result_wrapper_t<bool> pandas_analyzer_t::analyze(py::object column) {
	// Disable analyze
	if (sample_size == 0) {
		return false;
	}
	bool can_convert = true;
	idx_t increment = get_sample_increment(py::len(column));
	auto analyzed = inner_analyze(column, can_convert, increment);
	if (analyzed.has_error()) {
		return analyzed.convert_error<bool>();
	}
	complex_logical_type type = analyzed.value();

	if (type == logical_type::NA && increment > 1) {
		// We did not see the whole dataset, hence we are not sure if nulls are really nulls
		// as a fallback we try to identify this specific type
		auto first_valid_index = column.attr("first_valid_index")();
		if (get_python_object_type(first_valid_index) != python_object_type_t::None) {
			// This means we do have a value that is not null, figure out its type
			auto row = column.attr("__getitem__");
			auto obj = row(first_valid_index);
			auto item = get_item_type(obj, can_convert);
			if (item.has_error()) {
				return item.convert_error<bool>();
			}
			type = item.value();
		}
	}
	if (can_convert) {
		analyzed_type_ = type;
	}
	return can_convert;
}

} // namespace otterbrix
