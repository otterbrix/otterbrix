#pragma once

#include "python_objects.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/types/logical_value.hpp>

namespace otterbrix {

enum class PythonObjectType {
	Other,
	None,
	Integer,
	Float,
	Bool,
	Decimal,
	Uuid,
	Datetime,
	Date,
	Time,
	Timedelta,
	String,
	ByteArray,
	MemoryView,
	Bytes,
	List,
	Tuple,
	Dict,
	NdArray,
	NdDatetime,
	Value
};

PythonObjectType GetPythonObjectType(py::handle &ele);

bool TryTransformPythonNumeric(components::types::logical_value_t &res, py::handle ele, 
        const components::types::complex_logical_type &target_type = components::types::logical_type::UNKNOWN);
bool DictionaryHasMapFormat(const PyDictionary &dict);
components::types::logical_value_t TransformPythonValue(py::handle ele, 
        const components::types::complex_logical_type &target_type = components::types::logical_type::UNKNOWN,
        bool nan_as_null = true);

} // namespace otterbrix
