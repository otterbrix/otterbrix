#pragma once

#include "python_objects.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace otterbrix {

    enum class python_object_type_t
    {
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

    python_object_type_t get_python_object_type(py::handle& ele);

    // Hot per-cell path: returns false when the python value cannot be represented as the requested
    // numeric type. Writes the result into `res` using res.resource(); callers surface a single error
    // per batch/column. (Kept bool-returning by design — see R2 hot-path carve-out.)
    bool try_transform_python_numeric(
        components::types::logical_value_t& res,
        py::handle ele,
        const components::types::complex_logical_type& target_type = components::types::logical_type::UNKNOWN);
    bool dictionary_has_map_format(const py_dictionary_t& dict);
    core::result_wrapper_t<components::types::logical_value_t> transform_python_value(
        std::pmr::memory_resource* resource,
        py::handle ele,
        const components::types::complex_logical_type& target_type = components::types::logical_type::UNKNOWN,
        bool nan_as_null = true);

} // namespace otterbrix
