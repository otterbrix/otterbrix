#pragma once

#include "python_objects.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
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

    bool dictionary_has_map_format(const py_dictionary_t& dict);
    core::result_wrapper_t<components::vector::vector_t> transform_python_value(std::pmr::memory_resource* resource,
                                                                                py::handle ele);

} // namespace otterbrix
