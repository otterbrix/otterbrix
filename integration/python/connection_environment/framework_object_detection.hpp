#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <numpy/numpy_type.hpp>

namespace otterbrix {

    class framework_object_detection_t {
    public:
        static numpy_object_type_t get_numpy_object_type(const py::object &object);
        static bool is_pandas_dataframe(const py::object &object);
        static bool is_polars_dataframe(const py::object &object);
    };

} // namespace otterbrix
