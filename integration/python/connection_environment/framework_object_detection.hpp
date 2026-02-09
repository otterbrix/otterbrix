#pragma once

#include <pybind11/pybind_wrapper.hpp>
//#include <arrow/arrow_array_stream.hpp>
#include <numpy/numpy_type.hpp>

namespace otterbrix {

    class FrameworkObjectDetection {
    public:
//        static PyArrowObjectType GetArrowType(const py::handle &object);
        static NumpyObjectType GetNumpyObjectType(const py::object &object);
        static bool IsPandasDataframe(const py::object &object);
    };

} // namespace otterbrix
