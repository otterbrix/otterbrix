#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {

    class otterbrix_py_typing_t {
    public:
        otterbrix_py_typing_t() = delete;

    public:
        static void initialize(py::module_& m);
    };

} // namespace otterbrix
