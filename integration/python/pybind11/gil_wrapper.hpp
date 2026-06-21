#pragma once

#include "pybind_wrapper.hpp"

namespace otterbrix {

    struct python_gil_wrapper_t {
        py::gil_scoped_acquire acquire;
    };

} // namespace otterbrix
