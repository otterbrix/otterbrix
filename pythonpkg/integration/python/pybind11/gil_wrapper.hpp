#pragma once

#include "pybind_wrapper.hpp"

namespace otterbrix {

struct PythonGILWrapper {
	py::gil_scoped_acquire acquire;
};

} // namespace otterbrix
