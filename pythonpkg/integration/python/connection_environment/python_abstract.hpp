#pragma once

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix::abc {

    bool is_list_like(py::handle obj);

    bool is_dict_like(py::handle obj);

} // namespace otterbrix::abc
