#pragma once

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {

    template <typename T>
    static bool ModuleIsLoaded() {
	    auto dict = pybind11::module_::import("sys").attr("modules");
	    return dict.contains(py::str(T::Name));
    }


} // namespace otterbrix
