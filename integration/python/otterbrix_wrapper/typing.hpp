#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <module_arena.hpp>

namespace otterbrix {

    class otterbrix_py_typing_t {
    public:
        otterbrix_py_typing_t() = delete;

    public:
        // Carries the module's arena (main.cpp's PYBIND11_MODULE body) through to
        // otterbrix_py_type_t::initialize; this submodule adds nothing of its own.
        static void initialize(py::module_& m, const module_arena_ptr& arena);
    };

} // namespace otterbrix
