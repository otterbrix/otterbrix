#include "typing.hpp"
#include "pytype.hpp"

using namespace components::types;

namespace otterbrix {

    void otterbrix_py_typing_t::initialize(py::module_& parent, const module_arena_ptr& arena) {
        auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
        otterbrix_py_type_t::initialize(m, arena);
    }

} // namespace otterbrix
