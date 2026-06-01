#include "typing.hpp"
#include "pytype.hpp"

using namespace components::types;

namespace otterbrix {             

void OtterBrixPyTyping::Initialize(py::module_ &parent) {
    auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
    OtterBrixPyType::Initialize(m);
}
        
} // namespace otterbrix     
