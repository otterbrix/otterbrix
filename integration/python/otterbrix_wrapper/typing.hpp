#pragma once                   

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {             

    class OtterBrixPyTyping {         
    public:                        
        OtterBrixPyTyping() = delete; 
    
    public:
        static void Initialize(py::module_ &m);
    };
  
} // namespace otterbrix

