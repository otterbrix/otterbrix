#include <iostream>
#include <otterbrix_wrapper/pytype.hpp>
#include <otterbrix_wrapper/type_creation.hpp>
#include <pybind11/pybind_wrapper.hpp>

using namespace otterbrix;

int main(int argc, char *argv[]) {
    std::cout << "Hello, World!\n";
    OtterBrixPyType lt(components::types::logical_type::INTEGER);
    py::dict d =py::dict(py::make_tuple("inner", lt));

    auto struct_val = TypeCreation::StructType(d);
    
    return 0;
}
