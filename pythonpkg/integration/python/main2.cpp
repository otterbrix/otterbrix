#include <pybind11/pybind_wrapper.hpp>

#include <otterbrix_wrapper/pyexpression.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/typing.hpp>
#include <otterbrix_wrapper/pytype.hpp>
#include <otterbrix_wrapper/type_creation.hpp>
#include <pyconnection/pyconnection.hpp>

#ifndef OTTERBRIX_PYTHON_LIB_NAME
#define OTTERBRIX_PYTHON_LIB_NAME otterbrix
#endif

using namespace otterbrix;

int add(int i, int j) {
    components::types::complex_logical_type lt(components::types::logical_type::INTEGER);
    return i + j + lt.size();
}


PYBIND11_MODULE(OTTERBRIX_PYTHON_LIB_NAME, m) {
    m.def("add", &add, "test"); 
    OtterBrixPyTyping::Initialize(m);
    TypeCreation::Initialize(m);
    PyExpression::Initialize(m);
    PyRelation::Initialize(m);
    PyConnection::Initialize(m);
    m.def("connect", &PyConnection::Connect, 
            "Create a OtterBrix database instance. Can take a database file name to read/write persistent data and a "
            "read_only flag if no changes are desired",
            py::arg("database") = "default", py::arg("read_only") = false, py::arg_v("config", py::dict(), "None"));

    // https://pybind11.readthedocs.io/en/stable/advanced/misc.html#module-destructors
    auto clean_default_connection = []() {
        PyConnection::Cleanup();
    };
    m.add_object("_clean_default_connection", py::capsule(clean_default_connection)); 


}
