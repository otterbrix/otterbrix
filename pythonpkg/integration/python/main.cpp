#include <common/exceptions.hpp>
#include <pyconnection/pyconnection.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>

#ifndef OTTERBRIX_PYTHON_LIB_NAME
#define OTTERBRIX_PYTHON_LIB_NAME otterbrix
#endif

using namespace otterbrix;

PYBIND11_MODULE(OTTERBRIX_PYTHON_LIB_NAME, m) {
    
    RegisterExceptions(m);

    PyRelation::Initialize(m);
    PyConnection::Initialize(m);
    m.def("connect", &PyConnection::Connect, 
            "Create a DuckDB database instance. Can take a database file name to read/write persistent data and a "
            "read_only flag if no changes are desired",
            py::arg("database") = "default", py::arg("read_only") = false, py::arg_v("config", py::dict(), "None"));

    // https://pybind11.readthedocs.io/en/stable/advanced/misc.html#module-destructors
    auto clean_default_connection = []() {
        PyConnection::Cleanup();
    };
    m.add_object("_clean_default_connection", py::capsule(clean_default_connection)); 
}
