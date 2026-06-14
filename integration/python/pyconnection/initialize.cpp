#include "pyconnection.hpp"
#include <memory>

#include <otterbrix_wrapper/pyrelation.hpp>

namespace otterbrix {

    void initialize_connection_methods(py::class_<py_connection_t, std::shared_ptr<py_connection_t>> &m) {
        m.def("cursor", &py_connection_t::cursor, "Create a duplicate of the current connection");
        // py_connection_t::execute is overloaded (py::object query / logical_plan node);
        // bind the Python-facing string-query overload explicitly.
        m.def("execute",
                static_cast<pycursor_ptr (py_connection_t::*)(const py::object&)>(&py_connection_t::execute),
                "Execute the given SQL statement",
                py::arg("query"));

        m.def("from_df", &py_connection_t::from_df,
                "Create a relation object from the DataFrame in df", py::arg("df"));
        m.def("from_object", &py_connection_t::from_object,
                "Create a relation object from the object in obj", py::arg("obj"));
        m.def("close", &py_connection_t::close, "Close the connection");
    }

    void py_connection_t::initialize(py::handle& m) {
        auto connection_module = py::class_<py_connection_t, std::shared_ptr<py_connection_t>>(
                m, "OtterBrixPyConnection", py::module_local());
        
        connection_module
                .def("listTables", &py_connection_t::list_tables);
    
        connection_module
            .def("__enter__", &py_connection_t::enter)
            .def("__exit__", &py_connection_t::exit,
                    py::arg("exc_type"), py::arg("exc"), py::arg("traceback"))
            .def("__del__", &py_connection_t::close);
        initialize_connection_methods(connection_module);
    }
} // namespace otterbrix
