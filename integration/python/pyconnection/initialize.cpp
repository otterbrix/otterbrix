#include "pyconnection.hpp"
#include <memory>

#include <otterbrix_wrapper/pyrelation.hpp>

namespace otterbrix {

    void InitializeConnectionMethods(py::class_<PyConnection, std::shared_ptr<PyConnection>> &m) {
        m.def("cursor", &PyConnection::Cursor, "Create a duplicate of the current connection");
        // PyConnection::Execute is overloaded (py::object query / logical_plan node);
        // bind the Python-facing string-query overload explicitly.
        m.def("execute",
                static_cast<pycursor_ptr (PyConnection::*)(const py::object&)>(&PyConnection::Execute),
                "Execute the given SQL statement",
                py::arg("query"));
        m.def("sql", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "");
        m.def("query", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "");
        m.def("from_query", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "");

        m.def("from_df", &PyConnection::FromDF,
                "Create a relation object from the DataFrame in df", py::arg("df"));
        m.def("from_object", &PyConnection::FromObject,
                "Create a relation object from the object in obj", py::arg("obj"));
        m.def("close", &PyConnection::Close, "Close the connection");
    }

    void PyConnection::Initialize(py::handle& m) {
        auto connection_module = py::class_<PyConnection, std::shared_ptr<PyConnection>>(
                m, "OtterBrixPyConnection", py::module_local());
        
        connection_module
                .def("listTables", &PyConnection::ListTables);
    
        connection_module
            .def("__enter__", &PyConnection::Enter)
            .def("__exit__", &PyConnection::Exit,
                    py::arg("exc_type"), py::arg("exc"), py::arg("traceback"))
            .def("__del__", &PyConnection::Close);
        InitializeConnectionMethods(connection_module);
    }
} // namespace otterbrix
