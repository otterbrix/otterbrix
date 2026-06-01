#include "pyconnection.hpp"

#include <otterbrix_wrapper/pyrelation.hpp>

namespace otterbrix {

    void InitializeConnectionMethods(py::class_<PyConnection, shared_ptr<PyConnection>> &m) {
        m.def("cursor", &PyConnection::Cursor, "Create a duplicate of the current connection");
        m.def("execute", &PyConnection::Execute,
                "Execute the given prepared statement multiple times "
                "using the list of parameter sets in parameters",
                py::arg("query"),
                py::arg("parameters") = py::none());
        m.def("sql", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none());
        m.def("query", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none());
        m.def("from_query", &PyConnection::RunQuery,
                "Run a SQL query. If it is a SELECT statement, create a relation object from the given SQL query, otherwise "
                "run the query as-is.",
                py::arg("query"), py::kw_only(), py::arg("alias") = "", py::arg("params") = py::none());

        m.def("from_df", &PyConnection::FromDF, 
                "Create a relation object from the DataFrame in df", py::arg("df"));
        m.def("from_object", &PyConnection::FromObject, 
                "Create a relation object from the object in obj", py::arg("obj"));
        m.def("close", &PyConnection::Close, "Close the connection");
        // m.def("fetchone", &PyConnection::FetchOne, 
        //         "Fetch a single row from a result following execute");

        m.def("begin", &PyConnection::Begin, "Start a new transaction");
        m.def("commit", &PyConnection::Commit, 
                "Commit changes performed within a transaction");
        m.def("rollback", &PyConnection::Rollback, 
                "Roll back changes performed within a transaction");
        m.def("checkpoint", &PyConnection::Checkpoint,
	      "Synchronizes data in the write-ahead log (WAL) to the database data file "
          "(no-op for in-memory connections)");
            
    }

    void PyConnection::Initialize(py::handle& m) {
        auto connection_module = py::class_<PyConnection, shared_ptr<PyConnection>>(
                m, "OtterBrixPyConnection", py::module_local());
        
        connection_module
                .def("listTables", &PyConnection::ListTables);
    
        connection_module
            .def("__enter__", &PyConnection::Enter)
            .def("__exit__", &PyConnection::Exit, 
                    py::arg("exc_type"), py::arg("exc"), py::arg("traceback"))
            .def("__del__", &PyConnection::Close);
            //.def("test", &PyConnection::test, py::arg("test"));
        InitializeConnectionMethods(connection_module);

        //PyDataTime_IMPORT;
        //PyConnection::ImportCache();
    }
} // namespace otterbrix
