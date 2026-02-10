#include <pybind11/pybind11.h>

#include "sql/wrapper_client.hpp"
#include "sql/wrapper_cursor.hpp"

#include "sql/convert.hpp"
#include "sql/spaces.hpp"
#include "sql/wrapper_connection.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using namespace otterbrix;
using namespace core;

PYBIND11_MODULE(otterbrix, m) {
    py::class_<wrapper_client>(m, "Client")
        .def(py::init([]() { return new wrapper_client(spaces::get_instance()); }))
        .def(py::init([](const py::str& s) { return new wrapper_client(spaces::get_instance(std::string(s))); }))
        .def("execute", &wrapper_client::execute, py::arg("query"));

    py::class_<wrapper_connection>(m, "Connection")
        .def(py::init([](wrapper_client* client) { return new wrapper_connection(client); }))
        .def("execute", &wrapper_connection::execute, py::arg("query"))
        .def("cursor", &wrapper_connection::cursor)
        .def("close", &wrapper_connection::close)
        .def("commit", &wrapper_connection::commit)
        .def("rollback", &wrapper_connection::rollback);

    py::class_<wrapper_cursor, boost::intrusive_ptr<wrapper_cursor>>(m, "Cursor")
        .def("__repr__", &wrapper_cursor::print)
        .def("__del__", &wrapper_cursor::close)
        .def("__len__", &wrapper_cursor::size)
        .def("__getitem__", &wrapper_cursor::get)
        .def("__iter__", &wrapper_cursor::iter)
        .def("__next__", &wrapper_cursor::next)
        .def("count", &wrapper_cursor::size)
        .def("close", &wrapper_cursor::close)
        .def("hasNext", &wrapper_cursor::has_next)
        .def("next", &wrapper_cursor::next)
        .def("is_success", &wrapper_cursor::is_success)
        .def("is_error", &wrapper_cursor::is_error)
        .def("get_error", &wrapper_cursor::get_error)
        .def("sort", &wrapper_cursor::sort, py::arg("key_or_list"), py::arg("direction") = py::none())
        .def("execute", &wrapper_cursor::execute, py::arg("querry"))
        .def("fetchone", &wrapper_cursor::fetchone)
        .def("fetchmany", &wrapper_cursor::fetchmany, py::arg("size") = 1)
        .def("fetchall", &wrapper_cursor::fetchall)
        .def_property_readonly("description", &wrapper_cursor::description)
        .def_property_readonly("rowcount", &wrapper_cursor::rowcount);

    m.def(
        "connect",
        [](const py::str& dsn) {
            auto client = new wrapper_client(spaces::get_instance(std::string(dsn)));
            return new wrapper_connection(client);
        },
        py::arg("dsn") = "");

    m.def("to_aggregate", &test_to_statement);
}