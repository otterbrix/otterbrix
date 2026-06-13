#include <pybind11/pybind_wrapper.hpp>

#include "sql/convert.hpp"
#include "sql/spaces.hpp"
#include "sql/wrapper_client.hpp"
#include "sql/wrapper_connection.hpp"
#include "sql/wrapper_cursor.hpp"

#include <otterbrix_wrapper/pyexpression.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/typing.hpp>
#include <otterbrix_wrapper/pytype.hpp>
#include <otterbrix_wrapper/type_creation.hpp>
#include <pyconnection/pyconnection.hpp>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

#ifndef OTTERBRIX_PYTHON_LIB_NAME
#define OTTERBRIX_PYTHON_LIB_NAME otterbrix
#endif

using namespace otterbrix;

PYBIND11_MODULE(OTTERBRIX_PYTHON_LIB_NAME, m) {

    OtterBrixPyTyping::Initialize(m);
    TypeCreation::Initialize(m);
    PyExpression::Initialize(m);
    PyRelation::Initialize(m);
    PyConnection::Initialize(m);

    m.def("connect", &PyConnection::Connect,
          "Create a OtterBrix database instance. Can take a database file name to read/write persistent data and a "
          "read_only flag if no changes are desired",
          pybind11::arg("database") = "default", pybind11::arg("read_only") = false,
          pybind11::arg_v("config", pybind11::dict(), "None"));

    // https://pybind11.readthedocs.io/en/stable/advanced/misc.html#module-destructors
    auto clean_default_connection = []() {
        PyConnection::Cleanup();
    };
    m.add_object("_clean_default_connection", pybind11::capsule(clean_default_connection));

    pybind11::class_<wrapper_client>(m, "Client")
        .def(pybind11::init([]() { return new wrapper_client(spaces::get_instance()); }))
        .def(pybind11::init([](const pybind11::str& s) { return new wrapper_client(spaces::get_instance(std::string(s))); }))
        .def("execute", &wrapper_client::execute, pybind11::arg("query"));

    pybind11::class_<wrapper_connection>(m, "Connection")
        .def(pybind11::init([](wrapper_client* client) { return new wrapper_connection(client); }))
        .def("execute", &wrapper_connection::execute, pybind11::arg("query"))
        .def("cursor", &wrapper_connection::cursor)
        .def("close", &wrapper_connection::close)
        .def("commit", &wrapper_connection::commit)
        .def("rollback", &wrapper_connection::rollback);

    pybind11::class_<wrapper_cursor, boost::intrusive_ptr<wrapper_cursor>>(m, "Cursor")
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
        .def("sort", &wrapper_cursor::sort, pybind11::arg("key_or_list"), pybind11::arg("direction") = pybind11::none())
        .def("execute", &wrapper_cursor::execute, pybind11::arg("querry"))
        .def("fetchone", &wrapper_cursor::fetchone)
        .def("fetchmany", &wrapper_cursor::fetchmany, pybind11::arg("size") = 1)
        .def("fetchall", &wrapper_cursor::fetchall)
        .def_property_readonly("description", &wrapper_cursor::description)
        .def_property_readonly("rowcount", &wrapper_cursor::rowcount);

    m.def("to_aggregate", &test_to_statement);
}
