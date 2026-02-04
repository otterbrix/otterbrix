#include <pybind11/pybind11.h>

#include "sql/wrapper_client.hpp"
#include "sql/wrapper_collection.hpp"
#include "sql/wrapper_cursor.hpp"
#include "sql/wrapper_database.hpp"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

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
        .def("__getitem__", &wrapper_client::get_or_create)
        .def("database_names", &wrapper_client::database_names)
        .def("execute", &wrapper_client::execute, py::arg("query"));

    py::class_<wrapper_connection>(m, "Connection")
        .def(py::init([](wrapper_client* client) { return new wrapper_connection(client); }))
        .def("execute", &wrapper_connection::execute, py::arg("query"))
        .def("cursor", &wrapper_connection::cursor)
        .def("close", &wrapper_connection::close);

    py::class_<wrapper_database, boost::intrusive_ptr<wrapper_database>>(m, "DataBase")
        .def("collection_names", &wrapper_database::collection_names)
        .def("drop_collection", &wrapper_database::drop_collection)
        .def("__getitem__", &wrapper_database::create);

    py::enum_<index_type>(m, "TypeIndex")
        .value("SINGLE", index_type::single)
        .value("COMPOSITE", index_type::composite)
        .value("MULTIKEY", index_type::multikey)
        .value("HASHED", index_type::hashed)
        .value("WILDCARD", index_type::wildcard)
        .export_values();

    py::class_<wrapper_collection, boost::intrusive_ptr<wrapper_collection>>(m, "Collection")
        .def("__repr__", &wrapper_collection::print)
        .def("__len__", &wrapper_collection::size)
        .def("count", &wrapper_collection::size)
        .def("insert", &wrapper_collection::insert, py::arg("documents"))
        .def("insert_one", &wrapper_collection::insert_one, py::arg("document"))
        .def("insert_many", &wrapper_collection::insert_many, py::arg("documents"))
        .def("update_one",
             &wrapper_collection::update_one,
             py::arg("filter"),
             py::arg("update"),
             py::arg("upsert") = false)
        .def("update_many",
             &wrapper_collection::update_many,
             py::arg("filter"),
             py::arg("update"),
             py::arg("upsert") = false)
        .def("find", &wrapper_collection::find, py::arg("filter") = py::dict())
        .def("find_one", &wrapper_collection::find_one, py::arg("filter") = py::dict())
        .def("delete_one", &wrapper_collection::delete_one, py::arg("filter") = py::dict())
        .def("delete_many", &wrapper_collection::delete_many, py::arg("filter") = py::dict())
        .def("drop", &wrapper_collection::drop)
        .def("create_index", &wrapper_collection::create_index, py::arg("keys"), py::arg("type"))
        ///.def("aggregate", &wrapper_collection::aggregate, py::arg("pipeline") = py::sequence())
        ;

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
        //.def("paginate", &wrapper_cursor::paginate)
        //.def("_order", &wrapper_cursor::_order)
        .def("sort", &wrapper_cursor::sort, py::arg("key_or_list"), py::arg("direction") = py::none())
        .def("execute", &wrapper_cursor::execute, py::arg("querry"));

    m.def("to_aggregate", &test_to_statement);
}
