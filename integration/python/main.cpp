#include <pybind11/pybind_wrapper.hpp>

#include <module_arena.hpp>

#include "sql/convert.hpp"
#include "sql/spaces.hpp"
#include "sql/wrapper_client.hpp"
#include "sql/wrapper_connection.hpp"
#include "sql/wrapper_cursor.hpp"

#include <otterbrix_wrapper/pyexpression.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/pyresult.hpp>
#include <otterbrix_wrapper/pytype.hpp>
#include <otterbrix_wrapper/type_creation.hpp>
#include <otterbrix_wrapper/typing.hpp>
#include <pyconnection/pyconnection.hpp>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

#ifndef OTTERBRIX_PYTHON_LIB_NAME
#define OTTERBRIX_PYTHON_LIB_NAME otterbrix
#endif

using namespace otterbrix;

PYBIND11_MODULE(OTTERBRIX_PYTHON_LIB_NAME, m) {
    // THE MODULE'S ARENA, CREATED HERE, OWNED BY THE MODULE, PASSED DOWN AS AN ARGUMENT.
    // The three reasons for this exact shape are written at module_arena_t
    // (integration/python/module_arena.hpp); what follows is only the mechanics.
    //
    // The capsule below is the module's OWNING reference: it holds the pointer this
    // `detach()` hands over, and its destructor adopts that same reference back
    // (`add_ref = false`) and lets it go. So `otterbrix.otterbrix.__arena__` -- the extension
    // module's own attribute, deliberately not one of the names the package's __init__.py
    // re-exports -- keeps the arena alive for as long as the module dictionary does, and that
    // is NOT ENOUGH ON ITS OWN: a capsule dies with the module while python objects built out
    // of the arena can still be alive. That is what the count is for: every such object holds its
    // own reference (otterbrix_py_type_t::arena_), so the last owner, not the module, is what
    // decides when the pool is released.
    module_arena_ptr module_arena{new module_arena_t()};
    m.add_object("__arena__",
                 pybind11::capsule(module_arena_ptr(module_arena).detach(), [](void* raw) {
                     module_arena_ptr released(static_cast<module_arena_t*>(raw), false);
                 }));

    otterbrix_py_typing_t::initialize(m, module_arena);
    type_creation::initialize(m, module_arena);
    py_expression_t::initialize(m);
    py_relation_t::initialize(m);
    // py_result_t is what `OtterBrixPyConnection.execute` hands back: compiling it is not
    // enough, an unregistered type is one Python cannot hold.
    py_result_t::initialize(m);
    py_connection_t::initialize(m);

    // The lambda CAPTURES the arena by value, so the bound function object is itself an
    // owner: `connect` cannot outlive the arena its refusals build their message on.
    // (make_space returns through the error channel, and every one of its refusals returns
    // BEFORE the engine -- and therefore the space's own arena -- exists, so the message has
    // to live somewhere else; the module's arena is that somewhere.)
    m.def("connect",
          [module_arena](const pybind11::object& database, bool read_only, const pybind11::dict& config) {
              return py_connection_t::connect(module_arena, database, read_only, config);
          },
          "Create a OtterBrix database instance. Can take a database file name to read/write persistent data and a "
          "read_only flag if no changes are desired",
          pybind11::arg("database") = "default",
          pybind11::arg("read_only") = false,
          pybind11::arg_v("config", pybind11::dict(), "None"));

    // https://pybind11.readthedocs.io/en/stable/advanced/misc.html#module-destructors
    auto clean_default_connection = []() { py_connection_t::cleanup(); };
    m.add_object("_clean_default_connection", pybind11::capsule(clean_default_connection));

    pybind11::class_<wrapper_client>(m, "Client")
        .def(pybind11::init([]() { return new wrapper_client(spaces::get_instance()); }))
        .def(pybind11::init(
            [](const pybind11::str& s) { return new wrapper_client(spaces::get_instance(std::string(s))); }))
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
