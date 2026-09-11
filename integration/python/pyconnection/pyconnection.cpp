#include "pyconnection.hpp"
#include <cassert>
#include <common/string_util/string_util.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <connection_environment/connection_environment.hpp>
#include <connection_environment/relation/relation_factory.hpp>
#include <integration/cpp/catalog_listing.hpp>
#include <memory>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/pyresult.hpp>
#include <scan/python_replacement_scan.hpp>
#include <stdexcept>
#include <string>
#include <vector>

using namespace components;

namespace otterbrix {
    default_connection_holder_t::default_connection_holder_t() = default;
    default_connection_holder_t::~default_connection_holder_t() = default;

    namespace {
        // Where make_space's error channel meets Python's only error channel (an exception);
        // list_tables below uses the same shape.
        boost::intrusive_ptr<otterbrix_t> open_space_or_raise(const module_arena_ptr& arena,
                                                              const std::filesystem::path& path) {
            // `space.error().what` is copied into the exception's std::string right here, so
            // nothing allocated from the module's arena leaves this frame.
            auto space = connection_environment_t::make_space(&arena->resource, path);
            if (space.has_error()) {
                const auto& err = space.error();
                throw std::runtime_error("connect: " + std::string(err.what.begin(), err.what.end()));
            }
            return std::move(space.value());
        }

        // Same translation, one step later: a cursor's core::error_t raised verbatim.
        void raise_if_error(const char* what, const components::cursor::cursor_t_ptr& cursor) {
            if (!cursor) {
                throw std::runtime_error(std::string(what) + ": the engine returned no cursor");
            }
            if (cursor->is_error()) {
                const auto err = cursor->get_error();
                throw std::runtime_error(std::string(what) + ": " + std::string(err.what.begin(), err.what.end()));
            }
        }
    } // namespace

    pyconnection_ptr default_connection_holder_t::get(const module_arena_ptr& arena) {
        std::lock_guard<std::mutex> guard(l);
        if (!connection) {
            auto default_path = std::filesystem::absolute(connection_environment_t::DEFAULT_FOLDER);
            connection = std::make_shared<py_connection_t>(open_space_or_raise(arena, default_path));
        }
        return connection;
    }

    void default_connection_holder_t::set(pyconnection_ptr conn) {
        std::lock_guard<std::mutex> guard(l);
        connection = conn;
    }

    cursors_t::cursors_t() = default;
    cursors_t::~cursors_t() = default;

    void cursors_t::add_cursor(pycursor_ptr conn) {
        std::lock_guard<std::mutex> l(lock);

        std::vector<std::weak_ptr<py_connection_t>> compacted_cursors;
        bool needs_compaction = false;
        for (auto& cur_p : cursors) {
            auto cur = cur_p.lock();
            if (!cur) {
                needs_compaction = true;
                continue;
            }
            compacted_cursors.push_back(cur_p);
        }
        if (needs_compaction) {
            cursors = std::move(compacted_cursors);
        }

        cursors.push_back(conn);
    }

    void cursors_t::clear_cursors() {
        std::lock_guard<std::mutex> l(lock);

        for (auto& cur : cursors) {
            auto cursor = cur.lock();
            if (!cursor) {
                continue;
            }
            // close() does a py::gil_scoped_release internally, so it needs the GIL held here in order to release it.
            py::gil_scoped_acquire gil;
            cursor->close();
        }

        cursors.clear();
    }

    default_connection_holder_t py_connection_t::default_connection_;

    pyconnection_ptr py_connection_t::default_connection(const module_arena_ptr& arena) {
        return default_connection_.get(arena);
    }

    void py_connection_t::set_default_connection(pyconnection_ptr conn) {
        return default_connection_.set(std::move(conn));
    }

    py_connection_t::py_connection_t(const boost::intrusive_ptr<otterbrix_t>& space)
        : relation_factory_t(space)
        , expression_factory_t(space)
        , space(space) {
        // `tmp` is relation_factory_t's scratch database for DataFrame aggregates. IF NOT EXISTS, since
        // make_space does not wipe the directory, so a second connect finds it already there (normal, not a
        // failure); any other failure is raised.
        auto session = otterbrix::session_id_t();
        auto cursor = space->dispatcher()->execute_sql(session, "CREATE DATABASE IF NOT EXISTS tmp;");
        if (!cursor) {
            throw std::runtime_error("connect: creating the scratch database 'tmp' returned no cursor");
        }
        if (cursor->is_error()) {
            const auto& err = cursor->get_error();
            throw std::runtime_error("connect: creating the scratch database 'tmp' failed: " +
                                     std::string(err.what.begin(), err.what.end()));
        }
    }

    py_connection_t::py_connection_t(const py_connection_t& other)
        : relation_factory_t(other)
        , expression_factory_t(other)
        , std::enable_shared_from_this<py_connection_t>(other)
        , space(other.space) {}

    pyconnection_ptr py_connection_t::connect(const module_arena_ptr& arena,
                                             const py::object& database_p,
                                             bool read_only,
                                             const py::dict& config_options) {
        // Without an arena the refusals below can't put their message anywhere.
        // A throw, not an assert: NDEBUG deletes the assert, leaving a null dereference.
        if (!arena) {
            throw std::runtime_error("connect needs the module's arena");
        }
        std::string db_str;
        if (py::isinstance<py::str>(database_p)) {
            db_str = py::str(database_p);
        } else {
            throw std::runtime_error("Please provide either a str or a pathlib.Path");
        }

        // Both parameters are advertised in main.cpp's `connect` binding but wired to
        // nothing -- `read_only=True` silently handed back a writable connection, and `config`
        // was discarded. Refused until implemented, rather than silently ignored.
        if (read_only) {
            throw std::runtime_error("connect: read_only=True is not implemented; the connection would be writable");
        }
        if (!config_options.empty()) {
            throw std::runtime_error("connect: config options are not implemented and would be ignored; "
                                     "pass no config");
        }

        std::filesystem::path path = db_str;
        if (path.is_relative()) {
            path = std::filesystem::absolute(path);
        }

        pyconnection_ptr con = nullptr;
        if (db_str == connection_environment_t::DEFAULT_FOLDER) {
            con = default_connection_.get(arena);
        } else {
            con = std::make_shared<py_connection_t>(open_space_or_raise(arena, path));
        }

        return con;
    }

    py_connection_t::~py_connection_t() { py::gil_scoped_release gil; }

    void py_connection_t::cleanup() {
        default_connection_.set(nullptr);
        connection_environment_t::cleanup();
    }

    void py_connection_t::set_null_connection() {
        space = nullptr;
        expression_factory_t::set_null_space();
    }

    void py_connection_t::create_database(const std::string& name) {
        auto session = session_id_t();
        space->dispatcher()->execute_sql(session, "CREATE DATABASE " + name + ";");
    }

    // A closed connection has a null space; calling `space->dispatcher()` on it relies on an assert that
    // NDEBUG removes, leaving a plain null dereference. Guards every road into the engine, including building
    // a relation (from_df/from_object, py_relation_t::live_env), which never dereferences this `space`
    // directly but would otherwise return a relation with none.
    void py_connection_t::refuse_if_closed() const {
        if (!space) {
            throw std::runtime_error("the connection is closed");
        }
    }

    result_t py_connection_t::execute_internal(const std::string& query) {
        refuse_if_closed();
        // Delegates rather than re-implementing wrapper_dispatcher_t::execute_sql, which
        // already catches raw_parser's throw, guards `linitial` against an empty parse list,
        // and passes the registered parser extensions and query text to the transformer.
        auto session = session_id_t();
        return space->dispatcher()->execute_sql(session, query);
    }

    result_t py_connection_t::execute(const components::logical_plan::node_ptr& node_in, bool optimize) {
        refuse_if_closed();
        auto session = session_id_t();
        auto node = node_in;
        if (optimize) {
            node = components::planner::optimize(node->resource(), node, nullptr);
        }
        return space->dispatcher()->execute_plan(
            session,
            components::logical_plan::execution_plan_t{node->resource(), node, expression_factory_t::get_params()});
    }

    cursor::cursor_t_ptr py_connection_t::query_relation(const components::logical_plan::node_ptr& rel) {
        refuse_if_closed();
        auto session = otterbrix::session_id_t();
        return space->dispatcher()->execute_plan(
            session,
            components::logical_plan::execution_plan_t{space->dispatcher()->resource(),
                                                       rel,
                                                       expression_factory_t::get_params()});
    }

    py::list py_connection_t::list_tables() {
        py::gil_scoped_acquire gil;

        // The query and the pg_class decode live in integration/cpp/catalog_listing.hpp so
        // the C++ suite can test them; this wrapper only translates errors to Python's.
        auto cursor = execute_internal(std::string{kListTablesQuery});
        auto names = user_table_names_from_pg_class(space->dispatcher()->resource(), cursor);

        // A failed catalog read is not "no tables"; returning [] here would make
        // the two indistinguishable to every caller.
        if (names.has_error()) {
            const auto& err = names.error();
            throw std::runtime_error("listTables: reading pg_class failed: " +
                                     std::string(err.what.begin(), err.what.end()));
        }

        py::list res;
        for (const auto& name : names.value()) {
            res.append(py::str(std::string(name.begin(), name.end())));
        }
        return res;
    }

    pyconnection_ptr py_connection_t::enter() { return shared_from_this(); }

    void py_connection_t::exit(const py::object& exc_type, const py::object& exc, const py::object& /*traceback*/) {
        this->close();
        if (exc_type.ptr() != Py_None) {
            PyErr_SetObject(exc_type.ptr(), exc.ptr());
            throw py::error_already_set();
        }
    }

    void py_connection_t::close() {
        assert(py::gil_check());
        py::gil_scoped_release release;
        set_null_connection();
        cursors.clear_cursors();
    }

    pycursor_ptr py_connection_t::cursor() {
        pycursor_ptr res = std::make_shared<py_connection_t>(*this);
        cursors.add_cursor(res);
        return res;
    }

    std::unique_ptr<py_result_t> py_connection_t::execute(const py::object& query) {
        py::gil_scoped_acquire gil;
        // A wrong argument type is a TypeError in Python, not a silent
        // no-statement-ran that hands the caller the connection back.
        if (!py::isinstance<py::str>(query)) {
            throw py::type_error("execute: query must be a str, got " +
                                 std::string(py::str(query.get_type().attr("__name__"))));
        }
        auto cursor = execute_internal(std::string(py::str(query)));
        raise_if_error("execute", cursor);
        return std::make_unique<py_result_t>(this, cursor, py_result_t::columns_of(cursor));
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_df(const py::object& value) {
        // Building is a road into the engine too, not just `execute`: create_df_relation allocates from
        // relation_factory_t's own (undying) copy of the space, but py_relation_t frees them via the same copy
        // that close() nulls -- without this refusal, a closed connection would return a relation with live
        // memory and no arena to free it.
        refuse_if_closed();
        std::string name = "df_no_idea";
        // Built on the space's arena, which refuse_if_closed() above guarantees is live.
        auto tableref = scan_t::replacement_object(space->dispatcher()->resource(), value, name);

        return std::make_unique<py_relation_t>(shared_from_this(),
                                              relation_factory_t::create_df_relation(std::move(tableref)));
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_object(const py::object& value) {
        // Same road as from_df above, same refusal.
        refuse_if_closed();
        std::string name = "object_no_idea";
        auto tableref = scan_t::try_replacement_object(space->dispatcher()->resource(), value, name);
        assert(tableref);

        return std::make_unique<py_relation_t>(shared_from_this(),
                                              relation_factory_t::create_df_relation(std::move(tableref)));
    }

} // namespace otterbrix
