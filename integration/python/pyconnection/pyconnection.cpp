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
    // default_connection_holder_t
    default_connection_holder_t::default_connection_holder_t() = default;
    default_connection_holder_t::~default_connection_holder_t() = default;

    namespace {
        // The single place where make_space's error channel meets Python's.
        // Python has exactly one way to say "this failed" — an exception — so the
        // engine's own message is carried into a RuntimeError verbatim rather than
        // swallowed. Same shape list_tables uses below.
        boost::intrusive_ptr<otterbrix_t> open_space_or_raise(const module_arena_ptr& arena,
                                                              const std::filesystem::path& path) {
            // The refusal message is built on the module's arena and read right here, on the
            // next line: `space.error().what` is copied into the std::string the exception
            // carries, so nothing allocated from the arena leaves this frame.
            auto space = connection_environment_t::make_space(&arena->resource, path);
            if (space.has_error()) {
                const auto& err = space.error();
                throw std::runtime_error("connect: " + std::string(err.what.begin(), err.what.end()));
            }
            return std::move(space.value());
        }

        // The same translation point, one step later. A cursor carries the
        // engine's own core::error_t; Python has exactly one way to hear about a
        // failure, so it is raised verbatim instead of being dropped.
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

    // cursors_t
    cursors_t::cursors_t() = default;
    cursors_t::~cursors_t() = default;

    void cursors_t::add_cursor(pycursor_ptr conn) {
        std::lock_guard<std::mutex> l(lock);

        // Clean up previously created cursors
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
                // The cursor has already been closed
                continue;
            }
            // This is *only* needed because we have a py::gil_scoped_release in close, so it *needs* the GIL in order to
            // release it don't ask me why it can't just realize there is no GIL and move on
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
        // `tmp` is the scratch database relation_factory_t materialises DataFrame
        // aggregates into. IF NOT EXISTS, because make_space no longer wipes the
        // directory: on the second connect to the same database `tmp` is already
        // there, and that is the normal case, not a failure. Anything else IS a
        // failure and is raised — a connection whose scratch database is missing
        // cannot serve from_df at all, and reporting success would only move the
        // error to a later, less obvious statement.
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
        // Rule 6: without an arena the refusals below have nowhere to put their message, and
        // a refusal that cannot speak is worse than the one it replaces. A throw, not an
        // assert: NDEBUG deletes the assert and leaves open_space_or_raise dereferencing null.
        if (!arena) {
            throw std::runtime_error("connect needs the module's arena");
        }
        std::string db_str;
        if (py::isinstance<py::str>(database_p)) {
            db_str = py::str(database_p);
        } else {
            throw std::runtime_error("Please provide either a str or a pathlib.Path");
        }

        // Rule 6. Both of these are advertised in integration/python/main.cpp's
        // `connect` binding and neither is wired to anything, so accepting them
        // told the caller their request had been honoured when it had not:
        // `read_only=True` handed back a fully writable connection, and every key
        // of `config` went into the bin. Until they are implemented, asking for
        // them is refused out loud instead of silently ignored.
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

    // --- Execution surface (formerly connection_environment_t) ---------------------

    void py_connection_t::set_null_connection() {
        space = nullptr;
        expression_factory_t::set_null_space();
    }

    void py_connection_t::create_database(const std::string& name) {
        auto session = session_id_t();
        space->dispatcher()->execute_sql(session, "CREATE DATABASE " + name + ";");
    }

    // A closed connection has no space, and `space->dispatcher()` on a null intrusive_ptr
    // aborts the process under an assert that NDEBUG removes -- leaving a plain null
    // dereference in the build that ships. Every road into the engine goes through this
    // refusal, not just the SQL one: executing a relation lands in `execute` below, and
    // BUILDING one lands in from_df / from_object and in py_relation_t::live_env -- that
    // road does not touch this `space` at all, and the relation it produced came out with
    // no space of its own to free its plan with.
    void py_connection_t::refuse_if_closed() const {
        if (!space) {
            throw std::runtime_error("the connection is closed");
        }
    }

    result_t py_connection_t::execute_internal(const std::string& query) {
        refuse_if_closed();
        // One SQL pipeline, not two. Re-implementing wrapper_dispatcher_t::execute_sql here
        // means re-deriving three things it already gets right: raw_parser's throw is caught,
        // `linitial` is never applied to an unchecked parse list (an empty statement
        // dereferences nothing and takes the process down), and both the registered parser
        // extensions and the query text reach the transformer — without them extension syntax
        // is rejected and error messages lose their position.
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

        // Enumerate USER tables straight from the engine catalog (pg_class) instead
        // of a local registry. The query and the decode that filters it down to user
        // tables both live in cpp_otterbrix (integration/cpp/catalog_listing.hpp), so
        // the C++ suite can gate them; everything this wrapper adds is the translation
        // from the engine's error channel to Python's.
        auto cursor = execute_internal(std::string{kListTablesQuery});
        auto names = user_table_names_from_pg_class(space->dispatcher()->resource(), cursor);

        // Rule 6: a catalog read that failed is NOT a database with no tables in it.
        // Python's only error channel is an exception, so the failure is raised as a
        // RuntimeError carrying the engine's own message. Returning [] here would tell
        // every caller "no tables" whenever the read broke, and no caller could tell the
        // difference.
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
            // Propagate the exception if any occurred
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
        // Rule 6: a wrong argument type is a TypeError in Python, not a silent
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
        // BUILDING is a road into the engine too, and it is not the one `execute` guards.
        // `relation_factory_t::create_df_relation` allocates the node and the schema out of
        // relation_factory_t's OWN copy of the space -- the copy close() deliberately leaves
        // alive so the scratch tables can still be dropped -- while the space py_relation_t
        // keeps to free them with comes from the copy close() nulls. Without this refusal a
        // closed connection handed back a relation holding live memory and no reference to
        // its arena, and the interpreter died freeing it at shutdown.
        refuse_if_closed();
        std::string name = "df_no_idea";
        // The ref is built on the SPACE's arena, the same one create_df_relation consumes it
        // with a line later. refuse_if_closed() above is what makes that arena live.
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
