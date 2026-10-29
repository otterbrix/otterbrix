#include "pyconnection.hpp"
#include <cassert>
#include <common/string_util/string_util.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <connection_environment/connection_environment.hpp>
#include <connection_environment/relation/relation_factory.hpp>
#include <memory>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <otterbrix_wrapper/pyresult.hpp>
#include <scan/python_replacement_scan.hpp>
#include <stdexcept>
#include <string>
#include <util/util.hpp>
#include <vector>

using namespace components;

namespace otterbrix {
    default_connection_holder_t::default_connection_holder_t() = default;
    default_connection_holder_t::~default_connection_holder_t() = default;

    namespace {
        // Where make_space's error channel meets Python's only error channel (an exception).
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
                util::raise_no_cursor_error(std::string(what) + ": the engine returned no cursor");
            }
            if (cursor->is_error()) {
                const auto err = cursor->get_error();
                util::raise_cursor_error(std::string(what) + ": " + std::string(err.what.begin(), err.what.end()),
                                         *cursor);
            }
        }
    } // namespace

    // The connection replaced here is destroyed after the lock is released: its destructor lets the GIL go.
    pyconnection_ptr default_connection_holder_t::get(const module_arena_ptr& arena) {
        pyconnection_ptr replaced;
        std::lock_guard<std::mutex> guard(l);
        if (!connection || connection->is_closed()) {
            auto default_path = std::filesystem::absolute(connection_environment_t::DEFAULT_FOLDER);
            replaced = std::move(connection);
            connection = std::make_shared<py_connection_t>(open_space_or_raise(arena, default_path));
        }
        return connection;
    }

    void default_connection_holder_t::set(pyconnection_ptr conn) {
        pyconnection_ptr replaced;
        std::lock_guard<std::mutex> guard(l);
        replaced = std::move(connection);
        connection = std::move(conn);
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

    py_session_t::py_session_t(boost::intrusive_ptr<otterbrix_t> space)
        : space_(std::move(space)) {}

    py_session_t::~py_session_t() {
        py::gil_scoped_release release;
        close();
    }

    void py_session_t::close() {
        if (closed_) {
            return;
        }
        closed_ = true;
        space_->dispatcher()->close_session(session_);
        space_ = nullptr;
    }

    py_connection_state_t::py_connection_state_t(boost::intrusive_ptr<py_connection_state_t> copied_from)
        : parent(std::move(copied_from)) {}

    bool py_connection_state_t::is_closed() const noexcept {
        for (const auto* state = this; state != nullptr; state = state->parent.get()) {
            if (state->closed) {
                return true;
            }
        }
        return false;
    }

    void py_connection_state_t::refuse_if_closed() const {
        if (is_closed()) {
            throw std::runtime_error("the connection is closed");
        }
    }

    py_connection_t::py_connection_t(const boost::intrusive_ptr<otterbrix_t>& space)
        : relation_factory_t(space->arena())
        , expression_factory_t(space->arena())
        , space(space)
        , session_(new py_session_t(space))
        , state_(new py_connection_state_t(nullptr)) {}

    py_connection_t::py_connection_t(const py_connection_t& other)
        : relation_factory_t(other)
        , expression_factory_t(other)
        , std::enable_shared_from_this<py_connection_t>(other)
        , space(other.space)
        , session_(other.session_)
        , closes_session_(false)
        , state_(new py_connection_state_t(other.state_)) {}

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

    void py_connection_t::set_null_connection() { space = nullptr; }

    void py_connection_t::create_database(const std::string& name) {
        space->dispatcher()->execute_sql(session_->id(), "CREATE DATABASE " + name + ";");
    }

    // Guards every road into the engine: a closed connection has no space, and under NDEBUG a dereference of it is a
    // plain null dereference. Building a relation (from_df/from_object, py_relation_t::live_env) is refused the same.
    void py_connection_t::refuse_if_closed() const { state_->refuse_if_closed(); }

    result_t py_connection_t::execute_internal(const std::string& query) {
        refuse_if_closed();
        // Delegates rather than re-implementing wrapper_dispatcher_t::execute_sql, which
        // already turns raw_parser's error into an error cursor, guards `linitial` against an empty parse list,
        // and passes the registered parser extensions and query text to the transformer.
        return space->dispatcher()->execute_sql(session_->id(), query);
    }

    result_t py_connection_t::execute(const components::logical_plan::node_ptr& node_in, bool optimize) {
        refuse_if_closed();
        auto node = node_in;
        if (optimize) {
            node =
                components::planner::optimize(node->resource(), node, nullptr, components::graph_execution_context{});
        }
        return space->dispatcher()->execute_plan(
            session_->id(),
            components::logical_plan::execution_plan_t{node->resource(), node, expression_factory_t::get_params()});
    }

    cursor::cursor_t_ptr py_connection_t::query_relation(const components::logical_plan::node_ptr& rel) {
        refuse_if_closed();
        return space->dispatcher()->execute_plan(
            session_->id(),
            components::logical_plan::execution_plan_t{space->dispatcher()->resource(),
                                                       rel,
                                                       expression_factory_t::get_params()});
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
        state_->closed = true;
        py::gil_scoped_release release;
        cursors.clear_cursors();
        if (closes_session_) {
            session_->close();
        }
        set_null_connection();
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
        return std::make_unique<py_result_t>(*this, cursor, py_result_t::columns_of(cursor));
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_df(const py::object& value) {
        refuse_if_closed();
        std::string name = "df_no_idea";
        auto tableref = scan_t::replacement_object(value, name);

        return std::make_unique<py_relation_t>(shared_from_this(),
                                               relation_factory_t::create_df_relation(std::move(tableref)));
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_object(const py::object& value) {
        // Same road as from_df above, same refusal.
        refuse_if_closed();
        std::string name = "object_no_idea";
        auto tableref = scan_t::try_replacement_object(value, name);
        assert(tableref);

        return std::make_unique<py_relation_t>(shared_from_this(),
                                               relation_factory_t::create_df_relation(std::move(tableref)));
    }

} // namespace otterbrix
