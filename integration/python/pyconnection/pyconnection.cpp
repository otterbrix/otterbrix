#include "pyconnection.hpp"
#include <common/string_util/string_util.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
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

    pyconnection_ptr default_connection_holder_t::get() {
        std::lock_guard<std::mutex> guard(l);
        if (!connection) {
            auto default_path = std::filesystem::absolute(connection_environment_t::DEFAULT_FOLDER);
            auto space = connection_environment_t::make_space(default_path);
            connection = std::make_shared<py_connection_t>(space);
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

    pyconnection_ptr py_connection_t::default_connection() { return default_connection_.get(); }

    void py_connection_t::set_default_connection(pyconnection_ptr conn) {
        return default_connection_.set(std::move(conn));
    }

    py_connection_t::py_connection_t(const boost::intrusive_ptr<otterbrix_t>& space)
        : expression_factory_t(space)
        , relation_factory_t(space)
        , space(space) {
        auto session = otterbrix::session_id_t();
        space->dispatcher()->execute_sql(session, "CREATE DATABASE tmp;");
    }

    py_connection_t::py_connection_t(const py_connection_t& other)
        : expression_factory_t(other)
        , relation_factory_t(other)
        , std::enable_shared_from_this<py_connection_t>(other)
        , space(other.space) {}

    pyconnection_ptr
    py_connection_t::connect(const py::object& database_p, bool /*read_only*/, const py::dict& /*config_options*/) {
        std::string db_str;
        if (py::isinstance<py::str>(database_p)) {
            db_str = py::str(database_p);
        } else {
            throw std::runtime_error("Please provide either a str or a pathlib.Path");
        }
        std::filesystem::path path = db_str;
        if (path.is_relative()) {
            path = std::filesystem::absolute(path);
        }

        pyconnection_ptr con = nullptr;
        if (db_str == connection_environment_t::DEFAULT_FOLDER) {
            con = default_connection_.get();
        } else {
            auto space = connection_environment_t::make_space(path);
            con = std::make_shared<py_connection_t>(space);
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

    result_t py_connection_t::execute_internal(const std::string& query) {
        using namespace components::sql::transform;

        auto session = session_id_t();
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));

        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (result.has_error()) {
            return components::cursor::make_cursor(space->dispatcher()->resource(), result.error());
        }
        auto plan = std::move(result).value();
        auto cursor = space->dispatcher()->execute_plan(session, std::move(plan));

        return cursor;
    }

    result_t py_connection_t::execute(const components::logical_plan::node_ptr& node_in, bool optimize) {
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
        // RuntimeError carrying the engine's own message. Returning [] here — what
        // this function used to do — told every caller "no tables" whenever the read
        // broke, and no caller could tell the difference.
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

    pycursor_ptr py_connection_t::execute(const py::object& query) {
        py::gil_scoped_acquire gil;
        if (py::isinstance<py::str>(query)) {
            execute_internal(std::string(py::str(query)));
        }
        return shared_from_this();
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_df(const py::object& value) {
        std::string name = "df_no_idea";
        auto tableref = scan_t::replacement_object(value, name);

        return std::make_unique<py_relation_t>(this, relation_factory_t::create_df_relation(std::move(tableref)));
    }

    std::unique_ptr<py_relation_t> py_connection_t::from_object(const py::object& value) {
        std::string name = "object_no_idea";
        auto tableref = scan_t::try_replacement_object(value, name);
        assert(tableref);

        return std::make_unique<py_relation_t>(this, relation_factory_t::create_df_relation(std::move(tableref)));
    }

} // namespace otterbrix
