#pragma once

#include <otterbrix_wrapper/pyrelation.hpp>
#include <pybind11/dataframe.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <connection_environment/expression/expression_factory.hpp>
#include <connection_environment/relation/relation_factory.hpp>

#include <common/string_util/case_insensitive.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <memory>

#include <mutex>
#include <string>
#include <vector>

namespace otterbrix {
    class otterbrix_t;
    class py_connection_t;
    class py_result_t;
    class py_relation_t;

    using result_t = components::cursor::cursor_t_ptr;
    using pyconnection_ptr = std::shared_ptr<py_connection_t>;
    using pycursor_ptr = std::shared_ptr<py_connection_t>;

    struct default_connection_holder_t {
    public:
        default_connection_holder_t();
        ~default_connection_holder_t();

    public:
        default_connection_holder_t(const default_connection_holder_t& other) = delete;
        default_connection_holder_t(default_connection_holder_t&& other) = delete;
        default_connection_holder_t& operator=(const default_connection_holder_t& other) = delete;
        default_connection_holder_t& operator=(default_connection_holder_t&& other) = delete;

    public:
        pyconnection_ptr get();
        void set(pyconnection_ptr conn);

    private:
        pyconnection_ptr connection;
        std::mutex l;
    };

    class cursors_t {
    public:
        cursors_t();
        ~cursors_t();

    public:
        void add_cursor(pycursor_ptr conn);
        void clear_cursors();

    private:
        std::mutex lock;
        std::vector<std::weak_ptr<py_connection_t>> cursors;
    };

    // Main class. py_connection_t IS the engine connection for the Python layer:
    // it inherits the expression / relation factories directly and delegates
    // execution to space->dispatcher() (execute_sql / execute_plan).
    class py_connection_t
        : public expression_factory_t
        , public relation_factory_t
        , public std::enable_shared_from_this<py_connection_t> {
    private:
        cursors_t cursors;
        boost::intrusive_ptr<otterbrix_t> space;

    public:
        py_connection_t(const boost::intrusive_ptr<otterbrix_t>& space);
        py_connection_t(const py_connection_t& other);
        static pyconnection_ptr connect(const py::object& database_p, bool read_only, const py::dict& config_options);
        ~py_connection_t();
        static void initialize(py::handle& m);

    private:
        static default_connection_holder_t default_connection_;

    public:
        static pyconnection_ptr default_connection();
        static void set_default_connection(pyconnection_ptr conn);

    public:
        static void cleanup();

    public:
        // Execution surface, formerly connection_environment_t. Every method here
        // routes through space->dispatcher().
        void set_null_connection();
        void create_database(const std::string& name);
        result_t execute_internal(const std::string& query);
        result_t execute(const components::logical_plan::node_ptr& node, bool optimize = false);
        components::cursor::cursor_t_ptr query_relation(const components::logical_plan::node_ptr& rel);

    public:
        py::list list_tables();

        pyconnection_ptr enter();
        void exit(const py::object& exc_type, const py::object& exc, const py::object& traceback);

        void close();

        pycursor_ptr cursor();
        pycursor_ptr execute(const py::object& query);

    public:
        std::unique_ptr<py_relation_t> from_df(const py::object& value);
        std::unique_ptr<py_relation_t> from_object(const py::object& value);
    };
} // namespace otterbrix
