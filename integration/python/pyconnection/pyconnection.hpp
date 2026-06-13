#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>

#include <connection_environment/expression/expression_factory.hpp>
#include <connection_environment/relation/relation_factory.hpp>

#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <core/string_util/case_insensitive.hpp>
#include <memory>

#include <mutex>
#include <string>
#include <vector>

namespace otterbrix {
    class otterbrix_t;
    class PyConnection;
    class PyResult;
    class PyRelation;

    using Result = components::cursor::cursor_t_ptr;
    using pyconnection_ptr = std::shared_ptr<PyConnection>;
    using pycursor_ptr = std::shared_ptr<PyConnection>;

    struct DefaultConnectionHolder {
    public:
        DefaultConnectionHolder();
        ~DefaultConnectionHolder();

    public:
        DefaultConnectionHolder(const DefaultConnectionHolder &other) = delete;
        DefaultConnectionHolder(DefaultConnectionHolder &&other) = delete;
        DefaultConnectionHolder &operator=(const DefaultConnectionHolder &other) = delete;
        DefaultConnectionHolder &operator=(DefaultConnectionHolder &&other) = delete;

    public:
        pyconnection_ptr Get();
        void Set(pyconnection_ptr conn);

    private:
        pyconnection_ptr connection;
        std::mutex l;
    };

    class Cursors {
    public:
        Cursors();
        ~Cursors();
    public:
        void AddCursor(pycursor_ptr conn);
        void ClearCursors();
    private:
        std::mutex lock;
        std::vector<std::weak_ptr<PyConnection>> cursors;
    };

    // Main class. PyConnection IS the engine connection for the Python layer:
    // it inherits the expression / relation factories directly and delegates
    // execution to space->dispatcher() (execute_sql / execute_plan).
    class PyConnection
        : public ExpressionFactory
        , public RelationFactory
        , public std::enable_shared_from_this<PyConnection>
    {
    private:
        Cursors cursors;
        boost::intrusive_ptr<otterbrix_t> space;
    public:
        PyConnection(const boost::intrusive_ptr<otterbrix_t>& space);
        PyConnection(const PyConnection& other);
        static pyconnection_ptr Connect(const py::object &database_p, bool read_only,
                const py::dict &config_options);
        ~PyConnection();
        static void Initialize(py::handle& m);
    private:
        static DefaultConnectionHolder default_connection;
    public:
	    static pyconnection_ptr DefaultConnection();
	    static void SetDefaultConnection(pyconnection_ptr conn);

    public:
        static void Cleanup();

    public:
        // Execution surface, formerly ConnectionEnvironment. Every method here
        // routes through space->dispatcher().
        void SetNullConnection();
        void CreateDatabase(const std::string& name);
        built_relation_t RelationFromQuery(const std::string& query);
        Result ExecuteInternal(const std::string& query);
        Result Execute(const components::logical_plan::node_ptr& node, bool optimize = false);
        components::cursor::cursor_t_ptr QueryRelation(const components::logical_plan::node_ptr& rel);

    public:
        py::list ListTables();

        pyconnection_ptr Enter();
        void Exit(const py::object& exc_type, const py::object& exc,
                const py::object& traceback);

        void Close();

        pycursor_ptr Cursor();
        pycursor_ptr Execute(const py::object& query);

        std::unique_ptr<PyRelation> RunQuery(const py::object& query, std::string alias = "");
    public:
        std::unique_ptr<PyRelation> FromDF(const py::object& value);
        std::unique_ptr<PyRelation> FromObject(const py::object& value);

    };
} // namespace otterbrix
