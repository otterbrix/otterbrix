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

#include <module_arena.hpp>
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
        // `arena` reaches make_space, which builds a refusal message on it when the
        // default folder cannot be opened; it is the module's arena (main.cpp). Taken by
        // reference and not stored: nothing this holder keeps is allocated from it.
        pyconnection_ptr get(const module_arena_ptr& arena);
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
    // BASE ORDER IS LOAD-BEARING, for the same reason relation_factory_t orders its own
    // members: bases are destroyed in REVERSE declaration order, so the base that still holds
    // the space must be declared FIRST and die LAST.
    //
    // close() nulls expression_factory_t's space and deliberately leaves relation_factory_t's
    // alive, so after a close the LAST reference to the arena is relation_factory_t's. With
    // expression_factory_t declared first it was destroyed last, which meant relation_factory_t
    // had already dropped the arena by the time expression_factory_t::values freed the
    // pmr strings of its string constants -- EXC_BAD_ACCESS inside memory_resource::deallocate.
    // Pinned by tests/test_constant_outlives_its_arena.py; the member-order half of the same
    // defect is documented on expression_factory_t::space.
    class py_connection_t
        : public relation_factory_t
        , public expression_factory_t
        , public std::enable_shared_from_this<py_connection_t> {
    private:
        cursors_t cursors;
        boost::intrusive_ptr<otterbrix_t> space;

    public:
        py_connection_t(const boost::intrusive_ptr<otterbrix_t>& space);
        py_connection_t(const py_connection_t& other);
        // `arena` is the module's arena; it is what a refused open builds its message on
        // (see connection_environment_t::make_space). Bound in main.cpp's PYBIND11_MODULE
        // body, which is where the arena is created.
        //
        // NOT STORED, and that is the enumeration answer for this class: everything a
        // connection keeps -- the plan nodes, the schemas, the constants in
        // expression_factory_t::values -- is allocated from the ENGINE's arena, which is a
        // member of `space` below and which this object already holds. The module's arena is
        // read on ONE path here, the refusal, and that message is turned into a Python
        // exception before this function returns.
        static pyconnection_ptr connect(const module_arena_ptr& arena,
                                        const py::object& database_p,
                                        bool read_only,
                                        const py::dict& config_options);
        ~py_connection_t();
        static void initialize(py::handle& m);

    private:
        static default_connection_holder_t default_connection_;

    public:
        static pyconnection_ptr default_connection(const module_arena_ptr& arena);
        static void set_default_connection(pyconnection_ptr conn);

    public:
        static void cleanup();

    public:
        // The space a result must keep alive: the result batch is pmr-allocated
        // from its memory resource, and `close()` can drop the last reference.
        const boost::intrusive_ptr<otterbrix_t>& space_ptr() const noexcept { return space; }

    public:
        // Throws unless this connection still holds its space. Every method below
        // dereferences `space`, and after close() there is none.
        //
        // Public because py_relation_t has to ask the same question: a relation chains
        // through this connection, and the roads it takes reach a space that close()
        // has already dropped -- both `space` here and expression_factory_t's copy.
        // One refusal, so a closed connection says the same thing whichever door is
        // knocked on.
        void refuse_if_closed() const;

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

        // Runs `query` and hands back its rows. Returns py_result_t rather than
        // the connection: the connection has no way to carry a result set, so
        // returning it leaves a SELECT with nothing a caller could read.
        // A statement the engine rejected raises instead of returning -- Python's
        // only error channel is an exception (same translation point `connect`
        // and `listTables` use).
        std::unique_ptr<py_result_t> execute(const py::object& query);

    public:
        std::unique_ptr<py_relation_t> from_df(const py::object& value);
        std::unique_ptr<py_relation_t> from_object(const py::object& value);
    };
} // namespace otterbrix
