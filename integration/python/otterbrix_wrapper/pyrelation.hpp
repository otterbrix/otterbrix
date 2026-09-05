#pragma once

#include "pyresult.hpp"

#include <components/logical_plan/node.hpp>
#include <components/table/column_definition.hpp>
#include <memory>
#include <pybind11/dataframe.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {
    class otterbrix_t;
    class py_connection_t;
    class py_expression_t;
    class expression_factory_t;
    struct built_relation_t;
    using pyexpr_ptr = std::shared_ptr<py_expression_t>;

    class py_relation_t {
    public:
        py_relation_t(std::shared_ptr<py_connection_t> env, built_relation_t rel);
        py_relation_t(std::unique_ptr<py_result_t> result);

        ~py_relation_t();
        static void initialize(py::handle& m);

        std::unique_ptr<py_relation_t> project(const py::args& args);
        std::unique_ptr<py_relation_t> filter(const py::object& expr);

        std::unique_ptr<py_relation_t> order(const std::string& expr);
        std::unique_ptr<py_relation_t> sort(const py::args& args);

        std::unique_ptr<py_relation_t> group(const py::args& args);

        std::unique_ptr<py_relation_t>
        join(const py_relation_t& other, const py::object& condition, const std::string& type);
        std::unique_ptr<py_relation_t> cross(const py_relation_t& other);

        std::unique_ptr<py_relation_t> limit(int64_t count);

        components::cursor::cursor_t_ptr execute_internal(bool stream_result = false);

        void execute_or_throw(bool stream_result = false);

        // Fetch
        py_optional_t<py::tuple> fetch_one();
        py::list fetch_many(idx_t size);
        py::list fetch_all();
        pandas_data_frame_t fetch_df();

        py::list columns();
        py::list column_types();

        // Internal functions (not exposed to Python)
        expression_factory_t* get_expression_factory();
        void assert_relation();

    private:
        // The connection every chaining op below has to go through, refused unless it is
        // still open. Chaining does not touch `space_` at all: the new node and schema are
        // allocated out of relation_factory_t's copy of the space, and the sort/join helpers
        // reach expression_factory_t's copy -- close() nulls the latter and LEAVES the former
        // alive on purpose, so the chain kept succeeding and produced relations with no space
        // of their own. Refusing here, before any of that runs, is also what keeps a closed
        // connection from creating one more scratch table nobody will ever read.
        py_connection_t& live_env();

    private:
        // The space `node_` and `schema_` were allocated out of, held so the relation owns
        // its own release path instead of borrowing the connection's. py_result_t holds the
        // space for the same reason and says so on its own `space` member in pyresult.hpp.
        // It is not covered by `env` below: `close()` can drop the space while the connection
        // object lives on, and the freeing happens in a destructor, which is the one place
        // that can neither check nor refuse. Declared FIRST so reverse-order member
        // destruction frees it LAST, after both members below have handed their memory back.
        // Never null while `node_` is set -- the constructor refuses that combination, and
        // live_env() keeps every chaining road from producing it.
        boost::intrusive_ptr<otterbrix_t> space_;

        // The eagerly-built logical_plan node (nullptr when this py_relation_t was
        // created from a result). schema_ carries the column names/types this
        // node produces, computed eagerly at each chaining op so that
        // columns()/column_types() work before execution.
        components::logical_plan::node_ptr node_;
        std::pmr::vector<components::table::column_definition_t> schema_;
        bool executed{false};
        // The connection every chaining op reaches back into, held rather than borrowed:
        // a relation is handed out to Python and the connection object that made it can be
        // dropped first, which left this back-pointer reading freed memory. Nothing here is
        // reachable from the connection, so there is no cycle to break.
        //
        // std::shared_ptr against rule 14, and a PARTIAL RECORD rather than a free choice:
        // pybind11 owns py_connection_t through a shared_ptr holder
        // (py::class_<py_connection_t, std::shared_ptr<py_connection_t>>, pyconnection/
        // initialize.cpp), so shared_ptr IS the connection's lifetime on this boundary --
        // pyconnection_ptr, enable_shared_from_this and the weak_ptr cursor list all predate
        // this member. Holding anything else here would either not keep the object alive or
        // would be a second, competing owner. Replacing it means replacing the holder, which
        // is the whole Python boundary, not this file.
        std::shared_ptr<py_connection_t> env;
        std::unique_ptr<py_result_t> result;
        bool optimize_ = false;
    };
} // namespace otterbrix
