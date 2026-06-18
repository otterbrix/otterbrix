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
    class py_connection_t;
    class py_expression_t;
    class expression_factory_t;
    struct built_relation_t;
    using pyexpr_ptr = std::shared_ptr<py_expression_t>;

    class py_relation_t {
    public:
        py_relation_t(py_connection_t* env, built_relation_t rel);
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
        // The eagerly-built logical_plan node (nullptr when this py_relation_t was
        // created from a result). schema_ carries the column names/types this
        // node produces, computed eagerly at each chaining op so that
        // columns()/column_types() work before execution.
        components::logical_plan::node_ptr node_;
        std::pmr::vector<components::table::column_definition_t> schema_;
        bool executed;
        py_connection_t* env;
        std::unique_ptr<py_result_t> result;
        bool optimize_ = false;
    };
} // namespace otterbrix
