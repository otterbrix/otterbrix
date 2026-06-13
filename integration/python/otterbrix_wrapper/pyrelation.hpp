#pragma once

#include "pyresult.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <components/logical_plan/node.hpp>
#include <components/table/column_definition.hpp>
#include <memory>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {
    class PyConnection;
    class PyExpression;
    class ExpressionFactory;
    struct built_relation_t;
    using pyexpr_ptr = std::shared_ptr<PyExpression>;

    class PyRelation {
    public:
        PyRelation(PyConnection* env, built_relation_t rel);
        PyRelation(std::unique_ptr<PyResult> result);

        ~PyRelation();
        static void Initialize(py::handle& m);

        std::unique_ptr<PyRelation> Project(const py::args& args);
        std::unique_ptr<PyRelation> Filter(const py::object &expr);

        std::unique_ptr<PyRelation> Order(const std::string& expr);
        std::unique_ptr<PyRelation> Sort(const py::args& args);

        std::unique_ptr<PyRelation> Group(const py::args& args);

        std::unique_ptr<PyRelation> Join(const PyRelation& other, const py::object& condition, const std::string& type);
        std::unique_ptr<PyRelation> Cross(const PyRelation& other);

        std::unique_ptr<PyRelation> Limit(int64_t count);

        components::cursor::cursor_t_ptr ExecuteInternal(bool stream_result = false);

        void ExecuteOrThrow(bool stream_result = false);

        // Fetch
        Optional<py::tuple> FetchOne();
        py::list FetchMany(idx_t size);
        py::list FetchAll();
        PandasDataFrame FetchDF();

        py::list Columns();
        py::list ColumnTypes();

        // Internal functions (not exposed to Python)
        ExpressionFactory* GetExpressionFactory();
        void AssertRelation();
    private:
        // The eagerly-built logical_plan node (nullptr when this PyRelation was
        // created from a result). schema_ carries the column names/types this
        // node produces, computed eagerly at each chaining op so that
        // Columns()/ColumnTypes() work before execution.
        components::logical_plan::node_ptr node_;
        std::pmr::vector<components::table::column_definition_t> schema_;
        bool executed;
        PyConnection* env;
        std::unique_ptr<PyResult> result;
        bool optimize_ = false;
    };
} // namespace otterbrix
