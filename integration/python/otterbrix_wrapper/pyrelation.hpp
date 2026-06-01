#pragma once

//#include "pyexpression.hpp"
#include "pyresult.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <components/logical_plan/node.hpp>
#include <components/table/column_definition.hpp>
#include <core/types/memory.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>

namespace otterbrix {
    class ConnectionEnvironment;
    class PyExpression;
    using pyexpr_ptr = shared_ptr<PyExpression>;

    class PyRelation {
    public:
        PyRelation(ConnectionEnvironment* env, shared_ptr<Relation> rel);
        PyRelation(unique_ptr<PyResult> result);
            
        ~PyRelation();
        static void Initialize(py::handle& m);

        pyexpr_ptr ColumnExpression(const string& name);

        unique_ptr<PyRelation> Project(const py::args& args);
        unique_ptr<PyRelation> Filter(const py::object &expr);

        unique_ptr<PyRelation> Order(const string& expr);
        unique_ptr<PyRelation> Sort(const py::args& args);

        unique_ptr<PyRelation> Group(const py::args& args);
        
        unique_ptr<PyRelation> Join(const PyRelation& other, const py::object& condition, const string& type);
        unique_ptr<PyRelation> Cross(const PyRelation& other);
        
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
        bool executed;
        ConnectionEnvironment* env;
        shared_ptr<Relation> rel;
        unique_ptr<PyResult> result;
    };
} // namespace otterbrix
