#include "pyrelation.hpp"

#include "pytype.hpp"

#include "pyexpression.hpp"

#include <connection_environment/connection_environment.hpp>
#include <connection_environment/relation/relation_factory.hpp>

#include <core/string_util/string_util.hpp>
#include <components/logical_plan/node_join.hpp>

#include <magic_enum.hpp>

using namespace components;

namespace otterbrix {
    
    PyRelation::PyRelation(ConnectionEnvironment* env, shared_ptr<Relation> rel_) 
        : env(env), rel(rel_) {
        if (!rel) {
            throw std::runtime_error("PyRelation created without a relation");
        }
        this->executed = false;
    }

    PyRelation::PyRelation(unique_ptr<PyResult> result) :
        rel(nullptr), result(std::move(result)) {
        if (!result) {
            throw std::runtime_error("PyRelation created without a result");
        }
        this->executed = true;

    }
    
    PyRelation::~PyRelation() {
        assert(py::gil_check()); 
        py::gil_scoped_release gil;
        rel.reset();
    }

    static cursor::cursor_t_ptr PyExecuteRelation(ConnectionEnvironment* env, const Relation& rel,
            bool /*stream_result*/ = false, bool optimize = false) {
        assert(py::gil_check());
        py::gil_scoped_release release;
        return env->Execute(rel, optimize);
    }

    unique_ptr<PyRelation> PyRelation::Project(const py::args& args) {
        if (args.size() == 0) {
            return nullptr;
        }
        vector<Expression> fields;
        fields.reserve(args.size());
        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            }
            fields.push_back(py_expr->GetExpression());
        }
        return make_unique<PyRelation>(env, env->SelectRelation(rel, std::move(fields)));
    }

    unique_ptr<PyRelation> PyRelation::Filter(const py::object &condition) {
        if (py::isinstance<py::str>(condition)) { 
            throw std::runtime_error("Implementation Error. Couldn\'t execute string expression");
        } 
        pyexpr_ptr py_expr;
        if (!py::try_cast(condition, py_expr)) {
            throw std::runtime_error("Invalid Input Exception. Please provide either a string or a PyExpression object to \'filter\'"); 
        }

        const auto& expr = py_expr->GetExpression();
        return make_unique<PyRelation>(env, env->FilterRelation(rel, expr));

    }

    unique_ptr<PyRelation> PyRelation::Order(const string& arg) {
        auto* factory = GetExpressionFactory();
        auto expr = factory->SortExpression(arg);
        return make_unique<PyRelation>(env, env->SortRelation(rel, {expr}));
    }

    unique_ptr<PyRelation> PyRelation::Sort(const py::args& args) {
        vector<Expression> order_nodes;
        order_nodes.reserve(args.size());

        auto* factory = GetExpressionFactory();
        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression(); 
                order_nodes.push_back(factory->SortExpression(expr));
            }
        }
        return make_unique<PyRelation>(env, env->SortRelation(rel, std::move(order_nodes)));
    }


    unique_ptr<PyRelation> PyRelation::Group(const py::args& args) {
        vector<Expression> fields;
        fields.reserve(args.size());
        

        for (auto arg : args) {
            shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression(); 
                fields.push_back(expr);
            }
            
        }
        return make_unique<PyRelation>(env, env->GroupRelation(rel, std::move(fields)));
    }
    unique_ptr<PyRelation> PyRelation::Join(const PyRelation& other, const py::object& condition, const string& type) {
        auto type_string = string_utils::Lower(type);
        logical_plan::join_type dtype;

        auto parse_result = magic_enum::enum_cast<logical_plan::join_type>(type);
        if (parse_result.has_value()) {
            dtype = parse_result.value();
        } else {
            throw std::runtime_error("Couldn\'t parse the join type");
        }

        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("OtterBrix couldn\'t parse condition. Please call join with an expression parameter");
        }

        vector<Expression> exprs;
        shared_ptr<PyExpression> py_expr;
        if (!condition.is_none()) {

            if (!py::try_cast<shared_ptr<PyExpression>>(condition, py_expr)) {
                throw std::runtime_error("Please provide condition as an expression either in string form or as an Expression object");
            }
            const auto& expr = py_expr->GetExpression();
            exprs.push_back(expr);
        } else {
            exprs.push_back(env->TrueExpression());
        }
        return make_unique<PyRelation>(env, env->JoinRelation(rel, other.rel, exprs, dtype));
    }


    unique_ptr<PyRelation> PyRelation::Cross(const PyRelation& other) {
        return Join(other, py::none(), "cross");
    }

    unique_ptr<PyRelation> PyRelation::Limit(int64_t count) {
        if (!rel) return nullptr;
        auto new_rel = env->LimitRelation(rel, count);
        return make_unique<PyRelation>(env, new_rel);
    }

    cursor::cursor_t_ptr PyRelation::ExecuteInternal(bool stream_result) {
        executed = true;
        if (!rel) {
            return nullptr;
        }
        return PyExecuteRelation(env, *rel, stream_result, optimize_);
    }


    void PyRelation::ExecuteOrThrow(bool stream_result) {
        py::gil_scoped_acquire gil;
        result.reset();
        auto query_result = ExecuteInternal(stream_result);
        if (!query_result) {
            throw std::runtime_error("ExecuteOrThrow - no query available to execute");
        }
        if (query_result->is_error()) {
            throw std::runtime_error(query_result->get_error().what.c_str());
        }
        result = make_unique<PyResult>(env, std::move(query_result), rel->GetColumns());
    }

    // Fetch

    Optional<py::tuple> PyRelation::FetchOne() {
        if (!result) {
            if (!rel) {
                return py::none();
            }    
            ExecuteOrThrow(true);
        }    
        if (result->IsClosed()) {
            return py::none();
        }    
        return result->Fetchone();
    }

    py::list PyRelation::FetchMany(idx_t size) {
        if (!result) {
            if (!rel) {
                return py::list();
            }    
            ExecuteOrThrow(true);
            assert(result);
        }    
        if (result->IsClosed()) {
            return py::list();
        }    
        return result->Fetchmany(size);
    }

    py::list PyRelation::FetchAll() {
        if (!result) {
            if (!rel) {
                return py::list();
            }    
            ExecuteOrThrow();
        }
        if (result->IsClosed()) {
            return py::list();
        }
        auto res = result->Fetchall();
        result = nullptr;
        return res;
    }

    PandasDataFrame PyRelation::FetchDF() {
        if (!result) {
            if (!rel) {
                return py::list();
            }    
            ExecuteOrThrow();
        }
        if (result->IsClosed()) {
            return py::list();
        }
        auto res = result->FetchDF();
        result = nullptr;
        return res;
    }

    pyexpr_ptr PyRelation::ColumnExpression(const string& name) {

        return make_shared<PyExpression>(expressions::key_t(std::pmr::get_default_resource(), name), GetExpressionFactory());
    }

    py::list PyRelation::Columns() {
        AssertRelation();
        py::list res;
        auto cols = rel->GetColumns();
        for (auto &col : cols) {
            res.append(col.name());
        }
        return res;
    }

    py::list PyRelation::ColumnTypes() {
        AssertRelation();
        py::list res;
        auto cols = rel->GetColumns();
        for (auto &col : cols) {
            res.append(OtterBrixPyType(col.type()));
        }
        return res;
    }

    // Internal functions (not exposed to Python)
    ExpressionFactory* PyRelation::GetExpressionFactory() {
        return static_cast<ExpressionFactory*>(env);
    }

    void PyRelation::AssertRelation() {
        if (!rel) {
            throw std::runtime_error("This relation was created from a result");
        }
    }
} // namespace otterbrix
