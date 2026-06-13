#include "pyrelation.hpp"
#include <memory>

#include "pytype.hpp"

#include "pyexpression.hpp"

#include <pyconnection/pyconnection.hpp>
#include <connection_environment/relation/relation_factory.hpp>

#include <core/string_util/string_util.hpp>
#include <components/logical_plan/node_join.hpp>

#include <optional>
#include <string>
#include <vector>

using namespace components;

namespace otterbrix {

    namespace {
        std::optional<logical_plan::join_type> parse_join_type(const std::string& name) {
            if (name == "inner") {
                return logical_plan::join_type::inner;
            }
            if (name == "full") {
                return logical_plan::join_type::full;
            }
            if (name == "left") {
                return logical_plan::join_type::left;
            }
            if (name == "right") {
                return logical_plan::join_type::right;
            }
            if (name == "cross") {
                return logical_plan::join_type::cross;
            }
            if (name == "invalid") {
                return logical_plan::join_type::invalid;
            }
            return std::nullopt;
        }
    } // namespace

    PyRelation::PyRelation(PyConnection* env, built_relation_t rel)
        : node_(std::move(rel.node)), schema_(std::move(rel.columns)), env(env) {
        if (!node_) {
            throw std::runtime_error("PyRelation created without a relation");
        }
        this->executed = false;
    }

    PyRelation::PyRelation(std::unique_ptr<PyResult> result) :
        node_(nullptr), result(std::move(result)) {
        if (!this->result) {
            throw std::runtime_error("PyRelation created without a result");
        }
        this->executed = true;
    }

    PyRelation::~PyRelation() {
        assert(py::gil_check());
        py::gil_scoped_release gil;
        node_.reset();
    }

    static cursor::cursor_t_ptr PyExecuteRelation(PyConnection* env,
            const logical_plan::node_ptr& node, bool optimize = false) {
        assert(py::gil_check());
        py::gil_scoped_release release;
        return env->Execute(node, optimize);
    }

    std::unique_ptr<PyRelation> PyRelation::Project(const py::args& args) {
        if (args.size() == 0) {
            return nullptr;
        }
        std::vector<Expression> fields;
        fields.reserve(args.size());
        for (auto arg : args) {
            std::shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<std::shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            }
            fields.push_back(py_expr->GetExpression());
        }
        return std::make_unique<PyRelation>(env, env->SelectRelation({node_, schema_}, std::move(fields)));
    }

    std::unique_ptr<PyRelation> PyRelation::Filter(const py::object &condition) {
        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("Implementation Error. Couldn\'t execute string expression");
        }
        pyexpr_ptr py_expr;
        if (!py::try_cast(condition, py_expr)) {
            throw std::runtime_error("Invalid Input Exception. Please provide either a string or a PyExpression object to \'filter\'");
        }

        const auto& expr = py_expr->GetExpression();
        return std::make_unique<PyRelation>(env, env->FilterRelation({node_, schema_}, expr));
    }

    std::unique_ptr<PyRelation> PyRelation::Order(const std::string& arg) {
        auto* factory = GetExpressionFactory();
        auto expr = factory->SortExpression(arg);
        return std::make_unique<PyRelation>(env, env->SortRelation({node_, schema_}, {expr}));
    }

    std::unique_ptr<PyRelation> PyRelation::Sort(const py::args& args) {
        std::vector<Expression> order_nodes;
        order_nodes.reserve(args.size());

        auto* factory = GetExpressionFactory();
        for (auto arg : args) {
            std::shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<std::shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression();
                auto sorted = factory->SortExpression(expr);
                if (sorted.has_error()) {
                    throw std::runtime_error(std::string(sorted.error().what));
                }
                order_nodes.push_back(std::move(sorted.value()));
            }
        }
        return std::make_unique<PyRelation>(env, env->SortRelation({node_, schema_}, std::move(order_nodes)));
    }


    std::unique_ptr<PyRelation> PyRelation::Group(const py::args& args) {
        std::vector<Expression> fields;
        fields.reserve(args.size());

        for (auto arg : args) {
            std::shared_ptr<PyExpression> py_expr;
            if (!py::try_cast<std::shared_ptr<PyExpression>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->GetExpression();
                fields.push_back(expr);
            }
        }
        return std::make_unique<PyRelation>(env, env->GroupRelation({node_, schema_}, std::move(fields)));
    }

    std::unique_ptr<PyRelation> PyRelation::Join(const PyRelation& other, const py::object& condition, const std::string& type) {
        auto type_string = string_utils::Lower(type);

        auto parse_result = parse_join_type(type_string);
        if (!parse_result.has_value()) {
            throw std::runtime_error("Couldn\'t parse the join type");
        }
        auto dtype = parse_result.value();

        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("OtterBrix couldn\'t parse condition. Please call join with an expression parameter");
        }

        std::vector<Expression> exprs;
        std::shared_ptr<PyExpression> py_expr;
        if (!condition.is_none()) {
            if (!py::try_cast<std::shared_ptr<PyExpression>>(condition, py_expr)) {
                throw std::runtime_error("Please provide condition as an expression either in string form or as an Expression object");
            }
            const auto& expr = py_expr->GetExpression();
            exprs.push_back(expr);
        } else {
            exprs.push_back(env->TrueExpression());
        }
        return std::make_unique<PyRelation>(env,
            env->JoinRelation({node_, schema_}, {other.node_, other.schema_}, exprs, dtype));
    }


    std::unique_ptr<PyRelation> PyRelation::Cross(const PyRelation& other) {
        return Join(other, py::none(), "cross");
    }

    std::unique_ptr<PyRelation> PyRelation::Limit(int64_t count) {
        if (!node_) return nullptr;
        return std::make_unique<PyRelation>(env, env->LimitRelation({node_, schema_}, count));
    }

    cursor::cursor_t_ptr PyRelation::ExecuteInternal(bool /*stream_result*/) {
        executed = true;
        if (!node_) {
            return nullptr;
        }
        return PyExecuteRelation(env, node_, optimize_);
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
        result = std::make_unique<PyResult>(env, std::move(query_result),
            std::vector<components::table::column_definition_t>(schema_.begin(), schema_.end()));
    }

    // Fetch

    Optional<py::tuple> PyRelation::FetchOne() {
        if (!result) {
            if (!node_) {
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
            if (!node_) {
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
            if (!node_) {
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
            if (!node_) {
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

    py::list PyRelation::Columns() {
        AssertRelation();
        py::list res;
        for (const auto& col : schema_) {
            res.append(col.name());
        }
        return res;
    }

    py::list PyRelation::ColumnTypes() {
        AssertRelation();
        py::list res;
        for (const auto& col : schema_) {
            res.append(OtterBrixPyType(col.type()));
        }
        return res;
    }

    // Internal functions (not exposed to Python)
    ExpressionFactory* PyRelation::GetExpressionFactory() {
        return static_cast<ExpressionFactory*>(env);
    }

    void PyRelation::AssertRelation() {
        if (!node_) {
            throw std::runtime_error("This relation was created from a result");
        }
    }
} // namespace otterbrix
