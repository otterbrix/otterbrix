#include "pyrelation.hpp"
#include <memory>

#include "pytype.hpp"

#include "pyexpression.hpp"

#include <connection_environment/relation/relation_factory.hpp>
#include <pyconnection/pyconnection.hpp>

#include <common/string_util/string_util.hpp>
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

    py_relation_t::py_relation_t(py_connection_t* env, built_relation_t rel)
        : node_(std::move(rel.node))
        , schema_(std::move(rel.columns))
        , env(env) {
        if (!node_) {
            throw std::runtime_error("PyRelation created without a relation");
        }
        this->executed = false;
    }

    py_relation_t::py_relation_t(std::unique_ptr<py_result_t> result)
        : node_(nullptr)
        , result(std::move(result)) {
        if (!this->result) {
            throw std::runtime_error("PyRelation created without a result");
        }
        this->executed = true;
    }

    py_relation_t::~py_relation_t() {
        assert(py::gil_check());
        py::gil_scoped_release gil;
        node_.reset();
    }

    static cursor::cursor_t_ptr
    PyExecuteRelation(py_connection_t* env, const logical_plan::node_ptr& node, bool optimize = false) {
        assert(py::gil_check());
        py::gil_scoped_release release;
        return env->execute(node, optimize);
    }

    std::unique_ptr<py_relation_t> py_relation_t::project(const py::args& args) {
        if (args.size() == 0) {
            return nullptr;
        }
        std::vector<expression_wrapper_t> fields;
        fields.reserve(args.size());
        for (auto arg : args) {
            std::shared_ptr<py_expression_t> py_expr;
            if (!py::try_cast<std::shared_ptr<py_expression_t>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            }
            fields.push_back(py_expr->get_expression());
        }
        return std::make_unique<py_relation_t>(env, env->select_relation({node_, schema_}, std::move(fields)));
    }

    std::unique_ptr<py_relation_t> py_relation_t::filter(const py::object& condition) {
        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("Implementation Error. Couldn\'t execute string expression");
        }
        pyexpr_ptr py_expr;
        if (!py::try_cast(condition, py_expr)) {
            throw std::runtime_error(
                "Invalid Input Exception. Please provide either a string or a PyExpression object to \'filter\'");
        }

        const auto& expr = py_expr->get_expression();
        return std::make_unique<py_relation_t>(env, env->filter_relation({node_, schema_}, expr));
    }

    std::unique_ptr<py_relation_t> py_relation_t::order(const std::string& arg) {
        auto* factory = get_expression_factory();
        auto expr = factory->sort_expression(arg);
        return std::make_unique<py_relation_t>(env, env->sort_relation({node_, schema_}, {expr}));
    }

    std::unique_ptr<py_relation_t> py_relation_t::sort(const py::args& args) {
        std::vector<expression_wrapper_t> order_nodes;
        order_nodes.reserve(args.size());

        auto* factory = get_expression_factory();
        for (auto arg : args) {
            std::shared_ptr<py_expression_t> py_expr;
            if (!py::try_cast<std::shared_ptr<py_expression_t>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->get_expression();
                auto sorted = factory->sort_expression(expr);
                if (sorted.has_error()) {
                    throw std::runtime_error(std::string(sorted.error().what));
                }
                order_nodes.push_back(std::move(sorted.value()));
            }
        }
        return std::make_unique<py_relation_t>(env, env->sort_relation({node_, schema_}, std::move(order_nodes)));
    }

    std::unique_ptr<py_relation_t> py_relation_t::group(const py::args& args) {
        std::vector<expression_wrapper_t> fields;
        fields.reserve(args.size());

        for (auto arg : args) {
            std::shared_ptr<py_expression_t> py_expr;
            if (!py::try_cast<std::shared_ptr<py_expression_t>>(arg, py_expr)) {
                throw std::runtime_error("Please provide arguments of type Expression");
            } else {
                const auto& expr = py_expr->get_expression();
                fields.push_back(expr);
            }
        }
        return std::make_unique<py_relation_t>(env, env->group_relation({node_, schema_}, std::move(fields)));
    }

    std::unique_ptr<py_relation_t>
    py_relation_t::join(const py_relation_t& other, const py::object& condition, const std::string& type) {
        auto type_string = string_utils::lower(type);

        auto parse_result = parse_join_type(type_string);
        if (!parse_result.has_value()) {
            throw std::runtime_error("Couldn\'t parse the join type");
        }
        auto dtype = parse_result.value();

        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error(
                "OtterBrix couldn\'t parse condition. Please call join with an expression parameter");
        }

        std::vector<expression_wrapper_t> exprs;
        std::shared_ptr<py_expression_t> py_expr;
        if (!condition.is_none()) {
            if (!py::try_cast<std::shared_ptr<py_expression_t>>(condition, py_expr)) {
                throw std::runtime_error(
                    "Please provide condition as an expression either in string form or as an Expression object");
            }
            const auto& expr = py_expr->get_expression();
            exprs.push_back(expr);
        } else {
            exprs.push_back(env->true_expression());
        }
        return std::make_unique<py_relation_t>(
            env,
            env->join_relation({node_, schema_}, {other.node_, other.schema_}, exprs, dtype));
    }

    std::unique_ptr<py_relation_t> py_relation_t::cross(const py_relation_t& other) {
        return join(other, py::none(), "cross");
    }

    std::unique_ptr<py_relation_t> py_relation_t::limit(int64_t count) {
        if (!node_)
            return nullptr;
        return std::make_unique<py_relation_t>(env, env->limit_relation({node_, schema_}, count));
    }

    cursor::cursor_t_ptr py_relation_t::execute_internal(bool /*stream_result*/) {
        executed = true;
        if (!node_) {
            return nullptr;
        }
        return PyExecuteRelation(env, node_, optimize_);
    }

    void py_relation_t::execute_or_throw(bool stream_result) {
        py::gil_scoped_acquire gil;
        result.reset();
        auto query_result = execute_internal(stream_result);
        if (!query_result) {
            throw std::runtime_error("ExecuteOrThrow - no query available to execute");
        }
        if (query_result->is_error()) {
            throw std::runtime_error(query_result->get_error().what.c_str());
        }
        result = std::make_unique<py_result_t>(
            env,
            std::move(query_result),
            std::vector<components::table::column_definition_t>(schema_.begin(), schema_.end()));
    }

    // Fetch

    py_optional_t<py::tuple> py_relation_t::fetch_one() {
        if (!result) {
            if (!node_) {
                return py::none();
            }
            execute_or_throw(true);
        }
        if (result->is_closed()) {
            return py::none();
        }
        return result->fetchone();
    }

    py::list py_relation_t::fetch_many(idx_t size) {
        if (!result) {
            if (!node_) {
                return py::list();
            }
            execute_or_throw(true);
            assert(result);
        }
        if (result->is_closed()) {
            return py::list();
        }
        return result->fetchmany(size);
    }

    py::list py_relation_t::fetch_all() {
        if (!result) {
            if (!node_) {
                return py::list();
            }
            execute_or_throw();
        }
        if (result->is_closed()) {
            return py::list();
        }
        auto res = result->fetchall();
        result = nullptr;
        return res;
    }

    pandas_data_frame_t py_relation_t::fetch_df() {
        if (!result) {
            if (!node_) {
                return py::list();
            }
            execute_or_throw();
        }
        if (result->is_closed()) {
            return py::list();
        }
        auto res = result->fetch_df();
        result = nullptr;
        return res;
    }

    py::list py_relation_t::columns() {
        assert_relation();
        py::list res;
        for (const auto& col : schema_) {
            res.append(col.name());
        }
        return res;
    }

    py::list py_relation_t::column_types() {
        assert_relation();
        py::list res;
        for (const auto& col : schema_) {
            res.append(otterbrix_py_type_t(col.type()));
        }
        return res;
    }

    // Internal functions (not exposed to Python)
    expression_factory_t* py_relation_t::get_expression_factory() { return static_cast<expression_factory_t*>(env); }

    void py_relation_t::assert_relation() {
        if (!node_) {
            throw std::runtime_error("This relation was created from a result");
        }
    }
} // namespace otterbrix
