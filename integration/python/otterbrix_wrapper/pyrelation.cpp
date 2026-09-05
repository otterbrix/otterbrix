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
        // The chaining input a factory call takes. Spelling the aggregate inline would
        // copy-construct the schema vector, and a std::pmr::vector's copy constructor does NOT
        // propagate the source's allocator -- a whole column schema would be rebuilt on the
        // process-wide default resource on every project/filter/sort/group/join/limit hop, while
        // the relation it was copied from keeps its arena alive through space_. Naming the source
        // vector's own resource is what makes the copy stay where the original lives.
        built_relation_t
        relation_input(const components::logical_plan::node_ptr& node,
                       const std::pmr::vector<components::table::column_definition_t>& schema) {
            return built_relation_t{node,
                                    std::pmr::vector<components::table::column_definition_t>(
                                        schema,
                                        schema.get_allocator().resource())};
        }

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

    py_relation_t::py_relation_t(std::shared_ptr<py_connection_t> env, built_relation_t rel)
        : space_(env ? env->space_ptr() : boost::intrusive_ptr<otterbrix_t>{})
        , node_(std::move(rel.node))
        , schema_(std::move(rel.columns))
        , env(std::move(env)) {
        if (!node_) {
            throw std::runtime_error("PyRelation created without a relation");
        }
        if (!this->env) {
            throw std::runtime_error("PyRelation created without a connection");
        }
        // A relation that carries a plan MUST carry the space that plan is allocated out of:
        // `node_` and `schema_` are pmr-allocated from the engine's arena and are freed in a
        // destructor, which can neither check nor refuse. Every road above refuses a closed
        // connection before it gets here, so this is the invariant they maintain rather than
        // a second opinion -- and it is what a road added later will hit instead of silently
        // handing back a relation that frees into a dead resource.
        if (!space_) {
            throw std::runtime_error("relation: the connection is closed");
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
        // No space_ and no env: this relation has no plan and no arena of its own, and the
        // result it wraps already holds the space its rows live in.
    }

    py_relation_t::~py_relation_t() {
        assert(py::gil_check());
        py::gil_scoped_release gil;
        node_.reset();
        // schema_ frees into the same arena and is destroyed after this body runs; space_
        // is destroyed after that again, so both releases still have their resource.
    }

    py_connection_t& py_relation_t::live_env() {
        if (!env) {
            throw std::runtime_error("This relation was created from a result");
        }
        env->refuse_if_closed();
        return *env;
    }

    static cursor::cursor_t_ptr
    PyExecuteRelation(const std::shared_ptr<py_connection_t>& env,
                      const logical_plan::node_ptr& node,
                      bool optimize = false) {
        assert(py::gil_check());
        py::gil_scoped_release release;
        return env->execute(node, optimize);
    }

    std::unique_ptr<py_relation_t> py_relation_t::project(const py::args& args) {
        auto& conn = live_env();
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
        return std::make_unique<py_relation_t>(env,
                                              conn.select_relation(relation_input(node_, schema_),
                                                                   std::move(fields)));
    }

    std::unique_ptr<py_relation_t> py_relation_t::filter(const py::object& condition) {
        auto& conn = live_env();
        if (py::isinstance<py::str>(condition)) {
            throw std::runtime_error("Implementation Error. Couldn\'t execute string expression");
        }
        pyexpr_ptr py_expr;
        if (!py::try_cast(condition, py_expr)) {
            throw std::runtime_error(
                "Invalid Input Exception. Please provide either a string or a PyExpression object to \'filter\'");
        }

        const auto& expr = py_expr->get_expression();
        return std::make_unique<py_relation_t>(env, conn.filter_relation(relation_input(node_, schema_), expr));
    }

    std::unique_ptr<py_relation_t> py_relation_t::order(const std::string& arg) {
        // Before get_expression_factory(): sort_expression() dereferences
        // expression_factory_t::space, the copy close() nulls, one frame further down.
        auto& conn = live_env();
        auto* factory = get_expression_factory();
        auto expr = factory->sort_expression(arg);
        return std::make_unique<py_relation_t>(env, conn.sort_relation(relation_input(node_, schema_), {expr}));
    }

    std::unique_ptr<py_relation_t> py_relation_t::sort(const py::args& args) {
        // Before get_expression_factory(), for the same reason as order() above.
        auto& conn = live_env();
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
        return std::make_unique<py_relation_t>(env,
                                              conn.sort_relation(relation_input(node_, schema_),
                                                                 std::move(order_nodes)));
    }

    std::unique_ptr<py_relation_t> py_relation_t::group(const py::args& args) {
        auto& conn = live_env();
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
        return std::make_unique<py_relation_t>(env,
                                              conn.group_relation(relation_input(node_, schema_),
                                                                  std::move(fields)));
    }

    std::unique_ptr<py_relation_t>
    py_relation_t::join(const py_relation_t& other, const py::object& condition, const std::string& type) {
        auto& conn = live_env();
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
            exprs.push_back(conn.true_expression());
        }
        return std::make_unique<py_relation_t>(
            env,
            conn.join_relation(relation_input(node_, schema_),
                               relation_input(other.node_, other.schema_),
                               exprs,
                               dtype));
    }

    std::unique_ptr<py_relation_t> py_relation_t::cross(const py_relation_t& other) {
        return join(other, py::none(), "cross");
    }

    std::unique_ptr<py_relation_t> py_relation_t::limit(int64_t count) {
        if (!node_)
            return nullptr;
        auto& conn = live_env();
        return std::make_unique<py_relation_t>(env, conn.limit_relation(relation_input(node_, schema_), count));
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
            env.get(),
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
            // The type handed to Python is a COPY of a schema type, and a copy of a nested
            // complex_logical_type keeps the SOURCE's allocator -- so a STRUCT / MAP column
            // hands out a child vector that still lives in this space's pool. The object
            // therefore has to hold the space, exactly as this relation does; built with no
            // owner at all, `rel.types` outlived the connection and read a released pool.
            res.append(otterbrix_py_type_t(space_, col.type()));
        }
        return res;
    }

    // Internal functions (not exposed to Python)
    expression_factory_t* py_relation_t::get_expression_factory() {
        return static_cast<expression_factory_t*>(env.get());
    }

    void py_relation_t::assert_relation() {
        if (!node_) {
            throw std::runtime_error("This relation was created from a result");
        }
    }
} // namespace otterbrix
