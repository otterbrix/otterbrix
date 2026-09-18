#include "pyexpression.hpp"
#include "pyrelation.hpp"
#include <memory>

#include <native/python_conversion.hpp>
#include <util/util.hpp>

#include <components/expressions/key.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_sort.hpp>

#include <optional>
#include <stdexcept>
#include <string>

using namespace components;

namespace otterbrix {

    namespace {
        // R14 boundary: the expression factory now reports invalid arguments via
        // core::result_wrapper_t. Surface those to Python as exceptions (mirrors
        // connection_environment.cpp's transform-result handling).
        expression_wrapper_t unwrap(core::result_wrapper_t<expression_wrapper_t>&& result) {
            if (result.has_error()) {
                throw std::runtime_error(std::string(result.error().what));
            }
            return std::move(result.value());
        }
    } // namespace
    // space_/env_ own their own copies (not borrowed from `conn`): the space's arena
    // (base_otterbrix_t::resource) must outlive `expr`, even if Python drops `conn` first.
    // See the member comments in pyexpression.hpp.
    py_expression_t::py_expression_t(expression_wrapper_t expr, py_connection_t& conn)
        : space_(conn.space_ptr())
        , env_(conn.shared_from_this())
        , expr(std::move(expr)) {
        // Unreachable from the static entry points below (each refuses a closed
        // connection first), but a null space here means the expression stands on an arena
        // nobody holds, and nothing above dereferences it before this check runs.
        if (!space_) {
            throw std::runtime_error("expression: the connection is closed");
        }
    }

    py_expression_t::py_expression_t(expression_wrapper_t expr, const py_expression_t& source)
        : space_(source.space_)
        , env_(source.env_)
        , expr(std::move(expr)) {}

    py_expression_t::~py_expression_t() = default;

    py_connection_t& py_expression_t::live_env() const {
        env_->refuse_if_closed();
        return *env_;
    }

    expression_factory_t& py_expression_t::factory() const { return *env_; }

    // refuse_if_closed() first, because a closed connection has no space to allocate the key
    // out of -- the same refusal every other door of this class gives.
    pyexpr_ptr
    py_expression_t::column_expression(const std::string& column_name, py_connection_t& conn, const std::string& side) {
        auto side_val = components::expressions::side_t::undefined;
        if (side == "left") {
            side_val = components::expressions::side_t::left;
        } else if (side == "right") {
            side_val = components::expressions::side_t::right;
        }
        conn.refuse_if_closed();
        return std::make_shared<py_expression_t>(
            components::expressions::key_t(conn.space_ptr()->dispatcher()->resource(), column_name, side_val),
            conn);
    }

    pyexpr_ptr py_expression_t::constant_expression(const py::object& value, py_connection_t& conn) {
        conn.refuse_if_closed();
        auto val = transform_python_value(conn.space_ptr()->dispatcher()->resource(), value);
        if (val.has_error()) {
            throw std::runtime_error(std::string(val.error().what));
        }
        return std::make_shared<py_expression_t>(conn.make_constant(std::move(val.value())), conn);
    }

    pyexpr_ptr py_expression_t::count_expression(py_connection_t& conn) {
        // The third door, and it was the one left unlocked: make_count_expression() dereferences
        // expression_factory_t::space, which close() nulls, one frame down.
        conn.refuse_if_closed();
        return std::make_shared<py_expression_t>(conn.make_count_expression(), conn);
    }

    // No open check: convert_to_string touches none of the space, so an expression stays
    // printable after its connection closes (`space_` keeps the bytes alive).
    std::string py_expression_t::to_string() const {
        auto result = factory().convert_to_string(expr);
        if (result.has_error()) {
            throw std::runtime_error(std::string(result.error().what));
        }
        return std::move(result.value());
    }

    void py_expression_t::print() const { py::print(to_string()); }

    // Aggregation operations
    pyexpr_ptr py_expression_t::count() { return aggregation_expression("count", *this); }

    pyexpr_ptr py_expression_t::sum() { return aggregation_expression("sum", *this); }

    pyexpr_ptr py_expression_t::min() { return aggregation_expression("min", *this); }

    pyexpr_ptr py_expression_t::max() { return aggregation_expression("max", *this); }

    pyexpr_ptr py_expression_t::avg() { return aggregation_expression("avg", *this); }

    pyexpr_ptr py_expression_t::round() { return scalar_unary_expression(expressions::scalar_type::round, *this); }

    pyexpr_ptr py_expression_t::ceil() { return scalar_unary_expression(expressions::scalar_type::ceil, *this); }

    pyexpr_ptr py_expression_t::floor() { return scalar_unary_expression(expressions::scalar_type::floor, *this); }

    pyexpr_ptr py_expression_t::abs() { return scalar_unary_expression(expressions::scalar_type::abs, *this); }

    pyexpr_ptr py_expression_t::negate() {
        // Live_env() first, so the -1 below allocates from a connection that still
        // has a space, rather than surfacing a closed connection inside multiply().
        auto& conn = live_env();
        auto value = py::int_(-1);
        auto val = transform_python_value(conn.space_ptr()->dispatcher()->resource(), value);
        if (val.has_error()) {
            throw std::runtime_error(std::string(val.error().what));
        }
        auto expr = std::make_shared<py_expression_t>(conn.make_constant(std::move(val.value())), *this);
        return multiply(*expr);
    }

    pyexpr_ptr py_expression_t::add(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::add, *this, other);
    }

    pyexpr_ptr py_expression_t::subtract(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::subtract, *this, other);
    }

    pyexpr_ptr py_expression_t::multiply(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::multiply, *this, other);
    }

    pyexpr_ptr py_expression_t::division(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::divide, *this, other);
    }

    pyexpr_ptr py_expression_t::modulo(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::mod, *this, other);
    }

    pyexpr_ptr py_expression_t::power(const py_expression_t& other) {
        return scalar_binary_expression(expressions::scalar_type::pow, *this, other);
    }

    // equality operations

    pyexpr_ptr py_expression_t::equality(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::eq, *this, other);
    }

    pyexpr_ptr py_expression_t::inequality(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::ne, *this, other);
    }

    pyexpr_ptr py_expression_t::greater_than(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::gt, *this, other);
    }

    pyexpr_ptr py_expression_t::greater_than_or_equal(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::gte, *this, other);
    }

    pyexpr_ptr py_expression_t::less_than(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::lt, *this, other);
    }

    pyexpr_ptr py_expression_t::less_than_or_equal(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::lte, *this, other);
    }

    pyexpr_ptr py_expression_t::regex(const py_expression_t& other) {
        return comparison_expression(expressions::compare_type::regex, *this, other);
    }

    pyexpr_ptr py_expression_t::set_alias(const std::string& alias) {
        return std::make_shared<py_expression_t>(unwrap(live_env().expression_with_alias(expr, alias)), *this);
    }

    // AND, OR and NOT

    pyexpr_ptr py_expression_t::not_() {
        return std::make_shared<py_expression_t>(unwrap(live_env().comparison_not_expression(this->expr)), *this);
    }

    pyexpr_ptr py_expression_t::and_(const py_expression_t& other) {
        return comparison_union_expression(expressions::compare_type::union_and, *this, other);
    }

    pyexpr_ptr py_expression_t::or_(const py_expression_t& other) {
        return comparison_union_expression(expressions::compare_type::union_or, *this, other);
    }

    pyexpr_ptr py_expression_t::ascending() { return sort_expression(expressions::sort_order::asc, *this); }

    pyexpr_ptr py_expression_t::descending() { return sort_expression(expressions::sort_order::desc, *this); }

    // Private methods

    const expression_wrapper_t& py_expression_t::get_expression() { return expr; }

    pyexpr_ptr py_expression_t::aggregation_expression(const std::string& function_name, const py_expression_t& expr) {
        return std::make_shared<py_expression_t>(
            unwrap(expr.live_env().aggregation_unary_expression(function_name, expr.expr)),
            expr);
    }

    pyexpr_ptr py_expression_t::scalar_binary_expression(components::expressions::scalar_type type,
                                                         const py_expression_t& left,
                                                         const py_expression_t& right) {
        return std::make_shared<py_expression_t>(
            unwrap(left.live_env().scalar_binary_expression(type, left.expr, right.expr)),
            left);
    }

    pyexpr_ptr py_expression_t::scalar_unary_expression(components::expressions::scalar_type type,
                                                        const py_expression_t& expr) {
        return std::make_shared<py_expression_t>(expr.live_env().scalar_unary_expression(type, expr.expr), expr);
    }

    pyexpr_ptr py_expression_t::comparison_expression(components::expressions::compare_type type,
                                                      const py_expression_t& left,
                                                      const py_expression_t& right) {
        return std::make_shared<py_expression_t>(
            unwrap(left.live_env().comparison_expression(type, left.expr, right.expr)),
            left);
    }

    pyexpr_ptr py_expression_t::comparison_union_expression(expressions::compare_type type,
                                                            const py_expression_t& left,
                                                            const py_expression_t& right) {
        return std::make_shared<py_expression_t>(
            unwrap(left.live_env().comparison_union_expression(type, left.expr, right.expr)),
            left);
    }

    pyexpr_ptr py_expression_t::sort_expression(components::expressions::sort_order type, const py_expression_t& expr) {
        return std::make_shared<py_expression_t>(unwrap(expr.live_env().sort_expression(expr.expr, type)), expr);
    }

} // namespace otterbrix
