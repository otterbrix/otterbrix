#include "pyexpression.hpp"
#include "pyrelation.hpp"

#include <native/python_conversion.hpp>
#include <util/util.hpp>

#include <components/logical_plan/node_sort.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/key.hpp>

#include <optional>

using namespace components;

namespace otterbrix {
    PyExpression::PyExpression(Expression expr, PyConnection& conn)
        : expr(std::move(expr)), factory(&conn) {}

    PyExpression::PyExpression(Expression expr, ExpressionFactory* factory)
        : expr(std::move(expr)), factory(factory) {}

    PyExpression::~PyExpression() = default;

    pyexpr_ptr PyExpression::ColumnExpression(const string& column_name, PyConnection& conn) {
        return make_shared<PyExpression>(components::expressions::key_t(column_name), conn);
    }

    pyexpr_ptr PyExpression::ConstantExpression(const py::object& value, PyConnection& conn) {
        auto val = TransformPythonValue(value);
        return make_shared<PyExpression>(conn.MakeConstant(std::move(val)), conn);
        
    }

    pyexpr_ptr PyExpression::CountExpression(PyConnection& conn) {
        return make_shared<PyExpression>(conn.MakeCountExpression(), conn);    
    }

    string PyExpression::ToString() const {
        return factory->ConvertToString(expr); 
    }

    void PyExpression::Print() const {
        py::print(ToString());
    }        
    

    // Aggregation operations
    pyexpr_ptr PyExpression::Count() {
        return AggregationExpression(components::expressions::aggregate_type::count, *this);
    }

    pyexpr_ptr PyExpression::Sum() {
        return AggregationExpression(components::expressions::aggregate_type::sum, *this);
    }

    pyexpr_ptr PyExpression::Min() {
        return AggregationExpression(components::expressions::aggregate_type::min, *this);
    }

    pyexpr_ptr PyExpression::Max() {
        return AggregationExpression(components::expressions::aggregate_type::max, *this);
    }

    pyexpr_ptr PyExpression::Avg() {
        return AggregationExpression(components::expressions::aggregate_type::avg, *this);
    }

    pyexpr_ptr PyExpression::Round() {
        return ScalarUnaryExpression(expressions::scalar_type::round, *this);
    }

    pyexpr_ptr PyExpression::Ceil() {
        return ScalarUnaryExpression(expressions::scalar_type::ceil, *this);
    }

    pyexpr_ptr PyExpression::Floor() {
        return ScalarUnaryExpression(expressions::scalar_type::floor, *this);
    }

    pyexpr_ptr PyExpression::Abs() {
        return ScalarUnaryExpression(expressions::scalar_type::abs, *this);
    }

    pyexpr_ptr PyExpression::Negate() {
        auto value = py::int_(-1);
        auto val = TransformPythonValue(value);
        auto expr = make_shared<PyExpression>(factory->MakeConstant(std::move(val)), factory);
        return Multiply(*expr);
    }

    pyexpr_ptr PyExpression::Add(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::add, *this, other); 
    }
    
    pyexpr_ptr PyExpression::Subtract(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::subtract, *this, other); 
    }

    pyexpr_ptr PyExpression::Multiply(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::multiply, *this, other); 
    }
    
    pyexpr_ptr PyExpression::Division(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::divide, *this, other); 
    }

    pyexpr_ptr PyExpression::Modulo(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::mod, *this, other); 
    }

    pyexpr_ptr PyExpression::Power(const PyExpression &other) {
        return ScalarBinaryExpression(expressions::scalar_type::pow, *this, other); 
    }

    // Equality operations

    pyexpr_ptr PyExpression::Equality(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::eq, *this, other);
    }

    pyexpr_ptr PyExpression::Inequality(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::ne, *this, other);
    }

    pyexpr_ptr PyExpression::GreaterThan(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::gt, *this, other);
    }

    pyexpr_ptr PyExpression::GreaterThanOrEqual(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::gte, *this, other);
    }

    pyexpr_ptr PyExpression::LessThan(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::lt, *this, other);
    }

    pyexpr_ptr PyExpression::LessThanOrEqual(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::lte, *this, other);
    }
    
    pyexpr_ptr PyExpression::Regex(const PyExpression &other) {
        return ComparisonExpression(expressions::compare_type::regex, *this, other);
    }

    pyexpr_ptr PyExpression::SetAlias(const string& alias) {
        return make_shared<PyExpression>(factory->ExpressionWithAlias(expr, alias), factory);      
    }

    // AND, OR and NOT

    pyexpr_ptr PyExpression::Not() {
        return make_shared<PyExpression>(factory->ComparisonNotExpression(this->expr), factory);
    }

    pyexpr_ptr PyExpression::And(const PyExpression &other) {
        return ComparisonUnionExpression(expressions::compare_type::union_and, *this, other);
    }

    pyexpr_ptr PyExpression::Or(const PyExpression &other) {
        return ComparisonUnionExpression(expressions::compare_type::union_or, *this, other);
    }

        
    pyexpr_ptr PyExpression::Ascending() {
        return SortExpression(expressions::sort_order::asc, *this);
    }

    pyexpr_ptr PyExpression::Descending() {
        return SortExpression(expressions::sort_order::desc, *this);
    }

    // Private methods


    const Expression& PyExpression::GetExpression() {
        return expr;
    }

    pyexpr_ptr PyExpression::AggregationExpression(components::expressions::aggregate_type type, 
        const PyExpression& expr) {
        return make_shared<PyExpression>(expr.factory->AggregationUnaryExpression(type, expr.expr), expr.factory);
    }

    pyexpr_ptr PyExpression::ScalarBinaryExpression(components::expressions::scalar_type type, 
        const PyExpression& left, const PyExpression& right) {
        return make_shared<PyExpression>(left.factory->ScalarBinaryExpression(type, left.expr, right.expr), left.factory);
    }

    pyexpr_ptr PyExpression::ScalarUnaryExpression(components::expressions::scalar_type type, 
        const PyExpression& expr) {
        return make_shared<PyExpression>(expr.factory->ScalarUnaryExpression(type, expr.expr), expr.factory);
    }

    pyexpr_ptr PyExpression::ComparisonExpression(components::expressions::compare_type type, 
        const PyExpression& left, const PyExpression& right) {
        // 	auto left = left_p.GetExpression().Copy();
	    // auto right = right_p.GetExpression().Copy();
        return make_shared<PyExpression>(left.factory->ComparisonExpression(type, left.expr, right.expr), left.factory);
    
    }


    pyexpr_ptr PyExpression::ComparisonUnionExpression(expressions::compare_type type, 
            const PyExpression& left, const PyExpression& right) {
        return make_shared<PyExpression>(left.factory->ComparisonUnionExpression(type, left.expr, right.expr), left.factory);
    }

    pyexpr_ptr PyExpression::SortExpression(components::expressions::sort_order type, const PyExpression& expr) {
        //todo remove to factory
        auto sort_expr = std::visit([&type](const auto& expr) -> Expression {
            using T = std::decay_t<decltype(expr)>;
            if constexpr (std::is_same_v<T, expressions::key_t>) {
                return expressions::make_sort_expression(expr, type);
            }
            throw std::runtime_error("Undefined sort expression"); 
        }, expr.expr);
        return make_shared<PyExpression>(sort_expr, expr.factory);
        
    }

} // namespace otterbrix
