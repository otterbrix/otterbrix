#pragma once

#include <pyconnection/pyconnection.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

#include <memory>
#include <string>


namespace otterbrix {

    class PyExpression;

    //! R14: std::shared_ptr<PyExpression> is MANDATED here -- it is the pybind11 holder type registered
    //! in expression_initialize.cpp as py::class_<PyExpression, std::shared_ptr<PyExpression>>. It is not
    //! a free-form internal ownership choice and cannot be replaced without breaking the binding.
    using pyexpr_ptr = std::shared_ptr<PyExpression>;

    //! The former std::enable_shared_from_this<PyExpression> base was removed: PyExpression never calls
    //! shared_from_this(); all instances are produced via make_shared and handed to pybind as the
    //! holder above.
    class PyExpression {
    public:
        PyExpression(Expression expr, PyConnection& conn);
        PyExpression(Expression expr, ExpressionFactory* factory);

        ~PyExpression();
        static void Initialize(py::module_ &m);

        static pyexpr_ptr ColumnExpression(const std::string& column_name, PyConnection& conn, const std::string& side = "");

        static pyexpr_ptr ConstantExpression(const py::object& value, PyConnection& conn);
        
        static pyexpr_ptr CountExpression(PyConnection& conn);


    public:

        std::string ToString() const;
        void Print() const;        

        // Aggregation operations
        pyexpr_ptr Count();
        pyexpr_ptr Sum();
        pyexpr_ptr Min();
        pyexpr_ptr Max();
        pyexpr_ptr Avg();

        // Scalar operations
        pyexpr_ptr Round();
        pyexpr_ptr Ceil();
        pyexpr_ptr Floor();
        pyexpr_ptr Abs();
        pyexpr_ptr Negate();

        pyexpr_ptr Add(const PyExpression &other);
        pyexpr_ptr Subtract(const PyExpression &other);
        pyexpr_ptr Multiply(const PyExpression &other);
        pyexpr_ptr Division(const PyExpression &other);
        pyexpr_ptr Modulo(const PyExpression &other);
        pyexpr_ptr Power(const PyExpression &other);

        // Equality operations
        pyexpr_ptr Equality(const PyExpression &other);
        pyexpr_ptr Inequality(const PyExpression &other);
        pyexpr_ptr GreaterThan(const PyExpression &other);
        pyexpr_ptr GreaterThanOrEqual(const PyExpression &other);
        pyexpr_ptr LessThan(const PyExpression &other);
        pyexpr_ptr LessThanOrEqual(const PyExpression &other);

        pyexpr_ptr Regex(const PyExpression &other);

        pyexpr_ptr SetAlias(const std::string& alias);
    
        // AND, OR and NOT

        pyexpr_ptr Not();
        pyexpr_ptr And(const PyExpression &other);
        pyexpr_ptr Or(const PyExpression &other);

        pyexpr_ptr Ascending();
        pyexpr_ptr Descending();

    public:
	    // Internal functions (not exposed to Python)
        
        const Expression& GetExpression();

        static pyexpr_ptr AggregationExpression(const std::string& function_name,
            const PyExpression& expr);

        static pyexpr_ptr ScalarBinaryExpression(components::expressions::scalar_type type, 
            const PyExpression& left, const PyExpression& right);

        static pyexpr_ptr ScalarUnaryExpression(components::expressions::scalar_type type, 
            const PyExpression& expr);

        static pyexpr_ptr ComparisonExpression(components::expressions::compare_type type, 
            const PyExpression& left, const PyExpression& right);

        static pyexpr_ptr ComparisonUnionExpression(components::expressions::compare_type type, 
            const PyExpression& left, const PyExpression& right);

        static pyexpr_ptr SortExpression(components::expressions::sort_order type, const PyExpression& expr);
    private:
        Expression expr;
        ExpressionFactory* factory;
    };


} // namespace otterbrix
