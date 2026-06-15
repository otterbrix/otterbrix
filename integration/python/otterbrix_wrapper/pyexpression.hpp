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

    class py_expression_t;

    //! R14: std::shared_ptr<py_expression_t> is MANDATED here -- it is the pybind11 holder type registered
    //! in expression_initialize.cpp as py::class_<py_expression_t, std::shared_ptr<py_expression_t>>. It is not
    //! a free-form internal ownership choice and cannot be replaced without breaking the binding.
    using pyexpr_ptr = std::shared_ptr<py_expression_t>;

    //! The former std::enable_shared_from_this<py_expression_t> base was removed: py_expression_t never calls
    //! shared_from_this(); all instances are produced via make_shared and handed to pybind as the
    //! holder above.
    class py_expression_t {
    public:
        py_expression_t(expression_wrapper_t expr, py_connection_t& conn);
        py_expression_t(expression_wrapper_t expr, expression_factory_t* factory);

        ~py_expression_t();
        static void initialize(py::module_ &m);

        static pyexpr_ptr column_expression(const std::string& column_name, py_connection_t& conn, const std::string& side = "");

        static pyexpr_ptr constant_expression(const py::object& value, py_connection_t& conn);
        
        static pyexpr_ptr count_expression(py_connection_t& conn);


    public:

        std::string to_string() const;
        void print() const;        

        // Aggregation operations
        pyexpr_ptr count();
        pyexpr_ptr sum();
        pyexpr_ptr min();
        pyexpr_ptr max();
        pyexpr_ptr avg();

        // Scalar operations
        pyexpr_ptr round();
        pyexpr_ptr ceil();
        pyexpr_ptr floor();
        pyexpr_ptr abs();
        pyexpr_ptr negate();

        pyexpr_ptr add(const py_expression_t &other);
        pyexpr_ptr subtract(const py_expression_t &other);
        pyexpr_ptr multiply(const py_expression_t &other);
        pyexpr_ptr division(const py_expression_t &other);
        pyexpr_ptr modulo(const py_expression_t &other);
        pyexpr_ptr power(const py_expression_t &other);

        // equality operations
        pyexpr_ptr equality(const py_expression_t &other);
        pyexpr_ptr inequality(const py_expression_t &other);
        pyexpr_ptr greater_than(const py_expression_t &other);
        pyexpr_ptr greater_than_or_equal(const py_expression_t &other);
        pyexpr_ptr less_than(const py_expression_t &other);
        pyexpr_ptr less_than_or_equal(const py_expression_t &other);

        pyexpr_ptr regex(const py_expression_t &other);

        pyexpr_ptr set_alias(const std::string& alias);
    
        // AND, OR and NOT

        pyexpr_ptr not_();
        pyexpr_ptr and_(const py_expression_t &other);
        pyexpr_ptr or_(const py_expression_t &other);

        pyexpr_ptr ascending();
        pyexpr_ptr descending();

    public:
	    // Internal functions (not exposed to Python)
        
        const expression_wrapper_t& get_expression();

        static pyexpr_ptr aggregation_expression(const std::string& function_name,
            const py_expression_t& expr);

        static pyexpr_ptr scalar_binary_expression(components::expressions::scalar_type type, 
            const py_expression_t& left, const py_expression_t& right);

        static pyexpr_ptr scalar_unary_expression(components::expressions::scalar_type type, 
            const py_expression_t& expr);

        static pyexpr_ptr comparison_expression(components::expressions::compare_type type, 
            const py_expression_t& left, const py_expression_t& right);

        static pyexpr_ptr comparison_union_expression(components::expressions::compare_type type, 
            const py_expression_t& left, const py_expression_t& right);

        static pyexpr_ptr sort_expression(components::expressions::sort_order type, const py_expression_t& expr);
    private:
        expression_wrapper_t expr;
        expression_factory_t* factory;
    };


} // namespace otterbrix
