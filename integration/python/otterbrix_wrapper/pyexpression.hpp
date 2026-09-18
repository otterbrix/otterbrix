#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pyconnection/pyconnection.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

#include <integration/cpp/otterbrix.hpp>

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
        //! Chaining ctor: takes the source expression, not a bare `expression_factory_t*`,
        //! since a factory pointer alone carries no lifetime (see the members below).
        py_expression_t(expression_wrapper_t expr, const py_expression_t& source);

        ~py_expression_t();
        static void initialize(py::module_& m);

        static pyexpr_ptr
        column_expression(const std::string& column_name, py_connection_t& conn, const std::string& side = "");

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

        pyexpr_ptr add(const py_expression_t& other);
        pyexpr_ptr subtract(const py_expression_t& other);
        pyexpr_ptr multiply(const py_expression_t& other);
        pyexpr_ptr division(const py_expression_t& other);
        pyexpr_ptr modulo(const py_expression_t& other);
        pyexpr_ptr power(const py_expression_t& other);

        // equality operations
        pyexpr_ptr equality(const py_expression_t& other);
        pyexpr_ptr inequality(const py_expression_t& other);
        pyexpr_ptr greater_than(const py_expression_t& other);
        pyexpr_ptr greater_than_or_equal(const py_expression_t& other);
        pyexpr_ptr less_than(const py_expression_t& other);
        pyexpr_ptr less_than_or_equal(const py_expression_t& other);

        pyexpr_ptr regex(const py_expression_t& other);

        pyexpr_ptr set_alias(const std::string& alias);

        // AND, OR and NOT

        pyexpr_ptr not_();
        pyexpr_ptr and_(const py_expression_t& other);
        pyexpr_ptr or_(const py_expression_t& other);

        pyexpr_ptr ascending();
        pyexpr_ptr descending();

    public:
        // Internal functions (not exposed to Python)

        const expression_wrapper_t& get_expression();

        static pyexpr_ptr aggregation_expression(const std::string& function_name, const py_expression_t& expr);

        static pyexpr_ptr scalar_binary_expression(components::expressions::scalar_type type,
                                                   const py_expression_t& left,
                                                   const py_expression_t& right);

        static pyexpr_ptr scalar_unary_expression(components::expressions::scalar_type type,
                                                  const py_expression_t& expr);

        static pyexpr_ptr comparison_expression(components::expressions::compare_type type,
                                                const py_expression_t& left,
                                                const py_expression_t& right);

        static pyexpr_ptr comparison_union_expression(components::expressions::compare_type type,
                                                      const py_expression_t& left,
                                                      const py_expression_t& right);

        static pyexpr_ptr sort_expression(components::expressions::sort_order type, const py_expression_t& expr);

    private:
        //! Refuses unless still open: building a new expression dereferences the (possibly
        //! null, after close()) space, which under NDEBUG is a plain null dereference, not an
        //! assert. Same refusal as py_connection_t/py_relation_t::live_env().
        py_connection_t& live_env() const;

        //! Without the open check: to_string() never touches the space, so a closed
        //! connection must still be able to print the expressions it made.
        expression_factory_t& factory() const;

    private:
        //! The space `expr` was allocated out of, held instead of borrowed (same reason as
        //! py_relation_t::space_ / py_result_t::space): `expr`'s pmr data lives on the space's
        //! arena, and Python can drop the connection before this expression -- a reachable
        //! use-after-free when it was a raw borrow. Declared first so reverse-order member
        //! destruction frees it last, after `expr` has deallocated into it. No ref cycle:
        //! neither py_connection_t nor otterbrix_t knows this class exists.
        boost::intrusive_ptr<otterbrix_t> space_;

        //! Held rather than borrowed, and not covered by `space_`: a CONSTANT expression is a
        //! parameter id whose value lives in `expression_factory_t::values`, a member of the
        //! connection object, not the arena. Measured: holding only the space made
        //! tests/test_expression_lifetime.py's 64 constants come back as IndexError while its
        //! 64 columns read fine. std::shared_ptr because pybind11 owns py_connection_t through
        //! a shared_ptr holder (pyconnection/initialize.cpp) -- that IS its lifetime here;
        //! py_relation_t::env holds it the same way.
        std::shared_ptr<py_connection_t> env_;

        expression_wrapper_t expr;
    };

} // namespace otterbrix
