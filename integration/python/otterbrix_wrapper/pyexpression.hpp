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
        //! Chaining ctor: a derived expression stands on the SAME space and the SAME connection
        //! as the expression it was derived from. It takes the source expression rather than a
        //! bare `expression_factory_t*` because the factory pointer alone carries no lifetime --
        //! that is the hole this class had, see the members at the bottom of the class.
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
        //! The connection, refused unless it is still open. Every road below that BUILDS a new
        //! expression reaches expression_factory_t::space -- the copy close() nulls -- and
        //! `boost::intrusive_ptr::operator->` on a null space is an assert that NDEBUG deletes,
        //! leaving a plain null dereference in the build that ships. One refusal, the same one
        //! py_connection_t and py_relation_t::live_env() give.
        py_connection_t& live_env() const;

        //! The factory WITHOUT the open check: reading an expression back (to_string) touches
        //! only the key / built node / parameter map, never the space, so a closed connection
        //! must still be able to print the expressions it made. Refusing here would break that.
        expression_factory_t& factory() const;

    private:
        //! The space `expr` was allocated out of, held so the expression owns its own release
        //! path instead of borrowing the connection's -- py_relation_t::space_ and
        //! py_result_t::space are held for exactly this reason and say so.
        //!
        //! This was a raw borrow and a reachable use-after-free: the key/constant inside `expr`
        //! is pmr-allocated from the space's arena (base_otterbrix_t::resource is a MEMBER of the
        //! space), the expression is handed to Python, and the connection object that made it can
        //! be dropped first -- Python releases the locals of a finished frame in no guaranteed
        //! order. Reading the expression then read freed memory and ~py_expression_t handed the
        //! bytes back to a destroyed pool.
        //!
        //! Declared FIRST so reverse-order member destruction frees it LAST: `expr` deallocates
        //! into this arena, and `env_`'s destructor drops the connection's scratch tables through
        //! the engine. No cycle: neither py_connection_t nor otterbrix_t knows this class exists.
        boost::intrusive_ptr<otterbrix_t> space_;

        //! The connection every op reaches back into, held rather than borrowed for the same
        //! reason, and NOT covered by `space_`: a CONSTANT expression is a parameter id, and the
        //! value it names lives in `expression_factory_t::values` -- a member of this OBJECT, not
        //! of the arena. Hold only the space and the 64 constants of
        //! tests/test_expression_lifetime.py come back as IndexError while the 64 columns read
        //! fine; that is the measurement, not a guess.
        //!
        //! std::shared_ptr against rule 14, and a PARTIAL RECORD rather than a free choice:
        //! pybind11 owns py_connection_t through a shared_ptr holder (py::class_<py_connection_t,
        //! std::shared_ptr<py_connection_t>>, pyconnection/initialize.cpp), so shared_ptr IS the
        //! connection's lifetime on this boundary. py_relation_t::env holds it the same way and
        //! carries the same record.
        std::shared_ptr<py_connection_t> env_;

        expression_wrapper_t expr;
    };

} // namespace otterbrix
