#include "pyexpression.hpp"
#include "pyrelation.hpp"
#include <memory>

#include <pybind11/pybind_wrapper.hpp>

#include <pyconnection/pyconnection.hpp>
#include <string>

namespace otterbrix {
    static void initialize_static_methods(py::module_& m) {
        const char* docs;

        // Constant expression_wrapper_t
        docs = "Create a constant expression from the provided value";
        m.def("ConstantExpression",
              &py_expression_t::constant_expression,
              py::arg("value"),
              py::arg("pyconnection"),
              docs);

        // ColumnRef expression_wrapper_t
        docs = "Create a column reference from the provided column name. `side` is one of \"left\", \"right\" or "
               "empty, used to disambiguate join conditions where both sides have the same column name.";
        m.def("ColumnExpression",
              &py_expression_t::column_expression,
              py::arg("name"),
              py::arg("pyconnection"),
              py::arg("side") = std::string{},
              docs);

        // count expression_wrapper_t
        docs = "Create a count expression for aggregation operations";
        m.def("CountExpression", &py_expression_t::count_expression, py::arg("pyconnection"), docs);
    }

    static void initialize_dunder_methods(py::class_<py_expression_t, std::shared_ptr<py_expression_t>>& m) {
        const char* docs;

        m.def("__round__", &py_expression_t::round);
        docs = R"(
			Computes the ceiling of the given value.

			Parameters:
				
			Returns:
				A column for the computed results.
		)";
        m.def("__ceil__", &py_expression_t::ceil, docs);
        m.def("__floor__", &py_expression_t::floor);
        docs = R"(
			Mathematical Function: Computes the absolute value of the given column or expression.

			Parameters:
				
			Returns:
				A new column object representing the absolute value of the input.
		)";
        m.def("__abs__", &py_expression_t::abs, docs);
        docs = R"(
    		add expr to self
    
    		Parameters:
    			expr: The expression to add together with
    
    		Returns:
    			FunctionExpression: self '+' expr
    	)";

        m.def("__add__", &py_expression_t::add, py::arg("expr"), docs);
        m.def("__radd__", &py_expression_t::add, py::arg("expr"), docs);

        docs = R"(
    		negate the expression.
    
    		Returns:
    			FunctionExpression: -self
    	)";
        m.def("__neg__", &py_expression_t::negate, docs);

        docs = R"(
    		subtract expr from self
    
    		Parameters:
    			expr: The expression to subtract from
    
    		Returns:
    			FunctionExpression: self '-' expr
    	)";
        m.def("__sub__", &py_expression_t::subtract, docs);
        m.def("__rsub__", &py_expression_t::subtract, docs);

        docs = R"(
    		multiply self by expr
    
    		Parameters:
    			expr: The expression to multiply by
    
    		Returns:
    			FunctionExpression: self '*' expr
    	)";
        m.def("__mul__", &py_expression_t::multiply, docs);
        m.def("__rmul__", &py_expression_t::multiply, docs);

        docs = R"(
    		Divide self by expr
    
    		Parameters:
    			expr: The expression to divide by
    
    		Returns:
    			FunctionExpression: self '/' expr
    	)";
        m.def("__div__", &py_expression_t::division, docs);
        m.def("__rdiv__", &py_expression_t::division, docs);

        m.def("__truediv__", &py_expression_t::division, docs);
        m.def("__rtruediv__", &py_expression_t::division, docs);

        docs = R"(
    		modulo self by expr
    
    		Parameters:
    			expr: The expression to modulo by
    
    		Returns:
    			FunctionExpression: self '%' expr
    	)";
        m.def("__mod__", &py_expression_t::modulo, docs);
        m.def("__rmod__", &py_expression_t::modulo, docs);

        docs = R"(
    		power self by expr
    
    		Parameters:
    			expr: The expression to power by
    
    		Returns:
    			FunctionExpression: self '**' expr
    	)";
        m.def("__pow__", &py_expression_t::power, docs);
        m.def("__rpow__", &py_expression_t::power, docs);

        docs = R"(
    		create an equality expression between two expressions
    
    		Parameters:
    			expr: The expression to check equality with
    
    		Returns:
    			FunctionExpression: self '=' expr
    	)";

        docs = R"(
            create an equality expression between two expressions

            Parameters:
                expr: The expression to check equality with

            Returns:
                FunctionExpression: self '=' expr
        )";
        m.def("__eq__", &py_expression_t::equality, docs);

        docs = R"(
            create an inequality expression between two expressions

            Parameters:
                expr: The expression to check inequality with

            Returns:
                FunctionExpression: self '!=' expr
        )";
        m.def("__ne__", &py_expression_t::inequality, docs);

        docs = R"(
            create a greater than expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '>' expr
        )";
        m.def("__gt__", &py_expression_t::greater_than, docs);

        docs = R"(
            create a greater than or equal expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '>=' expr
        )";
        m.def("__ge__", &py_expression_t::greater_than_or_equal, docs);

        docs = R"(
            create a less than expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '<' expr
        )";
        m.def("__lt__", &py_expression_t::less_than, docs);

        docs = R"(
            create a less than or equal expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '<=' expr
        )";
        m.def("__le__", &py_expression_t::less_than_or_equal, docs);

        docs = R"(
            A rlike expression based on a SQL REGEX match

            Parameters:
                expr: The std::string and the pattern

            Returns:
                FunctionExpression: selt REGEX pattern
        )";
        m.def("rlike", &py_expression_t::regex, docs);

        m.def("__and__", &py_expression_t::and_, docs);

        docs = R"(
    		Binary-or self together with expr
    
    		Parameters:
    			expr: The expression to OR together with self
    
    		Returns:
    			FunctionExpression: self '|' expr
    	)";
        m.def("__or__", &py_expression_t::or_, docs);

        docs = R"(
    		create a binary-not expression from self
    
    		Returns:
    			FunctionExpression: ~self
    	)";
        m.def("__invert__", &py_expression_t::not_, docs);

        docs = R"(
    		Binary-and self together with expr
    
    		Parameters:
    			expr: The expression to AND together with self
    
    		Returns:
    			FunctionExpression: expr '&' self
    	)";
        m.def("__rand__", &py_expression_t::and_, docs);

        docs = R"(
    		Binary-or self together with expr
    
    		Parameters:
    			expr: The expression to OR together with self
    
    		Returns:
    			FunctionExpression: expr '|' self
    	)";
        m.def("__ror__", &py_expression_t::or_, docs);
    }

    static void initialize_implicit_conversion(py::class_<py_expression_t, std::shared_ptr<py_expression_t>>& /*m*/) {}
    void py_expression_t::initialize(py::module_& m) {
        auto expression =
            py::class_<py_expression_t, std::shared_ptr<py_expression_t>>(m, "Expression", py::module_local());
        initialize_static_methods(m);
        initialize_dunder_methods(expression);
        initialize_implicit_conversion(expression);
        const char* docs;

        docs = R"(
    		print the stringified version of the expression.
    	)";
        expression.def("show", &py_expression_t::print, docs);

        docs = R"(
    		set the order by modifier to ASCENDING.
    	)";
        expression.def("asc", &py_expression_t::ascending, docs);

        docs = R"(
    		set the order by modifier to DESCENDING.
    	)";
        expression.def("desc", &py_expression_t::descending, docs);

        docs = R"(
     		Return the stringified version of the expression.
     
     		Returns:
     			str: The std::string representation.
     	)";
        expression.def("__repr__", &py_expression_t::to_string, docs);

        docs = R"(
     		create a copy of this expression with the given alias.
     
     		Parameters:
     			name: The alias to use for the expression, this will affect how it can be referenced.
     
     		Returns:
     			expression_wrapper_t: self with an alias.
     	)";
        expression.def("alias", &py_expression_t::set_alias, docs);

        expression.def("count", &py_expression_t::count);
        expression.def("sum", &py_expression_t::sum);
        expression.def("min", &py_expression_t::min);
        expression.def("max", &py_expression_t::max);
        expression.def("avg", &py_expression_t::avg);
    }
} // namespace otterbrix
