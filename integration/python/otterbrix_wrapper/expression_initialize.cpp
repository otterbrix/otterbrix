#include "pyexpression.hpp"
#include "pyrelation.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <pyconnection/pyconnection.hpp>

namespace otterbrix {
    static void InitializeStaticMethods(py::module_ &m) {
	    const char *docs;

        // Constant Expression
	    docs = "Create a constant expression from the provided value";
	    m.def("ConstantExpression", &PyExpression::ConstantExpression, 
            py::arg("value"), py::arg("pyconnection"), docs);
        
	    // ColumnRef Expression
	    docs = "Create a column reference from the provided column name";
	    m.def("ColumnExpression", &PyExpression::ColumnExpression, py::arg("name"), py::arg("pyconnection"), docs);
       
        // Count Expression 
        docs = "Create a count expression for aggregation operations";
        m.def("CountExpression", &PyExpression::CountExpression, py::arg("pyconnection"), docs);
    }

    static void InitializeDunderMethods(py::class_<PyExpression, shared_ptr<PyExpression>> &m) {
	    const char *docs;

		m.def("__round__", &PyExpression::Round);
		docs = R"(
			Computes the ceiling of the given value.

			Parameters:
				
			Returns:
				A column for the computed results.
		)";
        m.def("__ceil__", &PyExpression::Ceil, docs);
        m.def("__floor__", &PyExpression::Floor);
		docs = R"(
			Mathematical Function: Computes the absolute value of the given column or expression.

			Parameters:
				
			Returns:
				A new column object representing the absolute value of the input.
		)";
		m.def("__abs__",&PyExpression::Abs, docs);
     	docs = R"(
    		Add expr to self
    
    		Parameters:
    			expr: The expression to add together with
    
    		Returns:
    			FunctionExpression: self '+' expr
    	)";
    
    	m.def("__add__", &PyExpression::Add, py::arg("expr"), docs);
    	m.def("__radd__", &PyExpression::Add, py::arg("expr"), docs);
    
    	docs = R"(
    		Negate the expression.
    
    		Returns:
    			FunctionExpression: -self
    	)";
    	m.def("__neg__", &PyExpression::Negate, docs);
    
    	docs = R"(
    		Subtract expr from self
    
    		Parameters:
    			expr: The expression to subtract from
    
    		Returns:
    			FunctionExpression: self '-' expr
    	)";
    	m.def("__sub__", &PyExpression::Subtract, docs);
    	m.def("__rsub__", &PyExpression::Subtract, docs);
    
    	docs = R"(
    		Multiply self by expr
    
    		Parameters:
    			expr: The expression to multiply by
    
    		Returns:
    			FunctionExpression: self '*' expr
    	)";
    	m.def("__mul__", &PyExpression::Multiply, docs);
    	m.def("__rmul__", &PyExpression::Multiply, docs);
    
    	docs = R"(
    		Divide self by expr
    
    		Parameters:
    			expr: The expression to divide by
    
    		Returns:
    			FunctionExpression: self '/' expr
    	)";
    	m.def("__div__", &PyExpression::Division, docs);
    	m.def("__rdiv__", &PyExpression::Division, docs);
    
    	m.def("__truediv__", &PyExpression::Division, docs);
    	m.def("__rtruediv__", &PyExpression::Division, docs);
    
    	docs = R"(
    		Modulo self by expr
    
    		Parameters:
    			expr: The expression to modulo by
    
    		Returns:
    			FunctionExpression: self '%' expr
    	)";
    	m.def("__mod__", &PyExpression::Modulo, docs);
    	m.def("__rmod__", &PyExpression::Modulo, docs);
    
    	docs = R"(
    		Power self by expr
    
    		Parameters:
    			expr: The expression to power by
    
    		Returns:
    			FunctionExpression: self '**' expr
    	)";
    	m.def("__pow__", &PyExpression::Power, docs);
    	m.def("__rpow__", &PyExpression::Power, docs);
    
    	docs = R"(
    		Create an equality expression between two expressions
    
    		Parameters:
    			expr: The expression to check equality with
    
    		Returns:
    			FunctionExpression: self '=' expr
    	)";
       
        docs = R"(
            Create an equality expression between two expressions

            Parameters:
                expr: The expression to check equality with

            Returns:
                FunctionExpression: self '=' expr
        )";
        m.def("__eq__", &PyExpression::Equality, docs);

        docs = R"(
            Create an inequality expression between two expressions

            Parameters:
                expr: The expression to check inequality with

            Returns:
                FunctionExpression: self '!=' expr
        )";
        m.def("__ne__", &PyExpression::Inequality, docs);

        docs = R"(
            Create a greater than expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '>' expr
        )";
        m.def("__gt__", &PyExpression::GreaterThan, docs);

        docs = R"(
            Create a greater than or equal expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '>=' expr
        )";
        m.def("__ge__", &PyExpression::GreaterThanOrEqual, docs);

        docs = R"(
            Create a less than expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '<' expr
        )";
        m.def("__lt__", &PyExpression::LessThan, docs);

        docs = R"(
            Create a less than or equal expression between two expressions

            Parameters:
                expr: The expression to check

            Returns:
                FunctionExpression: self '<=' expr
        )";
        m.def("__le__", &PyExpression::LessThanOrEqual, docs);

        docs = R"(
            A rlike expression based on a SQL REGEX match

            Parameters:
                expr: The string and the pattern

            Returns:
                FunctionExpression: selt REGEX pattern
        )";
        m.def("rlike", &PyExpression::Regex, docs);

    	m.def("__and__", &PyExpression::And, docs);
    
    	docs = R"(
    		Binary-or self together with expr
    
    		Parameters:
    			expr: The expression to OR together with self
    
    		Returns:
    			FunctionExpression: self '|' expr
    	)";
    	m.def("__or__", &PyExpression::Or, docs);
    
    	docs = R"(
    		Create a binary-not expression from self
    
    		Returns:
    			FunctionExpression: ~self
    	)";
    	m.def("__invert__", &PyExpression::Not, docs);
    
    	docs = R"(
    		Binary-and self together with expr
    
    		Parameters:
    			expr: The expression to AND together with self
    
    		Returns:
    			FunctionExpression: expr '&' self
    	)";
    	m.def("__rand__", &PyExpression::And, docs);
    
    	docs = R"(
    		Binary-or self together with expr
    
    		Parameters:
    			expr: The expression to OR together with self
    
    		Returns:
    			FunctionExpression: expr '|' self
    	)";
    	m.def("__ror__", &PyExpression::Or, docs);
    }
    
    static void InitializeImplicitConversion(py::class_<PyExpression, shared_ptr<PyExpression>> &m) {
    	// m.def(py::init<>([](const string &name) { return PyExpression::ColumnExpression(name); }));
        /*m.def(py::init<>([](const py::object &obj) {
    		auto val = TransformPythonValue(obj);
    		return PyExpression::InternalConstantExpression(std::move(val));
    	}));*/
    	// py::implicitly_convertible<py::str, PyExpression>();
    	//py::implicitly_convertible<py::object, PyExpression>();
    }
    void PyExpression::Initialize(py::module_ &m) {
        auto expression =
	        py::class_<PyExpression, shared_ptr<PyExpression>>(m, "Expression", py::module_local());
        InitializeStaticMethods(m);
        InitializeDunderMethods(expression);
	    InitializeImplicitConversion(expression);
    	const char *docs;
    
    	docs = R"(
    		Print the stringified version of the expression.
    	)";
    	expression.def("show", &PyExpression::Print, docs);
    
    	docs = R"(
    		Set the order by modifier to ASCENDING.
    	)";
    	expression.def("asc", &PyExpression::Ascending, docs);
    
    	docs = R"(
    		Set the order by modifier to DESCENDING.
    	)";
    	expression.def("desc", &PyExpression::Descending, docs);

     	docs = R"(
     		Return the stringified version of the expression.
     
     		Returns:
     			str: The string representation.
     	)";
     	expression.def("__repr__", &PyExpression::ToString, docs);
     
     	docs = R"(
     		Create a copy of this expression with the given alias.
     
     		Parameters:
     			name: The alias to use for the expression, this will affect how it can be referenced.
     
     		Returns:
     			Expression: self with an alias.
     	)";
     	expression.def("alias", &PyExpression::SetAlias, docs);

		expression.def("count", &PyExpression::Count);
        expression.def("sum", &PyExpression::Sum);
        expression.def("min", &PyExpression::Min);
        expression.def("max", &PyExpression::Max);
        expression.def("avg", &PyExpression::Avg);

    }
} // namespace otterbrix
