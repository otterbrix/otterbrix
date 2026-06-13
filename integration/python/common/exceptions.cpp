#include "exceptions.hpp"

#include <stdexcept>
#include <core/types/string.hpp>

namespace py = pybind11;

namespace otterbrix {
/**
 * @see https://peps.python.org/pep-0249/#exceptions
 */
void RegisterExceptions(const py::module_ &m) {
	// The base class is mapped to Error in python to somewhat match the DBAPI 2.0 specifications
	py::register_exception<Warning>(m, "Warning");
	auto error = py::register_exception<PyError>(m, "Error").ptr();
	auto db_error = py::register_exception<DatabaseError>(m, "DatabaseError", error).ptr();

	// order of declaration matters, and this needs to be checked last
	// Unknown
	py::register_exception<PyFatalException>(m, "FatalException", db_error);
	py::register_exception<PyInterruptException>(m, "InterruptException", db_error);
	py::register_exception<PyPermissionException>(m, "PermissionException", db_error);
	py::register_exception<PySequenceException>(m, "SequenceException", db_error);
	py::register_exception<PyDependencyException>(m, "DependencyException", db_error);

	// DataError
	auto data_error = py::register_exception<DataError>(m, "DataError", db_error).ptr();
	py::register_exception<PyOutOfRangeException>(m, "OutOfRangeException", data_error);
	py::register_exception<PyConversionException>(m, "ConversionException", data_error);
	// no unknown type error, or decimal type
	py::register_exception<PyTypeMismatchException>(m, "TypeMismatchException", data_error);

	// OperationalError
	auto operational_error = py::register_exception<OperationalError>(m, "OperationalError", db_error).ptr();
	py::register_exception<PyTransactionException>(m, "TransactionException", operational_error);
	py::register_exception<PyOutOfMemoryException>(m, "OutOfMemoryException", operational_error);
	py::register_exception<PyConnectionException>(m, "ConnectionException", operational_error);
	// no object size error
	// no null pointer errors
	auto io_exception = py::register_exception<PyIOException>(m, "IOException", operational_error).ptr();
	py::register_exception<PySerializationException>(m, "SerializationException", operational_error);

	static py::exception<PyHTTPException> HTTP_EXCEPTION(m, "HTTPException", io_exception);
	const auto string_type = py::type::of(py::str());
	const auto Dict = py::module_::import("typing").attr("Dict");
	HTTP_EXCEPTION.attr("__annotations__") =
	    py::dict(py::arg("status_code") = py::type::of(py::int_()), py::arg("body") = string_type,
	             py::arg("reason") = string_type, py::arg("headers") = Dict[py::make_tuple(string_type, string_type)]);
	HTTP_EXCEPTION.doc() = "Thrown when an error occurs in the httpfs extension, or whilst downloading an extension.";

	// IntegrityError
	auto integrity_error = py::register_exception<IntegrityError>(m, "IntegrityError", db_error).ptr();
	py::register_exception<PyConstraintException>(m, "ConstraintException", integrity_error);

	// InternalError
	auto internal_error = py::register_exception<InternalError>(m, "InternalError", db_error).ptr();
	py::register_exception<PyInternalException>(m, "InternalException", internal_error);

	//// ProgrammingError
	auto programming_error = py::register_exception<ProgrammingError>(m, "ProgrammingError", db_error).ptr();
	py::register_exception<PyParserException>(m, "ParserException", programming_error);
	py::register_exception<PySyntaxException>(m, "SyntaxException", programming_error);
	py::register_exception<PyBinderException>(m, "BinderException", programming_error);
	py::register_exception<PyInvalidInputException>(m, "InvalidInputException", programming_error);
	py::register_exception<PyInvalidTypeException>(m, "InvalidTypeException", programming_error);
	// no type for expression exceptions?
	py::register_exception<PyCatalogException>(m, "CatalogException", programming_error);

	// NotSupportedError
	auto not_supported_error = py::register_exception<NotSupportedError>(m, "NotSupportedError", db_error).ptr();
	py::register_exception<PyNotImplementedException>(m, "NotImplementedException", not_supported_error);

}
} // namespace otterbrix
