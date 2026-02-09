#pragma once

#include <pybind11/pybind11.h>

namespace otterbrix {

class Warning : public std::exception {};

// This is the error structure defined in the DBAPI spec
// StandardError
// |__ Warning
// |__ Error
//    |__ InterfaceError
//    |__ DatabaseError
//       |__ DataError
//       |__ OperationalError
//       |__ IntegrityError
//       |__ InternalError
//       |__ ProgrammingError
//       |__ NotSupportedError
//===--------------------------------------------------------------------===//
// Base Error
//===--------------------------------------------------------------------===//
class PyError : public std::runtime_error {
public:
	explicit PyError(const std::string &err) : std::runtime_error(err) {
	}
};

class DatabaseError : public PyError {
public:
	explicit DatabaseError(const std::string &err) : PyError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Unknown Errors
//===--------------------------------------------------------------------===//
class PyFatalException : public DatabaseError {
public:
	explicit PyFatalException(const std::string &err) : DatabaseError(err) {
	}
};

class PyInterruptException : public DatabaseError {
public:
	explicit PyInterruptException(const std::string &err) : DatabaseError(err) {
	}
};

class PyPermissionException : public DatabaseError {
public:
	explicit PyPermissionException(const std::string &err) : DatabaseError(err) {
	}
};

class PySequenceException : public DatabaseError {
public:
	explicit PySequenceException(const std::string &err) : DatabaseError(err) {
	}
};

class PyDependencyException : public DatabaseError {
public:
	explicit PyDependencyException(const std::string &err) : DatabaseError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Data Error
//===--------------------------------------------------------------------===//
class DataError : public DatabaseError {
public:
	explicit DataError(const std::string &err) : DatabaseError(err) {
	}
};

class PyOutOfRangeException : public DataError {
public:
	explicit PyOutOfRangeException(const std::string &err) : DataError(err) {
	}
};

class PyConversionException : public DataError {
public:
	explicit PyConversionException(const std::string &err) : DataError(err) {
	}
};

class PyTypeMismatchException : public DataError {
public:
	explicit PyTypeMismatchException(const std::string &err) : DataError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Operational Error
//===--------------------------------------------------------------------===//
class OperationalError : public DatabaseError {
public:
	explicit OperationalError(const std::string &err) : DatabaseError(err) {
	}
};

class PyTransactionException : public OperationalError {
public:
	explicit PyTransactionException(const std::string &err) : OperationalError(err) {
	}
};

class PyOutOfMemoryException : public OperationalError {
public:
	explicit PyOutOfMemoryException(const std::string &err) : OperationalError(err) {
	}
};

class PyConnectionException : public OperationalError {
public:
	explicit PyConnectionException(const std::string &err) : OperationalError(err) {
	}
};

class PySerializationException : public OperationalError {
public:
	explicit PySerializationException(const std::string &err) : OperationalError(err) {
	}
};

class PyIOException : public OperationalError {
public:
	explicit PyIOException(const std::string &err) : OperationalError(err) {
	}
};

class PyHTTPException : public PyIOException {
public:
	explicit PyHTTPException(const std::string &err) : PyIOException(err) {
	}
};

//===--------------------------------------------------------------------===//
// Integrity Error
//===--------------------------------------------------------------------===//
class IntegrityError : public DatabaseError {
public:
	explicit IntegrityError(const std::string &err) : DatabaseError(err) {
	}
};

class PyConstraintException : public IntegrityError {
public:
	explicit PyConstraintException(const std::string &err) : IntegrityError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Internal Error
//===--------------------------------------------------------------------===//
class InternalError : public DatabaseError {
public:
	explicit InternalError(const std::string &err) : DatabaseError(err) {
	}
};

class PyInternalException : public InternalError {
public:
	explicit PyInternalException(const std::string &err) : InternalError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Programming Error
//===--------------------------------------------------------------------===//
class ProgrammingError : public DatabaseError {
public:
	explicit ProgrammingError(const std::string &err) : DatabaseError(err) {
	}
};

class PyParserException : public ProgrammingError {
public:
	explicit PyParserException(const std::string &err) : ProgrammingError(err) {
	}
};

class PySyntaxException : public ProgrammingError {
public:
	explicit PySyntaxException(const std::string &err) : ProgrammingError(err) {
	}
};

class PyBinderException : public ProgrammingError {
public:
	explicit PyBinderException(const std::string &err) : ProgrammingError(err) {
	}
};

class PyInvalidInputException : public ProgrammingError {
public:
	explicit PyInvalidInputException(const std::string &err) : ProgrammingError(err) {
	}
};

class PyInvalidTypeException : public ProgrammingError {
public:
	explicit PyInvalidTypeException(const std::string &err) : ProgrammingError(err) {
	}
};

class PyCatalogException : public ProgrammingError {
public:
	explicit PyCatalogException(const std::string &err) : ProgrammingError(err) {
	}
};

//===--------------------------------------------------------------------===//
// Not Supported Error
//===--------------------------------------------------------------------===//
class NotSupportedError : public DatabaseError {
public:
	explicit NotSupportedError(const std::string &err) : DatabaseError(err) {
	}
};

class PyNotImplementedException : public NotSupportedError {
public:
	explicit PyNotImplementedException(const std::string &err) : NotSupportedError(err) {
	}
};

void RegisterExceptions(const pybind11::module_ &m);

} // namespace otterbrix
