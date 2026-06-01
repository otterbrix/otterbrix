#pragma once

#include "pybind11/pybind_wrapper.hpp"
#include <core/typedefs.hpp>

namespace otterbrix {

struct PyUtil {
	static const char *PyUnicodeData(py::handle &obj) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
		return const_char_ptr_cast(PyUnicode_DATA(obj.ptr()));
#pragma GCC diagnostic pop
	}

	static idx_t PyUnicodeGetLength(py::handle &obj) {
		return static_cast<idx_t>(PyUnicode_GET_LENGTH(obj.ptr()));
	}

	static bool PyUnicodeIsCompact(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_COMPACT(obj);
	}

	static bool PyUnicodeIsASCII(PyCompactUnicodeObject *obj) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
		return PyUnicode_IS_ASCII(obj);
#pragma GCC diagnostic pop
	}

	static int PyUnicodeKind(py::handle &obj) {
		return PyUnicode_KIND(obj.ptr());
	}

	static Py_UCS1 *PyUnicode1ByteData(py::handle &obj) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
		return PyUnicode_1BYTE_DATA(obj.ptr());
#pragma GCC diagnostic pop
	}

	static Py_UCS2 *PyUnicode2ByteData(py::handle &obj) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
		return PyUnicode_2BYTE_DATA(obj.ptr());
#pragma GCC diagnostic pop
	}

	static Py_UCS4 *PyUnicode4ByteData(py::handle &obj) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
		return PyUnicode_4BYTE_DATA(obj.ptr());
#pragma GCC diagnostic pop
	}
};

} // namespace otterbrix
