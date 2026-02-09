#pragma once

#include "pybind_wrapper.hpp"
#include "gil_wrapper.hpp"

#include <core/types/vector.hpp>

#include <cassert>

namespace otterbrix {

//! Every Python Object Must be created through our container
//! The Container ensures that the GIL is HOLD on Python Object Construction/Destruction/Modification
class PythonObjectContainer {
public:
	PythonObjectContainer() {
	}

	~PythonObjectContainer() {
		py::gil_scoped_acquire acquire;
		py_obj.clear();
	}

	void Push(py::object &&obj) {
		py::gil_scoped_acquire gil;
		PushInternal(std::move(obj));
	}

	const py::object &LastAddedObject() {
		assert(!py_obj.empty());
		return py_obj.back();
	}

private:
	void PushInternal(py::object &&obj) {
		py_obj.emplace_back(obj);
	}

	vector<py::object> py_obj;
};
} // namespace otterbrix
