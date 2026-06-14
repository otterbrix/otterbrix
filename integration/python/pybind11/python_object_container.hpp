#pragma once

#include "pybind_wrapper.hpp"
#include "gil_wrapper.hpp"


#include <cassert>
#include <vector>

namespace otterbrix {

//! Every Python Object Must be created through our container
//! The Container ensures that the GIL is HOLD on Python Object Construction/Destruction/Modification
class python_object_container_t {
public:
	python_object_container_t() {
	}

	~python_object_container_t() {
		py::gil_scoped_acquire acquire;
		py_obj.clear();
	}

	void push(py::object &&obj) {
		py::gil_scoped_acquire gil;
		push_internal(std::move(obj));
	}

	const py::object &last_added_object() {
		assert(!py_obj.empty());
		return py_obj.back();
	}

private:
	void push_internal(py::object &&obj) {
		py_obj.emplace_back(obj);
	}

	std::vector<py::object> py_obj;
};
} // namespace otterbrix
