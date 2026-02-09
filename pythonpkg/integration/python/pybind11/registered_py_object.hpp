#pragma once

#include "pybind_wrapper.hpp"

namespace otterbrix {

class RegisteredObject {
public:
	explicit RegisteredObject(py::object obj_p) : obj(std::move(obj_p)) {
	}
	virtual ~RegisteredObject() {
		py::gil_scoped_acquire acquire;
		obj = py::none();
	}

	py::object obj;
};

} // namespace otterbrix
