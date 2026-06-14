#pragma once

#include "pybind_wrapper.hpp"

namespace otterbrix {

class registered_object_t {
public:
	explicit registered_object_t(py::object obj_p) : obj(std::move(obj_p)) {
	}
	virtual ~registered_object_t() {
		py::gil_scoped_acquire acquire;
		obj = py::none();
	}

	py::object obj;
};

} // namespace otterbrix
