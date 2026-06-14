#pragma once

#include "../pandas_column.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include<common/typedefs.hpp>

namespace otterbrix {

class pandas_numpy_column_t : public pandas_column_t {
public:
	pandas_numpy_column_t(py::array array_p) : pandas_column_t(pandas_column_backend_t::NUMPY), array(std::move(array_p)) {
		assert(py::hasattr(array, "strides"));
		stride = array.attr("strides").attr("__getitem__")(0).cast<idx_t>();
	}

public:
	py::array array;
	idx_t stride;
};

} // namespace otterbrix
