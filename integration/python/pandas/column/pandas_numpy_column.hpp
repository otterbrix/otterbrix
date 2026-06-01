#pragma once

#include "../pandas_column.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include<core/typedefs.hpp>

namespace otterbrix {

class PandasNumpyColumn : public PandasColumn {
public:
	PandasNumpyColumn(py::array array_p) : PandasColumn(PandasColumnBackend::NUMPY), array(std::move(array_p)) {
		assert(py::hasattr(array, "strides"));
		stride = array.attr("strides").attr("__getitem__")(0).cast<idx_t>();
	}

public:
	py::array array;
	idx_t stride;
};

} // namespace otterbrix
