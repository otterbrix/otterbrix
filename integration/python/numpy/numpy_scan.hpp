#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <core/typedefs.hpp>
#include <components/vector/vector.hpp>

namespace otterbrix {

struct PandasColumnBindData;

struct NumpyScan {
	static void Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, components::vector::vector_t &out);
	static void ScanObjectColumn(PyObject **col, idx_t stride, idx_t count, idx_t offset, components::vector::vector_t &out);
};

} // namespace otterbrix
