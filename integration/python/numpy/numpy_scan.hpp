#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <core/result_wrapper.hpp>
#include <core/typedefs.hpp>
#include <components/vector/vector.hpp>

#include <memory_resource>

namespace otterbrix {

struct PandasColumnBindData;

struct NumpyScan {
	static core::error_t Scan(std::pmr::memory_resource *resource, PandasColumnBindData &bind_data, idx_t count, idx_t offset, components::vector::vector_t &out);
	static core::error_t ScanObjectColumn(std::pmr::memory_resource *resource, PyObject **col, idx_t stride, idx_t count, idx_t offset, components::vector::vector_t &out);
};

} // namespace otterbrix
