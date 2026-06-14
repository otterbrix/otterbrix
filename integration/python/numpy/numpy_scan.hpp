#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <core/result_wrapper.hpp>
#include <common/typedefs.hpp>
#include <components/vector/vector.hpp>

#include <memory_resource>

namespace otterbrix {

struct pandas_column_bind_data_t;

struct numpy_scan_t {
	static core::error_t scan(std::pmr::memory_resource *resource, pandas_column_bind_data_t &bind_data, idx_t count, idx_t offset, components::vector::vector_t &out);
	static core::error_t scan_object_column(std::pmr::memory_resource *resource, PyObject **col, idx_t stride, idx_t count, idx_t offset, components::vector::vector_t &out);
};

} // namespace otterbrix
