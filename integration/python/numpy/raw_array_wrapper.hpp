#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>
#include <core/typedefs.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {

struct RawArrayWrapper {

	explicit RawArrayWrapper(const components::types::complex_logical_type &type);

	py::array array;
	data_ptr_t data;
	components::types::complex_logical_type type;
	idx_t type_width;
	idx_t count;

public:
	static core::result_wrapper_t<std::string> OtterBrixToNumpyDtype(std::pmr::memory_resource *resource, const components::types::complex_logical_type &type);
	core::error_t Initialize(std::pmr::memory_resource *resource, idx_t capacity);
	void Resize(idx_t new_capacity);
};

} // namespace otterbrix
