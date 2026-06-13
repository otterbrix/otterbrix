#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/typedefs.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>

namespace otterbrix {

struct RawArrayWrapper {

	explicit RawArrayWrapper(const components::types::complex_logical_type &type);

	py::array array;
	data_ptr_t data;
	components::types::complex_logical_type type;
	idx_t type_width;
	idx_t count;

public:
	static string OtterBrixToNumpyDtype(const components::types::complex_logical_type &type);
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
};

} // namespace otterbrix
