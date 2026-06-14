#pragma once

#include "array_wrapper.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <core/result_wrapper.hpp>
#include <common/typedefs.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <vector>

namespace otterbrix {

class numpy_result_conversion_t {
public:
	numpy_result_conversion_t(
            std::pmr::memory_resource *resource,
            const std::vector<components::types::complex_logical_type> &types,
            idx_t initial_capacity, bool pandas = false);

	// Allocates the backing arrays at the requested initial capacity. Must be called once before append.
	core::error_t initialize(idx_t initial_capacity);
	core::error_t append(components::vector::data_chunk_t &chunk);

	py::object to_array(idx_t col_idx) {
		return owned_data[col_idx].to_array();
	}
	bool to_pandas() const {
		return pandas;
	}

private:
	core::error_t resize(idx_t new_capacity);

private:
	std::pmr::memory_resource *resource_;
	std::vector<array_wrapper_t> owned_data;
	idx_t count;
	idx_t capacity;
	bool pandas;
};

} // namespace otterbrix
