#pragma once

#include "array_wrapper.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <core/result_wrapper.hpp>
#include <core/types/vector.hpp>
#include <core/typedefs.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>

namespace otterbrix {

class NumpyResultConversion {
public:
	NumpyResultConversion(
            std::pmr::memory_resource *resource,
            const vector<components::types::complex_logical_type> &types,
            idx_t initial_capacity, bool pandas = false);

	// Allocates the backing arrays at the requested initial capacity. Must be called once before Append.
	core::error_t Initialize(idx_t initial_capacity);
	core::error_t Append(components::vector::data_chunk_t &chunk);

	py::object ToArray(idx_t col_idx) {
		return owned_data[col_idx].ToArray();
	}
	bool ToPandas() const {
		return pandas;
	}

private:
	core::error_t Resize(idx_t new_capacity);

private:
	std::pmr::memory_resource *resource_;
	vector<ArrayWrapper> owned_data;
	idx_t count;
	idx_t capacity;
	bool pandas;
};

} // namespace otterbrix
