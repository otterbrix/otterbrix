#pragma once

#include "array_wrapper.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <core/types/vector.hpp>
#include <core/typedefs.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

namespace otterbrix {

class NumpyResultConversion {
public:
	NumpyResultConversion(
            const vector<components::types::complex_logical_type> &types, 
            idx_t initial_capacity, bool pandas = false);

	void Append(components::vector::data_chunk_t &chunk);

	py::object ToArray(idx_t col_idx) {
		return owned_data[col_idx].ToArray();
	}
	bool ToPandas() const {
		return pandas;
	}

private:
	void Resize(idx_t new_capacity);

private:
	vector<ArrayWrapper> owned_data;
	idx_t count;
	idx_t capacity;
	bool pandas;
};

} // namespace otterbrix
