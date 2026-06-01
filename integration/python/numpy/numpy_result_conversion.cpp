#include "numpy_result_conversion.hpp"

#include "array_wrapper.hpp"

#include <cassert>

namespace otterbrix {

NumpyResultConversion::NumpyResultConversion(
        const vector<components::types::complex_logical_type> &types, 
        idx_t initial_capacity, bool pandas)
    : count(0), capacity(0), pandas(pandas) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type, pandas);
	}
	Resize(initial_capacity);
}

void NumpyResultConversion::Resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			data.Initialize(new_capacity);
		}
	} else {
		for (auto &data : owned_data) {
			data.Resize(new_capacity);
		}
	}
	capacity = new_capacity;
}

void NumpyResultConversion::Append(components::vector::data_chunk_t &chunk) {
	if (count + chunk.size() > capacity) {
		Resize(capacity * 2);
	}
	auto chunk_types = chunk.types();
	idx_t source_offset = 0;
	auto source_size = chunk.size();
	auto to_append = chunk.size();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		owned_data[col_idx].Append(count, chunk.data[col_idx], source_size, source_offset, to_append);
	}
	count += to_append;
}

} // namespace otterbrix
