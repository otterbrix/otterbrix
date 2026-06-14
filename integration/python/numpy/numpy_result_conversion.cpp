#include "numpy_result_conversion.hpp"

#include "array_wrapper.hpp"

#include <cassert>
#include <memory_resource>
#include <vector>

namespace otterbrix {

numpy_result_conversion_t::numpy_result_conversion_t(
        std::pmr::memory_resource *resource,
        const std::vector<components::types::complex_logical_type> &types,
        idx_t initial_capacity, bool pandas)
    : resource_(resource), count(0), capacity(0), pandas(pandas) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type, pandas);
	}
	(void)initial_capacity;
}

core::error_t numpy_result_conversion_t::initialize(idx_t initial_capacity) {
	return resize(initial_capacity);
}

core::error_t numpy_result_conversion_t::resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			if (auto err = data.initialize(resource_, new_capacity); err.contains_error()) {
				return err;
			}
		}
	} else {
		for (auto &data : owned_data) {
			data.resize(new_capacity);
		}
	}
	capacity = new_capacity;
	return core::error_t::no_error();
}

core::error_t numpy_result_conversion_t::append(components::vector::data_chunk_t &chunk) {
	if (count + chunk.size() > capacity) {
		if (auto err = resize(capacity * 2); err.contains_error()) {
			return err;
		}
	}
	auto chunk_types = chunk.types();
	idx_t source_offset = 0;
	auto source_size = chunk.size();
	auto to_append = chunk.size();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		if (auto err = owned_data[col_idx].append(resource_, count, chunk.data[col_idx], source_size, source_offset, to_append);
		    err.contains_error()) {
			return err;
		}
	}
	count += to_append;
	return core::error_t::no_error();
}

} // namespace otterbrix
