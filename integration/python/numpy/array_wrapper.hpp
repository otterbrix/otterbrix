#pragma once

#include "raw_array_wrapper.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/vector/vector.hpp>
#include <components/types/types.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <core/typedefs.hpp>
#include <core/types/memory.hpp>

namespace otterbrix {

struct NumpyAppendData {
public:
	NumpyAppendData(components::vector::unified_vector_format &idata, 
            components::vector::vector_t &input)
	    : idata(idata), input(input) {
	}

public:
	components::vector::unified_vector_format &idata;
	components::vector::vector_t &input;

	idx_t source_offset;
	idx_t target_offset;
	data_ptr_t target_data;
	bool *target_mask;
	idx_t count;
	idx_t source_size;
	components::types::physical_type physical_type = components::types::physical_type::INVALID;
	bool pandas = false;
};

struct ArrayWrapper {
	explicit ArrayWrapper(const components::types::complex_logical_type &type, bool pandas = false);

	unique_ptr<RawArrayWrapper> data;
	unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;
	bool pandas;

public:
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, components::vector::vector_t &input, 
            idx_t source_size, idx_t source_offset = 0,
	        idx_t count = components::table::storage::INVALID_INDEX);
	py::object ToArray() const;
};

} // namespace otterbrix
