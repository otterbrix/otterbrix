#pragma once

#include "raw_array_wrapper.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/vector/vector.hpp>
#include <components/types/types.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <core/result_wrapper.hpp>
#include <core/typedefs.hpp>
#include <memory>

#include <memory_resource>

namespace otterbrix {

struct NumpyAppendData {
public:
	NumpyAppendData(std::pmr::memory_resource *resource,
            components::vector::unified_vector_format &idata,
            components::vector::vector_t &input)
	    : resource(resource), idata(idata), input(input), error(core::error_t::no_error()) {
	}

public:
	std::pmr::memory_resource *resource;
	components::vector::unified_vector_format &idata;
	components::vector::vector_t &input;

	idx_t source_offset;
	idx_t target_offset;
	data_ptr_t target_data;
	bool *target_mask;
	idx_t count;
	idx_t source_size;
	bool pandas = false;
	// Set by nested converters when a recursive Append fails; surfaced once per batch.
	core::error_t error;
};

struct ArrayWrapper {
	explicit ArrayWrapper(const components::types::complex_logical_type &type, bool pandas = false);

	std::unique_ptr<RawArrayWrapper> data;
	std::unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;
	bool pandas;

public:
	core::error_t Initialize(std::pmr::memory_resource *resource, idx_t capacity);
	void Resize(idx_t new_capacity);
	core::error_t Append(std::pmr::memory_resource *resource, idx_t current_offset, components::vector::vector_t &input,
            idx_t source_size, idx_t source_offset = 0,
	        idx_t count = components::table::storage::INVALID_INDEX);
	py::object ToArray() const;
};

} // namespace otterbrix
