#pragma once

#include "../arrow.hpp"
#include "../arrow_buffer.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <vector>
#include <memory>
#include <array>

namespace components::arrow::appender {

    struct ArrowAppendData;
    
    //===--------------------------------------------------------------------===//
    // Arrow append data
    //===--------------------------------------------------------------------===//
    typedef void (*initialize_t)(ArrowAppendData &result, const types::complex_logical_type &type, uint64_t capacity);
    // append_data: The arrow array we're appending into
    // input: The data we're appending
    // from: The offset into the input we're scanning
    // to: The last index of the input we're scanning
    // input_size: The total size of the 'input' Vector.
    typedef void (*append_vector_t)(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size);
    typedef void (*finalize_t)(ArrowAppendData &append_data, const types::complex_logical_type &type, ArrowArray *result);
    
    // This struct is used to save state for appending a column
    // afterwards the ownership is passed to the arrow array, as 'private_data'
    // FIXME: we should separate the append state variables from the variables required by the ArrowArray into
    // ArrowAppendState
    struct ArrowAppendData {
    	explicit ArrowAppendData() {
    		dictionary.release = nullptr;
    		arrow_buffers.resize(3);
    	}
    
    	//! Getters for the Buffers
    	ArrowBuffer &GetValidityBuffer() {
    		return arrow_buffers[0];
    	}
    
    	ArrowBuffer &GetMainBuffer() {
    		return arrow_buffers[1];
    	}
    
    	ArrowBuffer &GetAuxBuffer() {
    		return arrow_buffers[2];
    	}
    
    	ArrowBuffer &GetBufferSizeBuffer() {
    		//! This is a special case, we resize it if necessary since it's a different size than set in the constructor
    		if (arrow_buffers.size() == 3) {
    			arrow_buffers.resize(4);
    		}
    		return arrow_buffers[3];
    	}
    
    	uint64_t row_count = 0;
    	uint64_t null_count = 0;
    
    	//! function pointers for construction
    	initialize_t initialize = nullptr;
    	append_vector_t append_vector = nullptr;
    	finalize_t finalize = nullptr;
    
    	//! child data (if any)
    	std::vector<std::unique_ptr<ArrowAppendData>> child_data;
    
    	//! the arrow array C API data, only set after Finalize
        std::unique_ptr<ArrowArray> array;
    	std::array<const void*, 4> buffers = {{nullptr, nullptr, nullptr, nullptr}};
    	std::vector<ArrowArray*> child_pointers;
    	//! Arrays so the children can be moved
    	std::vector<ArrowArray> child_arrays;
    	ArrowArray dictionary;
    
    	//! Offset used to keep data positions when producing a mix of inlined and not-inlined arrow string views.
    	uint64_t offset = 0;
    
    private:
    	//! The buffers of the arrow vector
    	std::vector<ArrowBuffer> arrow_buffers;
    };
    
    //===--------------------------------------------------------------------===//
    // Append Helper Functions
    //===--------------------------------------------------------------------===//
    
    static void GetBitPosition(uint64_t row_idx, uint64_t &current_byte, uint8_t &current_bit) {
    	current_byte = row_idx / 8;
    	current_bit = row_idx % 8;
    }
    
    static void UnsetBit(uint8_t *data, uint64_t current_byte, uint8_t current_bit) {
    	data[current_byte] &= ~((uint64_t)1 << current_bit);
    }
    
    static void NextBit(uint64_t &current_byte, uint8_t &current_bit) {
    	current_bit++;
    	if (current_bit == 8) {
    		current_byte++;
    		current_bit = 0;
    	}
    }
    
    static void ResizeValidity(ArrowBuffer &buffer, uint64_t row_count) {
    	auto byte_count = (row_count + 7) / 8;
    	buffer.resize(byte_count, 0xFF);
    }
    
    static void SetNull(ArrowAppendData &append_data, uint8_t *validity_data, uint64_t current_byte, uint8_t current_bit) {
    	UnsetBit(validity_data, current_byte, current_bit);
    	append_data.null_count++;
    }
    
    static void AppendValidity(ArrowAppendData &append_data, vector::unified_vector_format &format, uint64_t from, uint64_t to) {
    	// resize the buffer, filling the validity buffer with all valid values
    	uint64_t size = to - from;
    	ResizeValidity(append_data.GetValidityBuffer(), append_data.row_count + size);
    	if (format.validity.all_valid()) {
    		// if all values are valid we don't need to do anything else
    		return;
    	}
    
    	// otherwise we iterate through the validity mask
    	auto validity_data = (uint8_t*)append_data.GetValidityBuffer().data();
    	uint8_t current_bit;
    	uint64_t current_byte;
    	GetBitPosition(append_data.row_count, current_byte, current_bit);
    	for (uint64_t i = from; i < to; i++) {
    		auto source_idx = format.referenced_indexing->get_index(i);
    		// append the validity mask
    		if (!format.validity.row_is_valid(source_idx)) {
    			SetNull(append_data, validity_data, current_byte, current_bit);
    		}
    		NextBit(current_byte, current_bit);
    	}
    }

} // namespace components::arrow::appender
