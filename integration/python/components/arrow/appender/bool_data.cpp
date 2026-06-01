#include "bool_data.hpp"

namespace components::arrow::appender {

    void ArrowBoolData::Initialize(ArrowAppendData &result, const types::complex_logical_type &type, uint64_t capacity) {
    	auto byte_count = (capacity + 7) / 8;
    	result.GetMainBuffer().reserve(byte_count);
    	(void)AppendValidity; // silence a compiler warning about unused static function
    }
    
    void ArrowBoolData::Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size) {
    	uint64_t size = to - from;
        vector::unified_vector_format format;
    	input.to_unified_format(input_size, format);
    	auto &main_buffer = append_data.GetMainBuffer();
    	auto &validity_buffer = append_data.GetValidityBuffer();
    	// we initialize both the validity and the bit set to 1's
    	ResizeValidity(validity_buffer, append_data.row_count + size);
    	ResizeValidity(main_buffer, append_data.row_count + size);
        auto data = format.get_data<bool>();
    
    	auto result_data = main_buffer.GetData<uint8_t>();
    	auto validity_data = validity_buffer.GetData<uint8_t>();
    	uint8_t current_bit;
    	uint64_t current_byte;
    	GetBitPosition(append_data.row_count, current_byte, current_bit);
    	for (uint64_t i = from; i < to; i++) {
    		auto source_idx = format.referenced_indexing->get_index(i);
    		// append the validity mask
    		if (!format.validity.row_is_valid(source_idx)) {
    			SetNull(append_data, validity_data, current_byte, current_bit);
    		} else if (!data[source_idx]) {
    			UnsetBit(result_data, current_byte, current_bit);
    		}
    		NextBit(current_byte, current_bit);
    	}
    	append_data.row_count += size;
    }
    
    void ArrowBoolData::Finalize(ArrowAppendData &append_data, const types::complex_logical_type &type, ArrowArray *result) {
    	result->n_buffers = 2;
    	result->buffers[1] = append_data.GetMainBuffer().data();
    }

} // namespace components::arrow::appender
