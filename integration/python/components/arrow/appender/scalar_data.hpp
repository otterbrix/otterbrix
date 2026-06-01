#pragma once

#include "append_data.hpp"
#include "../arrow.hpp"
#include "../arrow_appender.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <cassert>

namespace components::arrow::appender {

    //===--------------------------------------------------------------------===//
    // Scalar Types
    //===--------------------------------------------------------------------===//
    struct ArrowScalarConverter {
    	template <class TGT, class SRC>
    	static TGT Operation(SRC input) {
    		return input;
    	}
    
    	static bool SkipNulls() {
    		return false;
    	}
    
    	template <class TGT>
    	static void SetNull(TGT &value) {
    	}
    };
    
    
    template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
    struct ArrowScalarBaseData {
    	static void Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size) {
    		assert(to >= from);
    		uint64_t size = to - from;
    		assert(size <= input_size);
            vector::unified_vector_format format;
    		input.to_unified_format(input_size, format);
    
    		// append the validity mask
    		AppendValidity(append_data, format, from, to);
    
    		// append the main data
    		auto &main_buffer = append_data.GetMainBuffer();
    		main_buffer.resize(main_buffer.size() + sizeof(TGT) * size);
    		auto data = format.get_data<SRC>();
    		auto result_data = main_buffer.GetData<TGT>();
    
    		for (uint64_t i = from; i < to; i++) {
    			auto source_idx = format.referenced_indexing->get_index(i);
    			auto result_idx = append_data.row_count + i - from;
    
    			if (OP::SkipNulls() && !format.validity.row_is_valid(source_idx)) {
    				OP::template SetNull<TGT>(result_data[result_idx]);
    				continue;
    			}
    			result_data[result_idx] = OP::template Operation<TGT, SRC>(data[source_idx]);
    		}
    		append_data.row_count += size;
    	}
    };
    
    template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
    struct ArrowScalarData : public ArrowScalarBaseData<TGT, SRC, OP> {
    	static void Initialize(ArrowAppendData &result, const types::complex_logical_type &type, uint64_t capacity) {
    		result.GetMainBuffer().reserve(capacity * sizeof(TGT));
    	}
    
    	static void Finalize(ArrowAppendData &append_data, const types::complex_logical_type &type, ArrowArray *result) {
    		result->n_buffers = 2;
    		result->buffers[1] = append_data.GetMainBuffer().data();
    	}
    };

} // namespace components::arrow::appender
