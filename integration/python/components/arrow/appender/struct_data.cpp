#include "struct_data.hpp"

namespace components::arrow::appender {
    using types::complex_logical_type;

    //===--------------------------------------------------------------------===//
    // Structs
    //===--------------------------------------------------------------------===//
    void ArrowStructData::Initialize(ArrowAppendData &result, const complex_logical_type &type, uint64_t capacity) {
    	auto &children = type.child_types(); 
    	for (auto &child : children) {
    		auto child_buffer = ArrowAppender::InitializeChild(child, capacity);
    		result.child_data.push_back(std::move(child_buffer));
    	}
    }
    
    void ArrowStructData::Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size) {
        vector::unified_vector_format format;
    	input.to_unified_format(input_size, format);
    	uint64_t size = to - from;
    	AppendValidity(append_data, format, from, to);
    	// append the children of the struct
    	//auto &children = StructVector::GetEntries(input);
        auto& children = input.entries();
    	for (uint64_t child_idx = 0; child_idx < children.size(); child_idx++) {
    		auto &child = children[child_idx];
    		auto &child_data = *append_data.child_data[child_idx];
    		child_data.append_vector(child_data, *child, from, to, size);
    	}
    	append_data.row_count += size;
    }
    
    void ArrowStructData::Finalize(ArrowAppendData &append_data, const complex_logical_type &type, ArrowArray *result) {
    	result->n_buffers = 1;
    
    	auto &child_types = type.child_types(); 
    	ArrowAppender::AddChildren(append_data, child_types.size());
    	result->children = append_data.child_pointers.data();
    	result->n_children = static_cast<int64_t>(child_types.size());
    	for (uint64_t i = 0; i < child_types.size(); i++) {
       		auto &child_type = child_types[i];
    		append_data.child_arrays[i] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[i]));
    	}
    }

} // namespace components::arrow::appender
