#include "fixed_size_list_data.hpp"


namespace components::arrow::appender {
    using types::complex_logical_type;

    //===--------------------------------------------------------------------===//
    // Arrays
    //===--------------------------------------------------------------------===//
    void ArrowFixedSizeListData::Initialize(ArrowAppendData &result, const complex_logical_type &type, uint64_t capacity) {
        auto array_extention = static_cast<types::array_logical_type_extention*>(type.extention());
    	auto array_size = array_extention->size();
    	auto &child_type = array_extention->internal_type();

    	auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity * array_size);
    	result.child_data.push_back(std::move(child_buffer));
    }
    
    void ArrowFixedSizeListData::Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to,
                                        uint64_t input_size) {
        vector::unified_vector_format format;
    	input.to_unified_format(input_size, format);
    	uint64_t size = to - from;
    	AppendValidity(append_data, format, from, to);
    	input.flatten(input_size);

        auto array_extention = static_cast<types::array_logical_type_extention*>(input.type().extention());
    	auto array_size = array_extention->size();
    	auto &child_vector = input.entry(); 
    	auto &child_data = *append_data.child_data[0];
    	child_data.append_vector(child_data, child_vector, from * array_size, to * array_size, size * array_size);
    	append_data.row_count += size;
    }
    
    void ArrowFixedSizeListData::Finalize(ArrowAppendData &append_data, const complex_logical_type &type, ArrowArray *result) {
    	result->n_buffers = 1;
    	auto &child_type = type.child_type(); 
    	ArrowAppender::AddChildren(append_data, 1);
    	result->children = append_data.child_pointers.data();
    	result->n_children = 1;
    	append_data.child_arrays[0] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[0]));
    }

} // namespace components::arrow::appender
