#pragma once

#include "append_data.hpp"
#include "../arrow.hpp"
#include "../arrow_appender.hpp"

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include <cassert>
#include <memory>
#include <vector>

namespace components::arrow::appender {
    using types::complex_logical_type;

    //===--------------------------------------------------------------------===//
    // Maps
    //===--------------------------------------------------------------------===//
    template <class BUFTYPE = int64_t>
    struct ArrowMapData {
    public:
    	static void Initialize(ArrowAppendData &result, const complex_logical_type &type, uint64_t capacity) {
    		// map types are stored in a (too) clever way
    		// the main buffer holds the null values and the offsets
    		// then we have a single child, which is a struct of the map_type, and the key_type
    		result.GetMainBuffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            auto map_extention = static_cast<types::map_logical_type_extention*>(type.extention());
    
    		auto &key_type = map_extention->key();
    		auto &value_type = map_extention->value(); 
    		auto internal_struct = std::make_unique<ArrowAppendData>();
    		internal_struct->child_data.push_back(ArrowAppender::InitializeChild(key_type, capacity));
    		internal_struct->child_data.push_back(ArrowAppender::InitializeChild(value_type, capacity));
    
    		result.child_data.push_back(std::move(internal_struct));
    	}
    
    	static void Append(ArrowAppendData &append_data, vector::vector_t &input, 
                uint64_t from, uint64_t to, uint64_t input_size) {
            vector::unified_vector_format format;
    		input.to_unified_format(input_size, format);
    		uint64_t size = to - from;
    		AppendValidity(append_data, format, from, to);
            std::vector<uint32_t> child_indices;
    		ArrowListData<BUFTYPE>::AppendOffsets(append_data, format, from, to, child_indices);
    
            vector::indexing_vector_t child_sel(child_indices.data());
    		//auto &key_vector = MapVector::GetKeys(input);
    		//auto &value_vector = MapVector::GetValues(input);
            auto &key_vector = input.entries().at(0);
            auto &value_vector = input.entries().at(1);
    		auto list_size = child_indices.size();
    
    		auto &struct_data = *append_data.child_data[0];
    		auto &key_data = *struct_data.child_data[0];
    		auto &value_data = *struct_data.child_data[1];
    
            vector::vector_t key_vector_copy(key_vector->resource(), key_vector->type());
    		key_vector_copy.slice(*key_vector, child_sel, list_size);
            vector::vector_t value_vector_copy(value_vector->resource(), value_vector->type());
    		value_vector_copy.slice(*value_vector, child_sel, list_size);
    		key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
    		value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);
    
    		append_data.row_count += size;
    		struct_data.row_count += size;
    	}
    
    	static void Finalize(ArrowAppendData &append_data, const complex_logical_type &type, ArrowArray *result) {
    		// set up the main map buffer
            assert(result);
    		result->n_buffers = 2;
    		result->buffers[1] = append_data.GetMainBuffer().data();
    
    		// the main map buffer has a single child: a struct
    		ArrowAppender::AddChildren(append_data, 1);
    		result->children = append_data.child_pointers.data();
    		result->n_children = 1;
    
    		auto &struct_data = *append_data.child_data[0];
    		auto struct_result = ArrowAppender::FinalizeChild(type, std::move(append_data.child_data[0]));
    
    		// Initialize the struct array data
    		const auto struct_child_count = 2;
    		ArrowAppender::AddChildren(struct_data, struct_child_count);
    		struct_result->children = struct_data.child_pointers.data();
    		struct_result->n_buffers = 1;
    		struct_result->n_children = struct_child_count;
    		struct_result->length = static_cast<int64_t>(struct_data.child_data[0]->row_count);
    
    		append_data.child_arrays[0] = *struct_result;
    
    		assert(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);
    
            auto map_extention = static_cast<types::map_logical_type_extention*>(type.extention());
    		auto &key_type = map_extention->key();
    		auto &value_type = map_extention->value();
    		auto key_data = ArrowAppender::FinalizeChild(key_type, std::move(struct_data.child_data[0]));
    		struct_data.child_arrays[0] = *key_data;
    		struct_data.child_arrays[1] = *ArrowAppender::FinalizeChild(value_type, std::move(struct_data.child_data[1]));
    
    		// keys cannot have null values
    		if (key_data->null_count > 0) {
    			throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
    		}
    	}
    };

} // namespace components::arrow::appender
