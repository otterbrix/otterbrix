#pragma once

#include "append_data.hpp"
#include "../arrow.hpp"
#include "../arrow_appender.hpp"

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace components::arrow::appender {
    using types::complex_logical_type;

    template <class BUFTYPE = int64_t>
    struct ArrowListData {
    public:
    	static void Initialize(ArrowAppendData &result, const complex_logical_type &type, uint64_t capacity) {
    		auto &child_type = type.child_type(); 
    		result.GetMainBuffer().reserve((capacity + 1) * sizeof(BUFTYPE));
    		auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity);
    		result.child_data.push_back(std::move(child_buffer));
    	}
    
    	static void Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size) {
            vector::unified_vector_format format;
    		input.to_unified_format(input_size, format);
    		uint64_t size = to - from;
            std::vector<uint32_t> child_indices;
    		AppendValidity(append_data, format, from, to);
    		AppendOffsets(append_data, format, from, to, child_indices);
    
    		// append the child vector of the list
            vector::indexing_vector_t child_sel(child_indices.data());
    		auto &child = input.entry(); 
    		auto child_size = child_indices.size();
            vector::vector_t child_copy(child.resource(), child.type());
    		child_copy.slice(child, child_sel, child_size);
    		append_data.child_data[0]->append_vector(*append_data.child_data[0], child_copy, 0, child_size, child_size);
    		append_data.row_count += size;
    	}
    
    	static void Finalize(ArrowAppendData &append_data, const complex_logical_type &type, ArrowArray *result) {
    		result->n_buffers = 2;
    		result->buffers[1] = append_data.GetMainBuffer().data();
    
    		auto &child_type = type.child_type();
    		ArrowAppender::AddChildren(append_data, 1);
    		result->children = append_data.child_pointers.data();
    		result->n_children = 1;
    		append_data.child_arrays[0] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[0]));
    	}
    
    public:
    	static void AppendOffsets(ArrowAppendData &append_data, vector::unified_vector_format &format, uint64_t from, uint64_t to,
    	                          std::vector<uint32_t> &child_sel) {
    		// resize the offset buffer - the offset buffer holds the offsets into the child array
    		uint64_t size = to - from;
    		auto &main_buffer = append_data.GetMainBuffer();
    
    		main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
    		auto data = format.get_data<types::list_entry_t>();
    		auto offset_data = main_buffer.GetData<BUFTYPE>();
    		if (append_data.row_count == 0) {
    			// first entry
    			offset_data[0] = 0;
    		}
    		// set up the offsets using the list entries
    		auto last_offset = offset_data[append_data.row_count];
    		for (uint64_t i = from; i < to; i++) {
    			auto source_idx = format.referenced_indexing->get_index(i);
    			auto offset_idx = append_data.row_count + i + 1 - from;
    
    			if (!format.validity.row_is_valid(source_idx)) {
    				offset_data[offset_idx] = last_offset;
    				continue;
    			}
    
    			// append the offset data
    			auto list_length = data[source_idx].length;
    			if (std::is_same<BUFTYPE, int32_t>::value == true &&
    			    (uint64_t)last_offset + list_length > std::numeric_limits<int32_t>::max()) {
                    auto limit = std::to_string(std::numeric_limits<int32_t>::max());
                    auto last_offset_str = std::to_string(last_offset);
    				throw std::runtime_error(
    				    "Arrow Appender: The maximum combined list offset for regular list buffers is " +
    				    limit+" but the offset of "+last_offset_str+" exceeds this.\n* SET arrow_large_buffer_size=true to use large list "+
    				    "buffers");
    			}
    			last_offset += list_length;
    			offset_data[offset_idx] = last_offset;
    
    			for (uint64_t k = 0; k < list_length; k++) {
    				child_sel.push_back(static_cast<uint32_t>(data[source_idx].offset + k));
    			}
    		}
    	}
    };

} // namespace components::arrow::appender
