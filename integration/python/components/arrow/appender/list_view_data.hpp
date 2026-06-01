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
    struct ArrowListViewData {
    public:
    	static void Initialize(ArrowAppendData &result, const complex_logical_type &type, uint64_t capacity) {
    		auto &child_type = type.child_type();
    		result.GetMainBuffer().reserve(capacity * sizeof(BUFTYPE));
    		result.GetAuxBuffer().reserve(capacity * sizeof(BUFTYPE));
    
    		auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity);
    		result.child_data.push_back(std::move(child_buffer));
    	}
    
    	static void Append(ArrowAppendData &append_data, vector::vector_t &input, 
                uint64_t from, uint64_t to, uint64_t input_size) {
            vector::unified_vector_format format;
    		input.to_unified_format(input_size, format);
    		uint64_t size = to - from;
            std::vector<uint32_t> child_indices;
    		AppendValidity(append_data, format, from, to);
    		AppendListMetadata(append_data, format, from, to, child_indices);
    
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
    		result->n_buffers = 3;
    		result->buffers[1] = append_data.GetMainBuffer().data();
    		result->buffers[2] = append_data.GetAuxBuffer().data();
    
    		auto &child_type = type.child_type(); 
    		ArrowAppender::AddChildren(append_data, 1);
    		result->children = append_data.child_pointers.data();
    		result->n_children = 1;
    		append_data.child_arrays[0] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[0]));
    	}
    
    public:
    	static void AppendListMetadata(ArrowAppendData &append_data, vector::unified_vector_format &format, uint64_t from, uint64_t to,
    	                               std::vector<uint32_t> &child_sel) {
    		// resize the offset buffer - the offset buffer holds the offsets into the child array
    		uint64_t size = to - from;
    		append_data.GetMainBuffer().resize(append_data.GetMainBuffer().size() + sizeof(BUFTYPE) * size);
    		append_data.GetAuxBuffer().resize(append_data.GetAuxBuffer().size() + sizeof(BUFTYPE) * size);
    		auto data = format.get_data<types::list_entry_t>();
    		auto offset_data = append_data.GetMainBuffer().GetData<BUFTYPE>();
    		auto size_data = append_data.GetAuxBuffer().GetData<BUFTYPE>();
    
    		BUFTYPE last_offset =
    		    append_data.row_count ? offset_data[append_data.row_count - 1] + size_data[append_data.row_count - 1] : 0;
    		for (uint64_t i = 0; i < size; i++) {
    			auto source_idx = format.referenced_indexing->get_index(i + from);
    			auto offset_idx = append_data.row_count + i;
    
    			if (!format.validity.row_is_valid(source_idx)) {
    				offset_data[offset_idx] = last_offset;
    				size_data[offset_idx] = 0;
    				continue;
    			}
    
    			// append the offset data
    			auto list_length = data[source_idx].length;
    			if (std::is_same<BUFTYPE, int32_t>::value == true &&
    			    (uint64_t)last_offset + list_length > std::numeric_limits<int32_t>::max()) {
                    auto limit = std::to_string(std::numeric_limits<int32_t>::max());
                    auto last_offset_str = std::to_string(last_offset);
    				throw std::runtime_error(
    				    "Arrow Appender: The maximum combined list offset for regular list buffers is "+
    				    limit+" but the offset of "+last_offset_str+" exceeds this.\n* SET arrow_large_buffer_size=true to use large list "+
    				    "buffers");
    			}
    			offset_data[offset_idx] = last_offset;
    			size_data[offset_idx] = static_cast<BUFTYPE>(list_length);
    			last_offset += list_length;
    
    			for (uint64_t k = 0; k < list_length; k++) {
    				child_sel.push_back(static_cast<uint32_t>(data[source_idx].offset + k));
    			}
    		}
    	}
    };

} // namespace components::arrow::appender
