#pragma once

#include "arrow.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory>
#include <vector>

// todo move to parameters
constexpr bool arrow_use_list_view = false;
enum class ArrowOffsetSize : uint8_t { REGULAR, LARGE };
constexpr ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;

namespace components::arrow {

    namespace appender {
        struct ArrowAppendData;
    } 
    
    //! The ArrowAppender class can be used to incrementally construct an arrow array by appending data chunks into it
    class ArrowAppender {
    public:
    	ArrowAppender(std::vector<types::complex_logical_type> types_p, const uint64_t initial_capacity);
    	~ArrowAppender();
    
    public:
    	//! Append a data chunk to the underlying arrow array
    	void Append(vector::data_chunk_t &input, uint64_t from, uint64_t to, uint64_t input_size);
    	//! Returns the underlying arrow array
    	ArrowArray Finalize();
    	uint64_t RowCount() const;
    	static void ReleaseArray(ArrowArray *array);
    	static ArrowArray *FinalizeChild(const types::complex_logical_type &type, std::unique_ptr<appender::ArrowAppendData> append_data_p);
    	static std::unique_ptr<appender::ArrowAppendData> InitializeChild(const types::complex_logical_type &type, const uint64_t capacity);
    	static void AddChildren(appender::ArrowAppendData &data, const uint64_t count);
    
    private:
    	//! The types of the chunks that will be appended in
    	std::vector<types::complex_logical_type> types;
    	//! The root arrow append data
    	std::vector<std::unique_ptr<appender::ArrowAppendData>> root_data;
    	//! The total row count that has been appended
    	uint64_t row_count = 0;
    };
} // namespace components::arrow

