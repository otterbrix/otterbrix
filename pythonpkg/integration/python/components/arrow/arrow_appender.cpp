#include "arrow_appender.hpp"

#include "appender/append_data.hpp"
#include "appender/list.hpp"

#include <absl/numeric/int128.h>

#include <cassert>
#include <stdexcept>
#include <vector>


namespace components::arrow {
    using components::arrow::appender::ArrowAppendData;
    using components::types::physical_type;
    using components::types::logical_type;
    using components::types::complex_logical_type;

    //===--------------------------------------------------------------------===//
    // ArrowAppender
    //===--------------------------------------------------------------------===//
    
    ArrowAppender::ArrowAppender(std::vector<complex_logical_type> types_p, const uint64_t initial_capacity) 
        : types(std::move(types_p)) {
        for (auto &type : types) {
            auto entry = InitializeChild(type, initial_capacity);
            root_data.push_back(std::move(entry));
        }
    }
    
    ArrowAppender::~ArrowAppender() {
    }
    
    //! Append a data chunk to the underlying arrow array
    void ArrowAppender::Append(components::vector::data_chunk_t &input, 
            uint64_t from, uint64_t to, uint64_t input_size) {
        assert(types == input.types());
        assert(to >= from);
        for (uint64_t i = 0; i < input.column_count(); i++) {
            root_data[i]->append_vector(*root_data[i], input.data[i], from, to, input_size);
        }
        row_count += to - from;
    }
    
    uint64_t ArrowAppender::RowCount() const {
        return row_count;
    }
    
    void ArrowAppender::ReleaseArray(ArrowArray *array) {
        if (!array || !array->release) {
            return;
        }
        auto holder = static_cast<ArrowAppendData*>(array->private_data);
        for (int64_t i = 0; i < array->n_children; i++) {
            auto child = array->children[i];
            if (!child->release) {
                // Child was moved out of the array
                continue;
            }
            child->release(child);
            assert(!child->release);
        }
        if (array->dictionary && array->dictionary->release) {
            array->dictionary->release(array->dictionary);
        }
        array->release = nullptr;
        delete holder;
    }
    
    //===--------------------------------------------------------------------===//
    // Finalize Arrow Child
    //===--------------------------------------------------------------------===//
    ArrowArray *ArrowAppender::FinalizeChild(const complex_logical_type &type, std::unique_ptr<ArrowAppendData> append_data_p) {
        auto result = std::make_unique<ArrowArray>();
    
        auto &append_data = *append_data_p;
        result->private_data = append_data_p.release();
        result->release = ReleaseArray;
        result->n_children = 0;
        result->null_count = 0;
        result->offset = 0;
        result->dictionary = nullptr;
        result->buffers = append_data.buffers.data();
        result->null_count = static_cast<int64_t>(append_data.null_count);
        result->length = static_cast<int64_t>(append_data.row_count);
        result->buffers[0] = append_data.GetValidityBuffer().data();
    
        if (append_data.finalize) {
            append_data.finalize(append_data, type, result.get());
        }
    
        append_data.array = std::move(result);
        return append_data.array.get();
    }
    
    //! Returns the underlying arrow array
    ArrowArray ArrowAppender::Finalize() {
        assert(root_data.size() == types.size());
        auto root_holder = std::make_unique<ArrowAppendData>();
        
        ArrowArray result;
        AddChildren(*root_holder, types.size());
        result.children = root_holder->child_pointers.data();
        result.n_children = static_cast<int64_t>(types.size());
        
        // Configure root array
        result.length = static_cast<int64_t>(row_count);
        result.n_buffers = 1;
        result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
        result.offset = 0;
        result.null_count = 0; // needs to be 0
        result.dictionary = nullptr;
        root_holder->child_data = std::move(root_data);
        
        for (uint64_t i = 0; i < root_holder->child_data.size(); i++) {
            root_holder->child_arrays[i] = *ArrowAppender::FinalizeChild(types[i], std::move(root_holder->child_data[i]));
        }
        
        // Release ownership to caller
        result.private_data = root_holder.release();
        result.release = ArrowAppender::ReleaseArray;
        return result;
    }
    
    //===--------------------------------------------------------------------===//
    // Initialize Arrow Child
    //===--------------------------------------------------------------------===//
    
    template <class OP>
    static void InitializeAppenderForType(ArrowAppendData &append_data) {
        append_data.initialize = OP::Initialize;
        append_data.append_vector = OP::Append;
    	append_data.finalize = OP::Finalize;
    }
    
    static void InitializeFunctionPointers(ArrowAppendData &append_data, const complex_logical_type &type) {
    	// handle special logical types
    	switch (type.type()) {
    	case logical_type::BOOLEAN:
    		InitializeAppenderForType<appender::ArrowBoolData>(append_data);
    		break;
    	case logical_type::TINYINT:
    		InitializeAppenderForType<appender::ArrowScalarData<int8_t>>(append_data);
    		break;
    	case logical_type::SMALLINT:
    		InitializeAppenderForType<appender::ArrowScalarData<int16_t>>(append_data);
    		break;
    	// case logical_type::DATE:
    	case logical_type::INTEGER:
    		InitializeAppenderForType<appender::ArrowScalarData<int32_t>>(append_data);
    		break;
    	/*case logical_type::TIME_TZ: {
    		if (append_data.options.arrow_lossless_conversion) {
    			InitializeAppenderForType<appender::ArrowScalarData<int64_t>>(append_data);
    		} else {
    			InitializeAppenderForType<appender::ArrowScalarData<int64_t, dtime_tz_t, ArrowTimeTzConverter>>(append_data);
    		}
    		break;
    	}
    	case logical_type::TIME:*/
    	case logical_type::TIMESTAMP_SEC:
    	case logical_type::TIMESTAMP_MS:
    	case logical_type::TIMESTAMP_US:
    	case logical_type::TIMESTAMP_NS:
    	// case logical_type::TIMESTAMP_TZ:
    	case logical_type::BIGINT:
    		InitializeAppenderForType<appender::ArrowScalarData<int64_t>>(append_data);
    		break;
    	/* case logical_type::UUID:
    		if (append_data.options.arrow_lossless_conversion) {
    			InitializeAppenderForType<appender::ArrowScalarData<absl::int128, absl::int128, ArrowUUIDBlobConverter>>(append_data);
    		} else {
    			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
    				InitializeAppenderForType<appender::ArrowVarcharData<absl::int128, ArrowUUIDConverter>>(append_data);
    			} else {
    				InitializeAppenderForType<appender::ArrowVarcharData<absl::int128, ArrowUUIDConverter, int32_t>>(append_data);
    			}
    		}
    		break;
    	case logical_type::HUGEINT:
    		InitializeAppenderForType<appender::ArrowScalarData<absl::int128>>(append_data);
    		break;
    	case logical_type::UHUGEINT:
    		InitializeAppenderForType<appender::ArrowScalarData<absl::uint128>>(append_data);
    		break;*/
    	case logical_type::UTINYINT:
    		InitializeAppenderForType<appender::ArrowScalarData<uint8_t>>(append_data);
    		break;
    	case logical_type::USMALLINT:
    		InitializeAppenderForType<appender::ArrowScalarData<uint16_t>>(append_data);
    		break;
    	case logical_type::UINTEGER:
    		InitializeAppenderForType<appender::ArrowScalarData<uint32_t>>(append_data);
    		break;
    	case logical_type::UBIGINT:
    		InitializeAppenderForType<appender::ArrowScalarData<uint64_t>>(append_data);
    		break;
    	case logical_type::FLOAT:
    		InitializeAppenderForType<appender::ArrowScalarData<float>>(append_data);
    		break;
    	case logical_type::DOUBLE:
    		InitializeAppenderForType<appender::ArrowScalarData<double>>(append_data);
    		break;
    	case logical_type::DECIMAL:
    		/*switch (type.InternalType()) {
    		case physical_type::INT16:
    			InitializeAppenderForType<appender::ArrowScalarData<absl::int128, int16_t>>(append_data);
    			break;
    		case physical_type::INT32:
    			InitializeAppenderForType<appender::ArrowScalarData<absl::int128, int32_t>>(append_data);
    			break;*/
    		//case physical_type::INT64:
    			InitializeAppenderForType<appender::ArrowScalarData<absl::int128, int64_t>>(append_data);
    		//	break;
    		/*case physical_type::INT128:
    			InitializeAppenderForType<appender::ArrowScalarData<absl::int128>>(append_data);
    			break;
    		default:
    			throw std::runtime_error("Unsupported internal decimal type");
    		}*/
    		break;
    	/*case logical_type::VARCHAR:
    		if (append_data.options.produce_arrow_string_view) {
    			InitializeAppenderForType<appender::ArrowVarcharToStringViewData>(append_data);
    		} else {
    			if (append_data.options.arrow_offset_size == ArrowOffsetSize::LARGE) {
    				InitializeAppenderForType<appender::ArrowVarcharData<>>(append_data);
    			} else {
    				InitializeAppenderForType<appender::ArrowVarcharData<string_t, ArrowVarcharConverter, int32_t>>(append_data);
    			}
    		}
    		break;
    	case logical_type::BLOB:*/
    	/*case logical_type::BIT:
    		if (arrow_offset_size == ArrowOffsetSize::LARGE) {
    			InitializeAppenderForType<appender::ArrowVarcharData<>>(append_data);
    		} else {
    			InitializeAppenderForType<appender::ArrowVarcharData<string_t, ArrowVarcharConverter, int32_t>>(append_data);
    		}
    		break;*/
    	/*case logical_type::ENUM:
    		switch (type.InternalType()) {
    		case physical_type::UINT8:
    			InitializeAppenderForType<appender::ArrowEnumData<int8_t>>(append_data);
    			break;
    		case physical_type::UINT16:
    			InitializeAppenderForType<appender::ArrowEnumData<int16_t>>(append_data);
    			break;
    		case physical_type::UINT32:
    			InitializeAppenderForType<appender::ArrowEnumData<int32_t>>(append_data);
    			break;
    		default:
    			throw std::runtime_error("Unsupported internal enum type");
    		}
    		break;
    	case logical_type::INTERVAL:
    		InitializeAppenderForType<appender::ArrowScalarData<ArrowInterval, interval_t, ArrowIntervalConverter>>(append_data);
    		break;
    	case logical_type::UNION:
    		InitializeAppenderForType<appender::ArrowUnionData>(append_data);
    		break;*/
    	case logical_type::STRUCT:
    		InitializeAppenderForType<appender::ArrowStructData>(append_data);
    		break;
    	case logical_type::ARRAY:
    		InitializeAppenderForType<appender::ArrowFixedSizeListData>(append_data);
    		break;
    	case logical_type::LIST: {
    		if (arrow_use_list_view) {
    			if (arrow_offset_size == ArrowOffsetSize::LARGE) {
    				InitializeAppenderForType<appender::ArrowListViewData<>>(append_data);
    			} else {
    				InitializeAppenderForType<appender::ArrowListViewData<int32_t>>(append_data);
    			}
    		} else {
    			if (arrow_offset_size == ArrowOffsetSize::LARGE) {
    				InitializeAppenderForType<appender::ArrowListData<>>(append_data);
    			} else {
    				InitializeAppenderForType<appender::ArrowListData<int32_t>>(append_data);
    			}
    		}
    		break;
    	}
    	case logical_type::MAP:
    		// Arrow MapArray only supports 32-bit offsets. There is no LargeMapArray type in Arrow.
    		InitializeAppenderForType<appender::ArrowMapData<int32_t>>(append_data);
    		break;
    	default:
    		throw std::runtime_error("Unsupported type in OtterBrix -> Arrow Conversion: "+std::to_string(int(type.type()));
    	}
    }
    
    std::unique_ptr<ArrowAppendData> ArrowAppender::InitializeChild(const complex_logical_type &type, const uint64_t capacity) {
    	auto result = std::make_unique<ArrowAppendData>();
    	InitializeFunctionPointers(*result, type);
    
    	const auto byte_count = (capacity + 7) / 8;
    	result->GetValidityBuffer().reserve(byte_count);
    	result->initialize(*result, type, capacity);
    	return result;
    }
    
    void ArrowAppender::AddChildren(ArrowAppendData &data, const uint64_t count) {
    	data.child_pointers.resize(count);
    	data.child_arrays.resize(count);
    	for (uint64_t i = 0; i < count; i++) {
    		data.child_pointers[i] = &data.child_arrays[i];
    	}
    }

} // namespace components::arrow
