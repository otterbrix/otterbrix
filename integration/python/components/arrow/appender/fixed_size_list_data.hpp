#pragma once

#include "append_data.hpp"
#include "../arrow.hpp"
#include "../arrow_appender.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

namespace components::arrow::appender {

    struct ArrowFixedSizeListData {
    public:
	    static void Initialize(ArrowAppendData &result, const types::complex_logical_type &type, uint64_t capacity);
	    static void Append(ArrowAppendData &append_data, vector::vector_t &input, uint64_t from, uint64_t to, uint64_t input_size);
	    static void Finalize(ArrowAppendData &append_data, const types::complex_logical_type &type, ArrowArray *result);
    };

} // namespace components::arrow::appender
