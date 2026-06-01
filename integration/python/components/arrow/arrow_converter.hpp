#pragma once

#include "arrow.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <string>
#include <vector>

namespace components::arrow {

    struct ArrowConverter {
        static void ToArrowSchema(ArrowSchema *out_schema, const std::vector<types::complex_logical_type> &types,
    	                                     const std::vector<std::string> &names);
    	static void ToArrowArray(vector::data_chunk_t &input, ArrowArray *out_array);
    };

} // namespace components::arrow
