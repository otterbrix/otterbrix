#pragma once

#include "append_data.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow::appender {

    struct arrow_fixed_size_list_data_t {
        [[nodiscard]] static core::error_t
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity);
        [[nodiscard]] static core::error_t
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size);
        [[nodiscard]] static core::error_t
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result);
    };

} // namespace components::vector::arrow::appender
