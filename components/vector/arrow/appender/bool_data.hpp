#pragma once

#include <components/vector/arrow/arrow_appender.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow::appender {

    struct arrow_bool_data_t {
        static void initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity);
        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size);
        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result);
    };

} // namespace components::vector::arrow::appender
