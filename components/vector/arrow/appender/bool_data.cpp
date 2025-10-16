#include "bool_data.hpp"

#include "append_data.hpp"

namespace components::vector::arrow::appender {

    void arrow_bool_data_t::initialize(arrow_append_data_t& result,
                                       const types::complex_logical_type& type,
                                       uint64_t capacity) {
        auto byte_count = (capacity + 7) / 8;
        result.main_buffer().reserve(byte_count);
    }

    void arrow_bool_data_t::append(arrow_append_data_t& append_data,
                                   vector_t& input,
                                   uint64_t from,
                                   uint64_t to,
                                   uint64_t input_size) {
        uint64_t size = to - from;
        unified_vector_format format(input.resource(), input_size);
        input.to_unified_format(input_size, format);
        auto& main_buffer = append_data.main_buffer();
        auto& validity_buffer = append_data.validity_buffer();
        validity_buffer.resize_validity(append_data.row_count + size);
        main_buffer.resize_validity(append_data.row_count + size);
        auto data = format.get_data<bool>();

        auto result_data = main_buffer.data<uint8_t>();
        auto validity_data = validity_buffer.data<uint8_t>();
        uint8_t current_bit;
        uint64_t current_byte;
        bit_position(append_data.row_count, current_byte, current_bit);
        for (uint64_t i = from; i < to; i++) {
            auto source_idx = format.referenced_indexing->get_index(i);
            // append the validity mask
            if (!format.validity.row_is_valid(source_idx)) {
                append_data.set_null(validity_data, current_byte, current_bit);
            } else if (!data[source_idx]) {
                unset_bit(result_data, current_byte, current_bit);
            }
            next_bit(current_byte, current_bit);
        }
        append_data.row_count += size;
    }

    void arrow_bool_data_t::finalize(arrow_append_data_t& append_data,
                                     const types::complex_logical_type& type,
                                     ArrowArray* result) {
        result->n_buffers = 2;
        result->buffers[1] = append_data.main_buffer().data();
    }

} // namespace components::vector::arrow::appender
