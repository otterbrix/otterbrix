#pragma once

#include <components/vector/arrow/appender/append_data.hpp>
#include <components/vector/arrow/appender/scalar_data.hpp>
#include <components/vector/arrow/arrow_string_view.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow::appender {

    template<class SRC = std::string_view, class BUFTYPE = int64_t>
    struct arrow_string_data_t {
        static void
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            result.auxiliary_buffer().reserve(capacity);
        }

        template<bool LARGE_STRING>
        static void
        append_templated(arrow_append_data_t& append_data, vector_t& input, size_t from, size_t to, size_t input_size) {
            size_t size = to - from;
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            auto& main_buffer = append_data.main_buffer();
            auto& validity_buffer = append_data.validity_buffer();
            auto& aux_buffer = append_data.auxiliary_buffer();

            validity_buffer.resize_validity(append_data.row_count + size);
            auto validity_data = (uint8_t*) validity_buffer.data();

            main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
            auto data = format.get_data<SRC>();
            auto offset_data = main_buffer.data<BUFTYPE>();
            if (append_data.row_count == 0) {
                offset_data[0] = 0;
            }
            auto last_offset = offset_data[append_data.row_count];
            for (size_t i = from; i < to; i++) {
                auto source_idx = format.referenced_indexing->get_index(i);
                auto offset_idx = append_data.row_count + i + 1 - from;

                if (!format.validity.row_is_valid(source_idx)) {
                    uint8_t current_bit;
                    uint64_t current_byte;
                    bit_position(append_data.row_count + i - from, current_byte, current_bit);
                    append_data.set_null(validity_data, current_byte, current_bit);
                    offset_data[offset_idx] = last_offset;
                    continue;
                }

                auto string_length = data[source_idx].size();

                auto current_offset = static_cast<size_t>(last_offset) + string_length;
                offset_data[offset_idx] = static_cast<BUFTYPE>(current_offset);

                aux_buffer.resize(current_offset);
                std::memcpy(aux_buffer.data() + last_offset, data[source_idx].data(), data[source_idx].size());

                last_offset = static_cast<BUFTYPE>(current_offset);
            }
            append_data.row_count += size;
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            append_templated<false>(append_data, input, from, to, input_size);
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            result->n_buffers = 3;
            result->buffers[1] = append_data.main_buffer().data();
            result->buffers[2] = append_data.auxiliary_buffer().data();
        }
    };

} // namespace components::vector::arrow::appender