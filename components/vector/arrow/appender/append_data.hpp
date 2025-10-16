#pragma once

#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_buffer.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <array>
#include <memory>
#include <vector>

namespace components::vector::arrow::appender {

    struct arrow_append_data_t;

    static void bit_position(uint64_t row_idx, uint64_t& current_byte, uint8_t& current_bit) {
        current_byte = row_idx / 8;
        current_bit = row_idx % 8;
    }

    static void unset_bit(uint8_t* data, uint64_t current_byte, uint8_t current_bit) {
        data[current_byte] &= ~((uint64_t) 1 << current_bit);
    }

    static void next_bit(uint64_t& current_byte, uint8_t& current_bit) {
        current_bit++;
        if (current_bit == 8) {
            current_byte++;
            current_bit = 0;
        }
    }

    typedef void (*initialize_t)(arrow_append_data_t& result,
                                 const types::complex_logical_type& type,
                                 uint64_t capacity);
    typedef void (*append_vector_t)(arrow_append_data_t& append_data,
                                    vector_t& input,
                                    uint64_t from,
                                    uint64_t to,
                                    uint64_t input_size);
    typedef void (*finalize_t)(arrow_append_data_t& append_data,
                               const types::complex_logical_type& type,
                               ArrowArray* result);

    struct arrow_append_data_t {
        explicit arrow_append_data_t() {
            dictionary.release = nullptr;
            arrow_buffers_.resize(3);
        }

        arrow_buffer_t& validity_buffer() { return arrow_buffers_[0]; }

        arrow_buffer_t& main_buffer() { return arrow_buffers_[1]; }

        arrow_buffer_t& auxiliary_buffer() { return arrow_buffers_[2]; }

        arrow_buffer_t& buffer_size_buffer() {
            if (arrow_buffers_.size() == 3) {
                arrow_buffers_.resize(4);
            }
            return arrow_buffers_[3];
        }

        void set_null(uint8_t* validity_data, uint64_t current_byte, uint8_t current_bit) {
            unset_bit(validity_data, current_byte, current_bit);
            null_count++;
        }

        void add_validity(unified_vector_format& format, uint64_t from, uint64_t to) {
            uint64_t size = to - from;
            validity_buffer().resize_validity(row_count + size);
            if (format.validity.all_valid()) {
                return;
            }

            auto validity_data = validity_buffer().data();
            uint8_t current_bit;
            uint64_t current_byte;
            bit_position(row_count, current_byte, current_bit);
            for (uint64_t i = from; i < to; i++) {
                auto source_idx = format.referenced_indexing->get_index(i);
                if (!format.validity.row_is_valid(source_idx)) {
                    set_null(validity_data, current_byte, current_bit);
                }
                next_bit(current_byte, current_bit);
            }
        }

        uint64_t row_count = 0;
        uint64_t null_count = 0;

        initialize_t initialize = nullptr;
        append_vector_t append_vector = nullptr;
        finalize_t finalize = nullptr;

        std::vector<std::unique_ptr<arrow_append_data_t>> child_data;

        std::unique_ptr<ArrowArray> array;
        std::array<const void*, 4> buffers = {{nullptr, nullptr, nullptr, nullptr}};
        std::vector<ArrowArray*> child_pointers;
        std::vector<ArrowArray> child_arrays;
        ArrowArray dictionary;

        uint64_t offset = 0;

    private:
        std::vector<arrow_buffer_t> arrow_buffers_;
    };

} // namespace components::vector::arrow::appender
