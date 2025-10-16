#pragma once

#include "append_data.hpp"
#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_appender.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <cassert>

namespace components::vector::arrow::appender {

    struct arrow_scalar_converter_t {
        template<class TGT, class SRC>
        static TGT operation(SRC input) {
            return input;
        }

        static bool skip_nulls() { return false; }

        template<class TGT>
        static void set_null(TGT& value) {}
    };

    template<class TGT, class SRC = TGT, class OP = arrow_scalar_converter_t>
    struct arrow_scalar_base_data_t {
        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            assert(to >= from);
            uint64_t size = to - from;
            assert(size <= input_size);
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);

            append_data.add_validity(format, from, to);

            auto& main_buffer = append_data.main_buffer();
            main_buffer.resize(main_buffer.size() + sizeof(TGT) * size);
            auto data = format.get_data<SRC>();
            auto result_data = main_buffer.data<TGT>();

            for (uint64_t i = from; i < to; i++) {
                auto source_idx = format.referenced_indexing->get_index(i);
                auto result_idx = append_data.row_count + i - from;

                if (OP::skip_nulls() && !format.validity.row_is_valid(source_idx)) {
                    OP::template set_null<TGT>(result_data[result_idx]);
                    continue;
                }
                result_data[result_idx] = OP::template operation<TGT, SRC>(data[source_idx]);
            }
            append_data.row_count += size;
        }
    };

    template<class TGT, class SRC = TGT, class OP = arrow_scalar_converter_t>
    struct arrow_scala_data : public arrow_scalar_base_data_t<TGT, SRC, OP> {
        static void
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            result.main_buffer().reserve(capacity * sizeof(TGT));
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();
        }
    };

} // namespace components::vector::arrow::appender
