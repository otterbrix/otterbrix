#pragma once

#include "append_data.hpp"
#include <components/vector/arrow/arrow_appender.hpp>

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace components::vector::arrow::appender {

    template<class BUFTYPE = int64_t>
    struct arrow_list_data_t {
        static void
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            auto& child_type = type.child_type();
            result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            auto child_buffer = arrow_appender_t::initialize_child(child_type, capacity);
            result.child_data.push_back(std::move(child_buffer));
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            uint64_t size = to - from;
            std::vector<uint64_t> child_indices;
            append_data.add_validity(format, from, to);
            append_offsets(append_data, format, from, to, child_indices);

            indexing_vector_t child_indexing(input.resource(), child_indices.data());
            auto& child = input.entry();
            auto child_size = child_indices.size();
            vector_t child_copy(child.resource(), child.type());
            child_copy.slice(child, child_indexing, child_size);
            append_data.child_data[0]->append_vector(*append_data.child_data[0], child_copy, 0, child_size, child_size);
            append_data.row_count += size;
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();

            auto& child_type = type.child_type();
            arrow_appender_t::add_children(append_data, 1);
            result->children = append_data.child_pointers.data();
            result->n_children = 1;
            append_data.child_arrays[0] =
                *arrow_appender_t::finalize_child(child_type, std::move(append_data.child_data[0]));
        }

        static void append_offsets(arrow_append_data_t& append_data,
                                   unified_vector_format& format,
                                   uint64_t from,
                                   uint64_t to,
                                   std::vector<uint64_t>& child_indexing) {
            uint64_t size = to - from;
            auto& main_buffer = append_data.main_buffer();

            main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
            auto data = format.get_data<types::list_entry_t>();
            auto offset_data = main_buffer.data<BUFTYPE>();
            if (append_data.row_count == 0) {
                offset_data[0] = 0;
            }
            auto last_offset = offset_data[append_data.row_count];
            for (uint64_t i = from; i < to; i++) {
                auto source_idx = format.referenced_indexing->get_index(i);
                auto offset_idx = append_data.row_count + i + 1 - from;

                if (!format.validity.row_is_valid(source_idx)) {
                    offset_data[offset_idx] = last_offset;
                    continue;
                }

                auto list_length = data[source_idx].length;
                if (std::is_same<BUFTYPE, int32_t>::value == true &&
                    (uint64_t) last_offset + list_length > std::numeric_limits<int32_t>::max()) {
                    auto limit = std::to_string(std::numeric_limits<int32_t>::max());
                    auto last_offset_str = std::to_string(last_offset);
                    throw std::runtime_error(
                        "Arrow Appender: The maximum combined list offset for regular list buffers is " + limit +
                        " but the offset of " + last_offset_str +
                        " exceeds this.\n* SET arrow_large_buffer_size=true to use large list " + "buffers");
                }
                last_offset += list_length;
                offset_data[offset_idx] = last_offset;

                for (uint64_t k = 0; k < list_length; k++) {
                    child_indexing.push_back(static_cast<uint32_t>(data[source_idx].offset + k));
                }
            }
        }
    };

} // namespace components::vector::arrow::appender
