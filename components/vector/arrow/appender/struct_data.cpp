#include "struct_data.hpp"

#include <components/vector/arrow/arrow_appender.hpp>

namespace components::vector::arrow::appender {
    using types::complex_logical_type;

    void arrow_struct_data_t::initialize(arrow_append_data_t& result,
                                         const types::complex_logical_type& type,
                                         uint64_t capacity) {
        auto& children = type.child_types();
        for (auto& child : children) {
            auto child_buffer = arrow_appender_t::initialize_child(child, capacity);
            result.child_data.push_back(std::move(child_buffer));
        }
    }

    void arrow_struct_data_t::append(arrow_append_data_t& append_data,
                                     vector_t& input,
                                     uint64_t from,
                                     uint64_t to,
                                     uint64_t input_size) {
        unified_vector_format format(input.resource(), input_size);
        input.to_unified_format(input_size, format);
        uint64_t size = to - from;
        append_data.add_validity(format, from, to);
        auto& children = input.entries();
        for (uint64_t child_idx = 0; child_idx < children.size(); child_idx++) {
            auto& child = children[child_idx];
            auto& child_data = *append_data.child_data[child_idx];
            child_data.append_vector(child_data, *child, from, to, size);
        }
        append_data.row_count += size;
    }

    void arrow_struct_data_t::finalize(arrow_append_data_t& append_data,
                                       const types::complex_logical_type& type,
                                       ArrowArray* result) {
        result->n_buffers = 1;

        auto& child_types = type.child_types();
        arrow_appender_t::add_children(append_data, child_types.size());
        result->children = append_data.child_pointers.data();
        result->n_children = static_cast<int64_t>(child_types.size());
        for (uint64_t i = 0; i < child_types.size(); i++) {
            auto& child_type = child_types[i];
            append_data.child_arrays[i] =
                *arrow_appender_t::finalize_child(child_type, std::move(append_data.child_data[i]));
        }
    }

} // namespace components::vector::arrow::appender
