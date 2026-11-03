#include "fixed_size_list_data.hpp"

#include <components/vector/arrow/arrow_appender.hpp>

namespace components::vector::arrow::appender {
    using types::complex_logical_type;

    core::error_t arrow_fixed_size_list_data_t::initialize(arrow_append_data_t& result,
                                                           const types::complex_logical_type& type,
                                                           uint64_t capacity) {
        auto array_extension = static_cast<types::array_logical_type_extension*>(type.extension());
        auto array_size = array_extension->size();
        auto& child_type = array_extension->internal_type();

        auto child_buffer = arrow_appender_t::initialize_child(result.resource, child_type, capacity * array_size);
        if (child_buffer.has_error()) {
            return core::error_on(result.resource, child_buffer.error());
        }
        result.child_data.push_back(std::move(child_buffer.value()));
        return core::error_t::no_error();
    }

    core::error_t arrow_fixed_size_list_data_t::append(arrow_append_data_t& append_data,
                                                       vector_t& input,
                                                       uint64_t from,
                                                       uint64_t to,
                                                       uint64_t input_size) {
        unified_vector_format format(input.resource(), input_size);
        input.to_unified_format(input_size, format);
        uint64_t size = to - from;
        append_data.add_validity(format, from, to);
        input.flatten(input_size);

        auto array_extension = static_cast<types::array_logical_type_extension*>(input.type().extension());
        auto array_size = array_extension->size();
        auto& child_vector = input.entry();
        auto& child_data = *append_data.child_data[0];
        if (auto err = child_data.append_vector(child_data,
                                                child_vector,
                                                from * array_size,
                                                to * array_size,
                                                size * array_size);
            err.contains_error()) {
            return err;
        }
        append_data.row_count += size;
        return core::error_t::no_error();
    }

    core::error_t arrow_fixed_size_list_data_t::finalize(arrow_append_data_t& append_data,
                                                         const types::complex_logical_type& type,
                                                         ArrowArray* result) {
        result->n_buffers = 1;
        auto& child_type = type.child_type();
        arrow_appender_t::add_children(append_data, 1);
        result->children = append_data.child_pointers.data();
        result->n_children = 1;
        auto child = arrow_appender_t::finalize_child(child_type, std::move(append_data.child_data[0]));
        if (child.has_error()) {
            return core::error_on(append_data.resource, child.error());
        }
        append_data.child_arrays[0] = *child.value();
        return core::error_t::no_error();
    }

} // namespace components::vector::arrow::appender
