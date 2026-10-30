#pragma once

#include "append_data.hpp"
#include <components/vector/arrow/arrow_appender.hpp>

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include <cassert>
#include <memory>
#include <vector>

namespace components::vector::arrow::appender {

    template<class BUFTYPE = int64_t>
    struct arrow_map_data_t {
        static core::error_t
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            auto map_extension = static_cast<types::map_logical_type_extension*>(type.extension());

            auto& key_type = map_extension->key();
            auto& value_type = map_extension->value();
            auto internal_struct = std::make_unique<arrow_append_data_t>(result.resource);
            auto key_data = arrow_appender_t::initialize_child(result.resource, key_type, capacity);
            if (key_data.has_error()) {
                return core::error_on(result.resource, key_data.error());
            }
            internal_struct->child_data.push_back(std::move(key_data.value()));
            auto value_data = arrow_appender_t::initialize_child(result.resource, value_type, capacity);
            if (value_data.has_error()) {
                return core::error_on(result.resource, value_data.error());
            }
            internal_struct->child_data.push_back(std::move(value_data.value()));

            result.child_data.push_back(std::move(internal_struct));
            return core::error_t::no_error();
        }

        static core::error_t
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            uint64_t size = to - from;
            append_data.add_validity(format, from, to);
            std::vector<uint64_t> child_indices;
            if (auto err = arrow_list_data_t<BUFTYPE>::append_offsets(append_data, format, from, to, child_indices);
                err.contains_error()) {
                return err;
            }

            indexing_vector_t child_indexing(input.resource(), child_indices.data());
            auto& entries = input.entry().entries();
            auto& key_vector = entries[0];
            auto& value_vector = entries[1];
            auto list_size = child_indices.size();

            auto& struct_data = *append_data.child_data[0];
            auto& key_data = *struct_data.child_data[0];
            auto& value_data = *struct_data.child_data[1];

            vector_t key_vector_copy(key_vector->resource(), key_vector->type());
            key_vector_copy.slice(*key_vector, child_indexing, list_size);
            vector_t value_vector_copy(value_vector->resource(), value_vector->type());
            value_vector_copy.slice(*value_vector, child_indexing, list_size);
            if (auto err = key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
                err.contains_error()) {
                return err;
            }
            if (auto err = value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);
                err.contains_error()) {
                return err;
            }

            append_data.row_count += size;
            struct_data.row_count += size;
            return core::error_t::no_error();
        }

        static core::error_t
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            assert(result);
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();

            arrow_appender_t::add_children(append_data, 1);
            result->children = append_data.child_pointers.data();
            result->n_children = 1;

            auto& struct_data = *append_data.child_data[0];
            auto struct_child = arrow_appender_t::finalize_child(type, std::move(append_data.child_data[0]));
            if (struct_child.has_error()) {
                return core::error_on(append_data.resource, struct_child.error());
            }
            auto struct_result = struct_child.value();

            const auto struct_child_count = 2;
            arrow_appender_t::add_children(struct_data, struct_child_count);
            struct_result->children = struct_data.child_pointers.data();
            struct_result->n_buffers = 1;
            struct_result->n_children = struct_child_count;
            struct_result->length = static_cast<int64_t>(struct_data.child_data[0]->row_count);

            append_data.child_arrays[0] = *struct_result;

            assert(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);

            auto map_extension = static_cast<types::map_logical_type_extension*>(type.extension());
            auto& key_type = map_extension->key();
            auto& value_type = map_extension->value();
            auto key_child = arrow_appender_t::finalize_child(key_type, std::move(struct_data.child_data[0]));
            if (key_child.has_error()) {
                return core::error_on(append_data.resource, key_child.error());
            }
            auto key_data = key_child.value();
            struct_data.child_arrays[0] = *key_data;
            auto value_child = arrow_appender_t::finalize_child(value_type, std::move(struct_data.child_data[1]));
            if (value_child.has_error()) {
                return core::error_on(append_data.resource, value_child.error());
            }
            struct_data.child_arrays[1] = *value_child.value();
            if (key_data->null_count > 0) {
                return core::error_t(core::error_code_t::null_value_not_allowed,
                                     std::pmr::string("Arrow doesn't accept NULL keys on Maps", append_data.resource));
            }
            return core::error_t::no_error();
        }
    };

} // namespace components::vector::arrow::appender
