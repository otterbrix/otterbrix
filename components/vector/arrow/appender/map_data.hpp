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
        static void
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            auto map_extension = static_cast<types::map_logical_type_extension*>(type.extension());

            auto& key_type = map_extension->key();
            auto& value_type = map_extension->value();
            auto internal_struct = std::make_unique<arrow_append_data_t>();
            internal_struct->child_data.push_back(arrow_appender_t::initialize_child(key_type, capacity));
            internal_struct->child_data.push_back(arrow_appender_t::initialize_child(value_type, capacity));

            result.child_data.push_back(std::move(internal_struct));
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            uint64_t size = to - from;
            append_data.add_validity(format, from, to);
            std::vector<uint64_t> child_indices;
            arrow_list_data_t<BUFTYPE>::append_offsets(append_data, format, from, to, child_indices);

            indexing_vector_t child_indexing(input.resource(), child_indices.data());
            auto& key_vector = input.entries().at(0);
            auto& value_vector = input.entries().at(1);
            auto list_size = child_indices.size();

            auto& struct_data = *append_data.child_data[0];
            auto& key_data = *struct_data.child_data[0];
            auto& value_data = *struct_data.child_data[1];

            vector_t key_vector_copy(key_vector->resource(), key_vector->type());
            key_vector_copy.slice(*key_vector, child_indexing, list_size);
            vector_t value_vector_copy(value_vector->resource(), value_vector->type());
            value_vector_copy.slice(*value_vector, child_indexing, list_size);
            key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
            value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);

            append_data.row_count += size;
            struct_data.row_count += size;
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            assert(result);
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();

            arrow_appender_t::add_children(append_data, 1);
            result->children = append_data.child_pointers.data();
            result->n_children = 1;

            auto& struct_data = *append_data.child_data[0];
            auto struct_result = arrow_appender_t::finalize_child(type, std::move(append_data.child_data[0]));

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
            auto key_data = arrow_appender_t::finalize_child(key_type, std::move(struct_data.child_data[0]));
            struct_data.child_arrays[0] = *key_data;
            struct_data.child_arrays[1] =
                *arrow_appender_t::finalize_child(value_type, std::move(struct_data.child_data[1]));
            if (key_data->null_count > 0) {
                throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
            }
        }
    };

} // namespace components::vector::arrow::appender
