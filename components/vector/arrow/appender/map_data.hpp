#pragma once

#include "append_data.hpp"
#include <components/vector/arrow/arrow_appender.hpp>

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <cassert>
#include <memory>
#include <memory_resource>
#include <vector>

namespace components::vector::arrow::appender {

    template<class BUFTYPE = int64_t>
    struct arrow_map_data_t {
        [[nodiscard]] static core::error_t
        initialize(arrow_append_data_t& result, const types::complex_logical_type& type, uint64_t capacity) {
            if (auto error = result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE)); error.contains_error()) {
                return error;
            }
            auto map_extension = static_cast<types::map_logical_type_extension*>(type.extension());

            auto& key_type = map_extension->key();
            auto& value_type = map_extension->value();
            auto internal_struct = std::make_unique<arrow_append_data_t>();
            auto key_child = arrow_appender_t::initialize_child(key_type, capacity);
            if (key_child.has_error()) {
                return key_child.error();
            }
            internal_struct->child_data.push_back(std::move(key_child.value()));
            auto value_child = arrow_appender_t::initialize_child(value_type, capacity);
            if (value_child.has_error()) {
                return value_child.error();
            }
            internal_struct->child_data.push_back(std::move(value_child.value()));

            result.child_data.push_back(std::move(internal_struct));
            return core::error_t::no_error();
        }

        [[nodiscard]] static core::error_t
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            uint64_t size = to - from;
            if (auto error = append_data.add_validity(format, from, to); error.contains_error()) {
                return error;
            }
            std::vector<uint64_t> child_indices;
            if (auto error = arrow_list_data_t<BUFTYPE>::append_offsets(append_data, format, from, to, child_indices);
                error.contains_error()) {
                return error;
            }

            indexing_vector_t child_indexing(input.resource(), child_indices.data());
            // A MAP vector is physically a LIST whose single child is a struct<key, value>; the
            // key and value vectors are that struct's entries, not entries() of the map vector
            // itself (which is a list buffer and has none).
            auto& entries_vector = input.entry();
            auto& key_vector = entries_vector.entries().at(0);
            auto& value_vector = entries_vector.entries().at(1);
            auto list_size = child_indices.size();

            auto& struct_data = *append_data.child_data[0];
            auto& key_data = *struct_data.child_data[0];
            auto& value_data = *struct_data.child_data[1];

            vector_t key_vector_copy(key_vector->resource(), key_vector->type());
            key_vector_copy.slice(*key_vector, child_indexing, list_size);
            vector_t value_vector_copy(value_vector->resource(), value_vector->type());
            value_vector_copy.slice(*value_vector, child_indexing, list_size);
            if (auto error = key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
                error.contains_error()) {
                return error;
            }
            if (auto error = value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);
                error.contains_error()) {
                return error;
            }

            append_data.row_count += size;
            struct_data.row_count += size;
            return core::error_t::no_error();
        }

        [[nodiscard]] static core::error_t
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            assert(result);
            // Checked BEFORE any child is finalized: finalize_child hands a child's ownership
            // to its ArrowArray (freed only through the release callback), so refusing after
            // that point strands every child already attached. The key child's null count is
            // known right here.
            if (append_data.child_data[0]->child_data[0]->null_count > 0) {
                return core::error_t(core::error_code_t::conversion_failure,
                                     std::pmr::string("arrow appender: Arrow does not accept NULL keys on a MAP",
                                                      std::pmr::get_default_resource()));
            }
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();

            arrow_appender_t::add_children(append_data, 1);
            result->children = append_data.child_pointers.data();
            result->n_children = 1;

            auto& struct_data = *append_data.child_data[0];
            auto struct_result = arrow_appender_t::finalize_child(type, std::move(append_data.child_data[0]));
            if (struct_result.has_error()) {
                return struct_result.error();
            }

            const auto struct_child_count = 2;
            arrow_appender_t::add_children(struct_data, struct_child_count);
            struct_result.value()->children = struct_data.child_pointers.data();
            struct_result.value()->n_buffers = 1;
            struct_result.value()->n_children = struct_child_count;
            struct_result.value()->length = static_cast<int64_t>(struct_data.child_data[0]->row_count);

            append_data.child_arrays[0] = *struct_result.value();

            assert(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);

            auto map_extension = static_cast<types::map_logical_type_extension*>(type.extension());
            auto& key_type = map_extension->key();
            auto& value_type = map_extension->value();
            auto key_data = arrow_appender_t::finalize_child(key_type, std::move(struct_data.child_data[0]));
            if (key_data.has_error()) {
                return key_data.error();
            }
            struct_data.child_arrays[0] = *key_data.value();
            auto value_data = arrow_appender_t::finalize_child(value_type, std::move(struct_data.child_data[1]));
            if (value_data.has_error()) {
                return value_data.error();
            }
            struct_data.child_arrays[1] = *value_data.value();
            return core::error_t::no_error();
        }
    };

} // namespace components::vector::arrow::appender
