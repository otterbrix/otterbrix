#include <components/casts/composite_cast.hpp>

#include <components/casts/cast_registry.hpp>

#include <cstring>
#include <vector>

namespace components::casts {

    namespace {

        void propagate_row_validity(const vector::vector_t& source, vector::vector_t* result, uint64_t count) {
            for (uint64_t row = 0; row < count; ++row) {
                result->set_null(row, source.is_null(row));
            }
        }

        [[nodiscard]] cast_t copy_leaf_closure() {
            return cast_t{[](cast_kind,
                             const vector::vector_t& source,
                             vector::vector_t* result,
                             const cast_context&,
                             uint64_t count) -> core::error_t {
                for (uint64_t row = 0; row < count; ++row) {
                    if (source.is_null(row)) {
                        result->set_null(row, true);
                    } else {
                        result->set_value(row, source.value(row));
                    }
                }
                return core::error_t::no_error();
            }};
        }

        // One field of a struct cast
        struct field_cast {
            size_t source_index;
            size_t target_index;
            cast_t cast;
        };

        [[nodiscard]] std::optional<cast_t> make_struct_cast(std::vector<field_cast> fields) {
            for (const field_cast& field : fields) {
                if (!field.cast) {
                    return std::nullopt;
                }
            }
            return cast_t{[fields = std::move(fields)](cast_kind kind,
                                                       const vector::vector_t& source,
                                                       vector::vector_t* result,
                                                       const cast_context& context,
                                                       uint64_t count) -> core::error_t {
                const auto& source_entries = source.entries();
                auto& result_entries = result->entries();
                for (const field_cast& field : fields) {
                    core::error_t error = field.cast(kind,
                                                     *source_entries[field.source_index],
                                                     result_entries[field.target_index].get(),
                                                     context,
                                                     count);
                    if (error.contains_error()) {
                        return error;
                    }
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] cast_t array_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const cast_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t stride = source.type().extension_as<types::array_logical_type_extension>()->size();
                core::error_t error = element(kind, source.entry(), &result->entry(), context, count * stride);
                if (error.contains_error()) {
                    return error;
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] cast_t list_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const cast_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t child_count =
                    static_cast<const vector::list_vector_buffer_t*>(source.auxiliary().get())->size();
                std::memcpy(result->data<types::list_entry_t>(),
                            source.data<types::list_entry_t>(),
                            count * sizeof(types::list_entry_t));
                result->set_list_size(child_count);
                core::error_t error = element(kind, source.entry(), &result->entry(), context, child_count);
                if (error.contains_error()) {
                    return error;
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] cast_t array_to_list_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const cast_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t stride = source.type().extension_as<types::array_logical_type_extension>()->size();
                uint64_t child_count = count * stride;
                auto* spans = result->data<types::list_entry_t>();
                for (uint64_t row = 0; row < count; ++row) {
                    spans[row] = types::list_entry_t{row * stride, stride};
                }
                result->set_list_size(child_count);
                core::error_t error = element(kind, source.entry(), &result->entry(), context, child_count);
                if (error.contains_error()) {
                    return error;
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] cast_t list_to_array_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const cast_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t stride = result->type().extension_as<types::array_logical_type_extension>()->size();
                const auto* spans = source.data<types::list_entry_t>();
                bool all_fit = true;
                bool contiguous = true;
                for (uint64_t row = 0; row < count; ++row) {
                    if (spans[row].length != stride) {
                        all_fit = false;
                    }
                    if (spans[row].offset != row * stride) {
                        contiguous = false;
                    }
                }
                // A row that does not fit is a hard failure for CAST
                if (!all_fit && kind == cast_kind::cast) {
                    return core::error_t{core::error_code_t::conversion_failure,
                                         std::pmr::string{"length mismatch", result->resource()}};
                }
                if (all_fit && contiguous) {
                    core::error_t error = element(kind, source.entry(), &result->entry(), context, count * stride);
                    if (error.contains_error()) {
                        return error;
                    }
                    propagate_row_validity(source, result, count);
                    return core::error_t::no_error();
                }
                uint64_t child_count =
                    static_cast<const vector::list_vector_buffer_t*>(source.auxiliary().get())->size();
                vector::vector_t staged{result->resource(),
                                        result->type().child_type(),
                                        child_count == 0 ? uint64_t{1} : child_count};
                core::error_t error = element(kind, source.entry(), &staged, context, child_count);
                if (error.contains_error()) {
                    // only reachable under cast; a try_cast element never errors
                    return error;
                }
                vector::vector_t& target_child = result->entry();
                for (uint64_t row = 0; row < count; ++row) {
                    types::list_entry_t span = spans[row];
                    uint64_t copy_length = span.length < stride ? span.length : stride;
                    for (uint64_t index = 0; index < copy_length; ++index) {
                        target_child.set_value(row * stride + index, staged.value(span.offset + index));
                    }
                    for (uint64_t index = copy_length; index < stride; ++index) {
                        target_child.set_null(row * stride + index, true); // null-pad the shortfall
                    }
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

    } // namespace

    cast_t leaf_closure(cast_function_t fn) {
        return cast_t{[fn](cast_kind kind,
                           const vector::vector_t& source,
                           vector::vector_t* result,
                           const cast_context& context,
                           uint64_t count) { return fn.invoke(kind, source, result, context, count); }};
    }

    std::optional<cast_t> build_cast(const cast_registry_t& registry,
                                     const types::complex_logical_type& source,
                                     const types::complex_logical_type& target,
                                     cast_type allowed) {
        bool source_is_list = source.type() == types::logical_type::LIST;
        bool source_is_array = source.type() == types::logical_type::ARRAY;
        bool target_is_list = target.type() == types::logical_type::LIST;
        bool target_is_array = target.type() == types::logical_type::ARRAY;
        if ((source_is_list || source_is_array) && (target_is_list || target_is_array)) {
            std::optional<cast_t> element = registry.resolve(source.child_type(), target.child_type(), allowed);
            if (!element.has_value()) {
                return std::nullopt;
            }
            if (source_is_array && target_is_array) {
                return array_cast(std::move(*element));
            }
            if (source_is_list && target_is_list) {
                return list_cast(std::move(*element));
            }
            if (source_is_array && target_is_list) {
                return array_to_list_cast(std::move(*element));
            }
            return list_to_array_cast(std::move(*element));
        }
        if (source.type() == types::logical_type::MAP && target.type() == types::logical_type::MAP) {
            // Built over the key and the value directly, mirroring how the registry derives a
            // map's coercion level. Going through the map's child struct<key,value> instead
            // would hit the STRUCT rule, which is about USER structs and does not apply here.
            const auto* source_map = source.extension_as<types::map_logical_type_extension>();
            const auto* target_map = target.extension_as<types::map_logical_type_extension>();
            std::optional<cast_t> key = registry.resolve(source_map->key(), target_map->key(), allowed);
            if (!key.has_value()) {
                return std::nullopt;
            }
            std::optional<cast_t> value = registry.resolve(source_map->value(), target_map->value(), allowed);
            if (!value.has_value()) {
                return std::nullopt;
            }
            std::vector<field_cast> entries;
            entries.reserve(2);
            entries.push_back(field_cast{0, 0, std::move(*key)});
            entries.push_back(field_cast{1, 1, std::move(*value)});
            std::optional<cast_t> element = make_struct_cast(std::move(entries));
            if (!element.has_value()) {
                return std::nullopt;
            }
            return list_cast(std::move(*element));
        }
        if (source.type() == types::logical_type::STRUCT && target.type() == types::logical_type::STRUCT) {
            const auto& source_fields = source.child_types();
            const auto& target_fields = target.child_types();
            if (source_fields.size() != target_fields.size()) {
                return std::nullopt;
            }
            std::vector<field_cast> fields;
            fields.reserve(source_fields.size());
            for (size_t index = 0; index < source_fields.size(); ++index) {
                std::optional<cast_t> field = registry.resolve(source_fields[index], target_fields[index], allowed);
                if (!field.has_value()) {
                    return std::nullopt;
                }
                fields.push_back(field_cast{index, index, std::move(*field)});
            }
            return make_struct_cast(std::move(fields));
        }
        const cast_entry* entry = registry.find(source, target);
        if (entry != nullptr) {
            return leaf_closure(entry->fn);
        }
        if (same_cast_type(source, target)) {
            return copy_leaf_closure();
        }
        // No registered conversion.
        return std::nullopt;
    }

} // namespace components::casts
