#include <components/casts/composite_cast.hpp>

#include <components/casts/cast_registry.hpp>

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace components::casts {

    namespace {

        void propagate_row_validity(const vector::vector_t& source, vector::vector_t* result, uint64_t count) {
            for (uint64_t row = 0; row < count; ++row) {
                result->set_null(row, source.is_null(row));
            }
        }

        void reserve_list_child(vector::vector_t* result, uint64_t child_count) {
            static_cast<vector::list_vector_buffer_t*>(result->auxiliary().get())->reserve(child_count);
        }

        [[nodiscard]] cast_t copy_leaf_closure() {
            return cast_t{[](cast_kind,
                             const vector::vector_t& source,
                             vector::vector_t* result,
                             const graph_execution_context&,
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
                                                       const graph_execution_context& context,
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

        void fill_array_row(const vector::vector_t& staged,
                            vector::vector_t* target_child,
                            const graph_execution_context& context,
                            uint64_t row,
                            types::list_entry_t source_span,
                            uint64_t target_stride) {
            const uint64_t copy_length = std::min<uint64_t>(source_span.length, target_stride);
            for (uint64_t index = 0; index < copy_length; ++index) {
                target_child->set_value(row * target_stride + index, staged.value(source_span.offset + index));
            }
            for (uint64_t index = copy_length; index < target_stride; ++index) {
                if (context.fill_value != nullptr) {
                    target_child->set_value(row * target_stride + index, *context.fill_value);
                } else {
                    target_child->set_null(row * target_stride + index, true);
                }
            }
        }

        [[nodiscard]] cast_t array_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const graph_execution_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t source_stride = source.type().extension_as<types::array_logical_type_extension>()->size();
                uint64_t target_stride = result->type().extension_as<types::array_logical_type_extension>()->size();
                if (source_stride == target_stride) {
                    core::error_t error =
                        element(kind, source.entry(), &result->entry(), context, count * source_stride);
                    if (error.contains_error()) {
                        return error;
                    }
                    propagate_row_validity(source, result, count);
                    return core::error_t::no_error();
                }

                uint64_t staged_count = count * source_stride;
                vector::vector_t staged{result->resource(),
                                        result->type().child_type(),
                                        staged_count == 0 ? uint64_t{1} : staged_count};
                core::error_t error = element(kind, source.entry(), &staged, context, staged_count);
                if (error.contains_error()) {
                    return error;
                }
                vector::vector_t& target_child = result->entry();
                for (uint64_t row = 0; row < count; ++row) {
                    fill_array_row(staged,
                                   &target_child,
                                   context,
                                   row,
                                   types::list_entry_t{row * source_stride, source_stride},
                                   target_stride);
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] cast_t list_cast(cast_t element) {
            return cast_t{[element = std::move(element)](cast_kind kind,
                                                         const vector::vector_t& source,
                                                         vector::vector_t* result,
                                                         const graph_execution_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t child_count =
                    static_cast<const vector::list_vector_buffer_t*>(source.auxiliary().get())->size();
                std::memcpy(result->data<types::list_entry_t>(),
                            source.data<types::list_entry_t>(),
                            count * sizeof(types::list_entry_t));
                reserve_list_child(result, child_count);
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
                                                         const graph_execution_context& context,
                                                         uint64_t count) -> core::error_t {
                uint64_t stride = source.type().extension_as<types::array_logical_type_extension>()->size();
                uint64_t child_count = count * stride;
                auto* spans = result->data<types::list_entry_t>();
                for (uint64_t row = 0; row < count; ++row) {
                    spans[row] = types::list_entry_t{row * stride, stride};
                }
                reserve_list_child(result, child_count);
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
                                                         const graph_execution_context& context,
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
                    fill_array_row(staged, &target_child, context, row, spans[row], stride);
                }
                propagate_row_validity(source, result, count);
                return core::error_t::no_error();
            }};
        }

        [[nodiscard]] bool is_list_or_array(const types::complex_logical_type& type) {
            return type.type() == types::logical_type::LIST || type.type() == types::logical_type::ARRAY;
        }

        [[nodiscard]] types::list_entry_t element_span(const vector::vector_t& source, uint64_t row) {
            if (source.type().type() == types::logical_type::LIST) {
                return source.data<types::list_entry_t>()[row];
            }
            const uint64_t stride = source.type().extension_as<types::array_logical_type_extension>()->size();
            return types::list_entry_t{row * stride, stride};
        }

        [[nodiscard]] uint64_t child_element_count(const vector::vector_t& source, uint64_t count) {
            if (source.type().type() == types::logical_type::LIST) {
                return static_cast<const vector::list_vector_buffer_t*>(source.auxiliary().get())->size();
            }
            return count * source.type().extension_as<types::array_logical_type_extension>()->size();
        }

        [[nodiscard]] bool is_ascii_space(char character) noexcept {
            return character == ' ' || character == '\t' || character == '\n' || character == '\r' ||
                   character == '\v' || character == '\f';
        }

        [[nodiscard]] bool spells_null(std::string_view text) noexcept {
            static constexpr std::string_view null_word{"null"};
            if (text.size() != null_word.size()) {
                return false;
            }
            for (size_t index = 0; index < text.size(); ++index) {
                char character = text[index];
                if (character >= 'A' && character <= 'Z') {
                    character = static_cast<char>(character - 'A' + 'a');
                }
                if (character != null_word[index]) {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool needs_quoting(std::string_view text) noexcept {
            if (text.empty() || spells_null(text)) {
                return true;
            }
            for (char character : text) {
                if (character == '{' || character == '}' || character == ',' || character == '"' || character == '\\' ||
                    is_ascii_space(character)) {
                    return true;
                }
            }
            return false;
        }

        void append_quoted(std::pmr::string* text, std::string_view element) {
            text->push_back('"');
            for (char character : element) {
                if (character == '"' || character == '\\') {
                    text->push_back('\\');
                }
                text->push_back(character);
            }
            text->push_back('"');
        }

        [[nodiscard]] cast_t list_or_array_to_string_cast(cast_t element, bool element_is_list_or_array) {
            return cast_t{
                [element = std::move(element), element_is_list_or_array](cast_kind kind,
                                                                         const vector::vector_t& source,
                                                                         vector::vector_t* result,
                                                                         const graph_execution_context& context,
                                                                         uint64_t count) -> core::error_t {
                    const uint64_t child_count = child_element_count(source, count);
                    vector::vector_t rendered{result->resource(),
                                              types::complex_logical_type{types::logical_type::STRING_LITERAL},
                                              child_count == 0 ? uint64_t{1} : child_count};
                    core::error_t error = element(kind, source.entry(), &rendered, context, child_count);
                    if (error.contains_error()) {
                        return error;
                    }
                    std::pmr::string text{result->resource()};
                    for (uint64_t row = 0; row < count; ++row) {
                        if (source.is_null(row)) {
                            result->set_null(row, true);
                            continue;
                        }
                        const types::list_entry_t span = element_span(source, row);
                        text.clear();
                        text.push_back('{');
                        for (uint64_t offset = 0; offset < span.length; ++offset) {
                            if (offset != 0) {
                                text.push_back(',');
                            }
                            const uint64_t index = span.offset + offset;
                            if (rendered.is_null(index)) {
                                text.append("NULL");
                                continue;
                            }
                            const std::string_view value = rendered.get_value<std::string_view>(index);
                            if (!element_is_list_or_array && needs_quoting(value)) {
                                append_quoted(&text, value);
                            } else {
                                text.append(value);
                            }
                        }
                        text.push_back('}');
                        result->set_value(row, std::string_view{text});
                    }
                    return core::error_t::no_error();
                }};
        }

    } // namespace

    cast_t leaf_closure(cast_function_t fn) {
        return cast_t{[fn](cast_kind kind,
                           const vector::vector_t& source,
                           vector::vector_t* result,
                           const graph_execution_context& context,
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
        if ((source_is_list || source_is_array) && target.type() == types::logical_type::STRING_LITERAL) {
            std::optional<cast_t> element = registry.resolve(source.child_type(), target, allowed);
            if (!element.has_value()) {
                return std::nullopt;
            }
            return list_or_array_to_string_cast(std::move(*element), is_list_or_array(source.child_type()));
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
