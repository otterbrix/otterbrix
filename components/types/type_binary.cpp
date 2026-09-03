#include <components/types/type_binary.hpp>

#include <components/types/logical_value.hpp>

#include <core/result_wrapper.hpp>

#include <cassert>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace components::types {
    namespace {
        inline void write_le16(char* destination, uint16_t value) { std::memcpy(destination, &value, 2); }
        inline void write_le32(char* destination, uint32_t value) { std::memcpy(destination, &value, 4); }
        inline void write_le64(char* destination, uint64_t value) { std::memcpy(destination, &value, 8); }

        inline uint16_t read_le16(const char* source) {
            uint16_t value;
            std::memcpy(&value, source, 2);
            return value;
        }
        inline uint32_t read_le32(const char* source) {
            uint32_t value;
            std::memcpy(&value, source, 4);
            return value;
        }
        inline uint64_t read_le64(const char* source) {
            uint64_t value;
            std::memcpy(&value, source, 8);
            return value;
        }

        // -------------------------------------------------------------------
        // Type header codec
        // -------------------------------------------------------------------
        // Recursive layout:
        //   type := [logical_type:1][alias:str][extension]
        //   str  := [length:2 LE][bytes]
        //   extension:
        //     0 NONE
        //     1 ARRAY_FLAT [element logical_type:1][size:4]   (legacy)
        //     2 DECIMAL    [width:1][scale:1]                 (legacy)
        //     3 LIST       [field_id:8][required:1][element:type]
        //     4 STRUCT     [name:str][child_count:2][child:type * N]
        //     5 ENUM       [name:str][entry_count:4][[entry_type:1][value:8][name:str] * N]
        //     6 UNKNOWN    [name:str]
        //     7 MAP        [key_id:8][value_id:8][value_required:1][key:type][value:type]
        //     8 ARRAY      [size:4][element:type]
        enum class wire_extension : uint8_t
        {
            NONE = 0,
            ARRAY_FLAT = 1,
            DECIMAL = 2,
            LIST = 3,
            STRUCT = 4,
            ENUM = 5,
            UNKNOWN = 6,
            MAP = 7,
            ARRAY = 8
        };

        // An ARRAY element needs no nested header when it carries nothing beyond
        // its own tag — exactly what the legacy flat encoding can express.
        bool fits_flat_array_element(const complex_logical_type& type) {
            return type.extension() == nullptr && !type.has_alias();
        }

        uint32_t string_field_size(std::string_view value) { return 2 + static_cast<uint32_t>(value.size()); }

        // ENUM entries are integer literals carrying the entry name as their alias
        // (enum_logical_type_extension::entries_).
        bool enum_entry_value(const logical_value_t& entry, int64_t& value) {
            switch (entry.type().type()) {
                case logical_type::TINYINT:
                    value = entry.value<int8_t>();
                    return true;
                case logical_type::SMALLINT:
                    value = entry.value<int16_t>();
                    return true;
                case logical_type::INTEGER:
                    value = entry.value<int32_t>();
                    return true;
                case logical_type::BIGINT:
                    value = entry.value<int64_t>();
                    return true;
                default:
                    return false;
            }
        }

        bool append_enum_entry(std::pmr::memory_resource* resource,
                               logical_type entry_type,
                               int64_t value,
                               std::string_view entry_name,
                               std::vector<logical_value_t>& entries) {
            switch (entry_type) {
                case logical_type::TINYINT:
                    entries.emplace_back(resource, static_cast<int8_t>(value));
                    break;
                case logical_type::SMALLINT:
                    entries.emplace_back(resource, static_cast<int16_t>(value));
                    break;
                case logical_type::INTEGER:
                    entries.emplace_back(resource, static_cast<int32_t>(value));
                    break;
                case logical_type::BIGINT:
                    entries.emplace_back(resource, static_cast<int64_t>(value));
                    break;
                default:
                    return false;
            }
            entries.back().set_alias(std::string{entry_name});
            return true;
        }

        // Compute the size of the type header for a single column.
        uint32_t detail_type_binary_size(const complex_logical_type& column_type) {
            uint32_t header_size = 1 + 2; // logical_type + alias_length
            if (column_type.has_alias()) {
                header_size += static_cast<uint32_t>(column_type.alias().size());
            }
            header_size += 1; // extension_type byte

            const auto* extension = column_type.extension();
            if (!extension) {
                return header_size;
            }

            using extension_type = logical_type_extension::extension_type;
            switch (extension->type()) {
                case extension_type::ARRAY: {
                    const auto* array = static_cast<const array_logical_type_extension*>(extension);
                    if (fits_flat_array_element(array->internal_type())) {
                        return header_size + 1 + 4;
                    }
                    return header_size + 4 + detail_type_binary_size(array->internal_type());
                }
                case extension_type::DECIMAL:
                    return header_size + 2;
                case extension_type::LIST: {
                    const auto* list = static_cast<const list_logical_type_extension*>(extension);
                    return header_size + 8 + 1 + detail_type_binary_size(list->node());
                }
                case extension_type::STRUCT: {
                    const auto* record = static_cast<const struct_logical_type_extension*>(extension);
                    header_size += string_field_size(record->type_name()) + 2;
                    for (const auto& child : record->child_types()) {
                        header_size += detail_type_binary_size(child);
                    }
                    return header_size;
                }
                case extension_type::ENUM: {
                    const auto* enumeration = static_cast<const enum_logical_type_extension*>(extension);
                    header_size += string_field_size(enumeration->type_name()) + 4;
                    for (const auto& entry : enumeration->entries()) {
                        header_size += 1 + 8 + string_field_size(entry.type().alias());
                    }
                    return header_size;
                }
                case extension_type::UNKNOWN: {
                    const auto* unknown = static_cast<const unknown_logical_type_extension*>(extension);
                    return header_size + string_field_size(unknown->type_name());
                }
                case extension_type::MAP: {
                    const auto* map = static_cast<const map_logical_type_extension*>(extension);
                    return header_size + 8 + 8 + 1 + detail_type_binary_size(map->key()) +
                           detail_type_binary_size(map->value());
                }
                default:
                    // GENERIC / USER / FUNCTION are not column types
                    return header_size;
            }
        }

        char* write_string_field(char* output, std::string_view value) {
            auto length = static_cast<uint16_t>(value.size());
            write_le16(output, length);
            output += 2;
            std::memcpy(output, value.data(), length);
            return output + length;
        }

        // Write the type header for a single column. Returns pointer past written data.
        char* detail_type_binary_write(char* output, const complex_logical_type& column_type) {
            *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(column_type.type());
            output += 1;

            if (column_type.has_alias()) {
                output = write_string_field(output, column_type.alias());
            } else {
                write_le16(output, 0);
                output += 2;
            }

            const auto* extension = column_type.extension();
            if (!extension) {
                *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::NONE);
                return output + 1;
            }

            using extension_type = logical_type_extension::extension_type;
            switch (extension->type()) {
                case extension_type::ARRAY: {
                    const auto* array = static_cast<const array_logical_type_extension*>(extension);
                    if (fits_flat_array_element(array->internal_type())) {
                        *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::ARRAY_FLAT);
                        output += 1;
                        *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(array->internal_type().type());
                        output += 1;
                        write_le32(output, static_cast<uint32_t>(array->size()));
                        return output + 4;
                    }
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::ARRAY);
                    output += 1;
                    write_le32(output, static_cast<uint32_t>(array->size()));
                    output += 4;
                    return detail_type_binary_write(output, array->internal_type());
                }
                case extension_type::DECIMAL: {
                    const auto* decimal = static_cast<const decimal_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::DECIMAL);
                    output += 1;
                    *reinterpret_cast<uint8_t*>(output) = decimal->width();
                    output += 1;
                    *reinterpret_cast<uint8_t*>(output) = decimal->scale();
                    return output + 1;
                }
                case extension_type::LIST: {
                    const auto* list = static_cast<const list_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::LIST);
                    output += 1;
                    write_le64(output, list->field_id());
                    output += 8;
                    *reinterpret_cast<uint8_t*>(output) = list->required() ? 1 : 0;
                    output += 1;
                    return detail_type_binary_write(output, list->node());
                }
                case extension_type::STRUCT: {
                    const auto* record = static_cast<const struct_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::STRUCT);
                    output += 1;
                    output = write_string_field(output, record->type_name());
                    const auto& children = record->child_types();
                    write_le16(output, static_cast<uint16_t>(children.size()));
                    output += 2;
                    for (const auto& child : children) {
                        output = detail_type_binary_write(output, child);
                    }
                    return output;
                }
                case extension_type::ENUM: {
                    const auto* enumeration = static_cast<const enum_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::ENUM);
                    output += 1;
                    output = write_string_field(output, enumeration->type_name());
                    const auto& entries = enumeration->entries();
                    write_le32(output, static_cast<uint32_t>(entries.size()));
                    output += 4;
                    for (const auto& entry : entries) {
                        int64_t value = 0;
                        if (!enum_entry_value(entry, value)) {
                            assert(false && "ENUM entry is not an integer literal");
                        }
                        *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(entry.type().type());
                        output += 1;
                        write_le64(output, static_cast<uint64_t>(value));
                        output += 8;
                        output = write_string_field(output, entry.type().alias());
                    }
                    return output;
                }
                case extension_type::UNKNOWN: {
                    const auto* unknown = static_cast<const unknown_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::UNKNOWN);
                    output += 1;
                    return write_string_field(output, unknown->type_name());
                }
                case extension_type::MAP: {
                    const auto* map = static_cast<const map_logical_type_extension*>(extension);
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::MAP);
                    output += 1;
                    write_le64(output, map->key_id());
                    output += 8;
                    write_le64(output, map->value_id());
                    output += 8;
                    *reinterpret_cast<uint8_t*>(output) = map->value_required() ? 1 : 0;
                    output += 1;
                    output = detail_type_binary_write(output, map->key());
                    return detail_type_binary_write(output, map->value());
                }
                default:
                    // GENERIC / USER / FUNCTION are not column types, see BUGS.md B-054.
                    *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(wire_extension::NONE);
                    return output + 1;
            }
        }

        core::error_t truncated(std::pmr::memory_resource* resource) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"type record: buffer ends inside the record", resource});
        }

        core::error_t malformed(std::pmr::memory_resource* resource, const char* what) {
            return core::error_t(core::error_code_t::data_corruption, std::pmr::string{what, resource});
        }

        bool read_string_field(const char*& scan, const char* end, std::string& value) {
            if (scan + 2 > end) {
                return false;
            }
            uint16_t length = read_le16(scan);
            scan += 2;
            if (scan + length > end) {
                return false;
            }
            value.assign(scan, length);
            scan += length;
            return true;
        }

        // Read the type record for a single column, advancing `scan`. A truncated buffer or a
        // body that does not match its tag is data_corruption, named rather than signalled by a
        // bare flag.
        core::result_wrapper_t<complex_logical_type>
        detail_type_binary_read(const char*& scan, const char* end, std::pmr::memory_resource* resource) {
            if (scan + 1 > end) {
                return truncated(resource);
            }
            auto logical_type_value = static_cast<logical_type>(*reinterpret_cast<const uint8_t*>(scan));
            scan += 1;

            std::string alias;
            if (!read_string_field(scan, end, alias)) {
                return truncated(resource);
            }

            if (scan >= end) {
                return truncated(resource);
            }
            uint8_t extension_tag = *reinterpret_cast<const uint8_t*>(scan);
            scan += 1;

            switch (static_cast<wire_extension>(extension_tag)) {
                case wire_extension::ARRAY_FLAT: {
                    if (scan + 5 > end) {
                        return truncated(resource);
                    }
                    auto element_type = static_cast<logical_type>(*reinterpret_cast<const uint8_t*>(scan));
                    scan += 1;
                    uint32_t array_size = read_le32(scan);
                    scan += 4;
                    return complex_logical_type::create_array(element_type,
                                                              static_cast<size_t>(array_size),
                                                              std::move(alias));
                }
                case wire_extension::DECIMAL: {
                    if (scan + 2 > end) {
                        return truncated(resource);
                    }
                    uint8_t width = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    uint8_t scale = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    return complex_logical_type::create_decimal(width, scale, std::move(alias));
                }
                case wire_extension::ARRAY: {
                    if (scan + 4 > end) {
                        return truncated(resource);
                    }
                    uint32_t array_size = read_le32(scan);
                    scan += 4;
                    auto element = detail_type_binary_read(scan, end, resource);
                    if (element.has_error()) {
                        return element;
                    }
                    return complex_logical_type::create_array(element.value(),
                                                              static_cast<size_t>(array_size),
                                                              std::move(alias));
                }
                case wire_extension::LIST: {
                    if (scan + 9 > end) {
                        return truncated(resource);
                    }
                    uint64_t field_id = read_le64(scan);
                    scan += 8;
                    bool required = *reinterpret_cast<const uint8_t*>(scan) != 0;
                    scan += 1;
                    auto element = detail_type_binary_read(scan, end, resource);
                    if (element.has_error()) {
                        return element;
                    }
                    return complex_logical_type(
                        logical_type_value,
                        std::make_unique<list_logical_type_extension>(field_id, std::move(element.value()), required),
                        std::move(alias));
                }
                case wire_extension::STRUCT: {
                    std::string type_name;
                    if (!read_string_field(scan, end, type_name)) {
                        return truncated(resource);
                    }
                    if (scan + 2 > end) {
                        return truncated(resource);
                    }
                    uint16_t child_count = read_le16(scan);
                    scan += 2;
                    std::pmr::vector<complex_logical_type> children(resource);
                    children.reserve(child_count);
                    for (uint16_t child_index = 0; child_index < child_count; ++child_index) {
                        auto child = detail_type_binary_read(scan, end, resource);
                        if (child.has_error()) {
                            return child;
                        }
                        children.push_back(std::move(child.value()));
                    }
                    return complex_logical_type(
                        logical_type_value,
                        std::make_unique<struct_logical_type_extension>(std::move(type_name), children),
                        std::move(alias));
                }
                case wire_extension::ENUM: {
                    std::string type_name;
                    if (!read_string_field(scan, end, type_name)) {
                        return truncated(resource);
                    }
                    if (scan + 4 > end) {
                        return truncated(resource);
                    }
                    uint32_t entry_count = read_le32(scan);
                    scan += 4;
                    std::vector<logical_value_t> entries;
                    entries.reserve(entry_count);
                    for (uint32_t entry_index = 0; entry_index < entry_count; ++entry_index) {
                        if (scan + 9 > end) {
                            return truncated(resource);
                        }
                        auto entry_type = static_cast<logical_type>(*reinterpret_cast<const uint8_t*>(scan));
                        scan += 1;
                        auto value = static_cast<int64_t>(read_le64(scan));
                        scan += 8;
                        std::string entry_name;
                        if (!read_string_field(scan, end, entry_name)) {
                            return truncated(resource);
                        }
                        if (!append_enum_entry(resource, entry_type, value, entry_name, entries)) {
                            return malformed(resource, "type record: ENUM entry is not an integer literal");
                        }
                    }
                    return complex_logical_type(
                        logical_type_value,
                        std::make_unique<enum_logical_type_extension>(std::move(type_name), std::move(entries)),
                        std::move(alias));
                }
                case wire_extension::UNKNOWN: {
                    std::string type_name;
                    if (!read_string_field(scan, end, type_name)) {
                        return truncated(resource);
                    }
                    return complex_logical_type::create_unknown(std::move(type_name), std::move(alias));
                }
                case wire_extension::MAP: {
                    if (scan + 17 > end) {
                        return truncated(resource);
                    }
                    uint64_t key_id = read_le64(scan);
                    scan += 8;
                    uint64_t value_id = read_le64(scan);
                    scan += 8;
                    bool value_required = *reinterpret_cast<const uint8_t*>(scan) != 0;
                    scan += 1;
                    auto key = detail_type_binary_read(scan, end, resource);
                    if (key.has_error()) {
                        return key;
                    }
                    auto value = detail_type_binary_read(scan, end, resource);
                    if (value.has_error()) {
                        return value;
                    }
                    return complex_logical_type(logical_type_value,
                                                std::make_unique<map_logical_type_extension>(resource,
                                                                                             key_id,
                                                                                             key.value(),
                                                                                             value_id,
                                                                                             value.value(),
                                                                                             value_required),
                                                std::move(alias));
                }
                default: // no extension
                    return complex_logical_type(logical_type_value, std::move(alias));
            }
        }
    } // namespace

    uint32_t type_binary_size(const complex_logical_type& type) { return detail_type_binary_size(type); }

    char* type_binary_write(char* output, const complex_logical_type& type) {
        return detail_type_binary_write(output, type);
    }

    core::result_wrapper_t<complex_logical_type>
    type_binary_read(const char*& scan, const char* end, std::pmr::memory_resource* resource) {
        return detail_type_binary_read(scan, end, resource);
    }
} // namespace components::types
