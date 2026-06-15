#include "data_chunk_binary.hpp"

#include <cassert>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <core/date/date_types.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_buffer.hpp>

namespace components::vector {

    // -----------------------------------------------------------------------
    // Little-endian helpers
    // -----------------------------------------------------------------------
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

        constexpr uint16_t kExtendedFormatMagic = 0xFFFF;
        constexpr uint16_t kExtendedFormatVersion = 2;

        data_chunk_t make_empty_error_chunk(std::pmr::memory_resource* resource);

        // Return the byte-size of one element for a fixed-width physical type.
        // Returns 0 for STRING (variable-width) and for composite types (ARRAY, etc.).
        size_t fixed_type_size(types::physical_type physical_type) {
            switch (physical_type) {
                case types::physical_type::BOOL:
                    return sizeof(bool);
                case types::physical_type::INT8:
                case types::physical_type::UINT8:
                    return 1;
                case types::physical_type::INT16:
                case types::physical_type::UINT16:
                    return 2;
                case types::physical_type::INT32:
                case types::physical_type::UINT32:
                case types::physical_type::FLOAT:
                    return 4;
                case types::physical_type::INT64:
                case types::physical_type::UINT64:
                case types::physical_type::DOUBLE:
                    return 8;
                case types::physical_type::INT128:
                case types::physical_type::UINT128:
                    return 16;
                default:
                    return 0;
            }
        }

        bool is_variable_type(types::physical_type physical_type) {
            return physical_type == types::physical_type::STRING;
        }

        bool uses_legacy_binary_format(const data_chunk_t& chunk) {
            if (chunk.column_count() > std::numeric_limits<uint16_t>::max()) {
                return false;
            }

            for (const auto& column : chunk.data) {
                const auto& type = column.type();
                auto physical_type = type.to_physical_type();

                if (type.type() == types::logical_type::ENUM) {
                    return false;
                }
                if (is_variable_type(physical_type)) {
                    if (type.type() != types::logical_type::STRING_LITERAL) {
                        return false;
                    }
                    continue;
                }
                if (fixed_type_size(physical_type) == 0) {
                    return false;
                }
            }
            return true;
        }

        void append_raw(services::wal::buffer_t& buffer, const void* data, size_t size) {
            buffer.append(reinterpret_cast<const char*>(data), size);
        }

        void append_u8(services::wal::buffer_t& buffer, uint8_t value) {
            buffer.push_back(static_cast<char>(value));
        }

        void append_le16(services::wal::buffer_t& buffer, uint16_t value) {
            char data[2];
            write_le16(data, value);
            append_raw(buffer, data, sizeof(data));
        }

        void append_le32(services::wal::buffer_t& buffer, uint32_t value) {
            char data[4];
            write_le32(data, value);
            append_raw(buffer, data, sizeof(data));
        }

        void append_le64(services::wal::buffer_t& buffer, uint64_t value) {
            char data[8];
            write_le64(data, value);
            append_raw(buffer, data, sizeof(data));
        }

        template<typename T>
        void append_trivial(services::wal::buffer_t& buffer, const T& value) {
            append_raw(buffer, &value, sizeof(T));
        }

        void patch_le32(services::wal::buffer_t& buffer, size_t offset, uint32_t value) {
            assert(offset + sizeof(uint32_t) <= buffer.size());
            write_le32(buffer.data() + offset, value);
        }

        void append_string(services::wal::buffer_t& buffer, std::string_view value) {
            if (value.size() > std::numeric_limits<uint32_t>::max()) {
                throw std::runtime_error("data_chunk_binary: string is too large to serialize");
            }
            append_le32(buffer, static_cast<uint32_t>(value.size()));
            if (!value.empty()) {
                append_raw(buffer, value.data(), value.size());
            }
        }

        class binary_reader_t {
        public:
            binary_reader_t(const char* data, size_t len)
                : pointer_(data)
                , end_(data + len) {}

            bool read_u8(uint8_t& value) {
                if (remaining() < 1) {
                    return false;
                }
                value = static_cast<uint8_t>(*pointer_++);
                return true;
            }

            bool read_le16(uint16_t& value) {
                if (remaining() < 2) {
                    return false;
                }
                std::memcpy(&value, pointer_, 2);
                pointer_ += 2;
                return true;
            }

            bool read_le32(uint32_t& value) {
                if (remaining() < 4) {
                    return false;
                }
                std::memcpy(&value, pointer_, 4);
                pointer_ += 4;
                return true;
            }

            bool read_le64(uint64_t& value) {
                if (remaining() < 8) {
                    return false;
                }
                std::memcpy(&value, pointer_, 8);
                pointer_ += 8;
                return true;
            }

            bool read_raw(void* out, size_t size) {
                if (remaining() < size) {
                    return false;
                }
                std::memcpy(out, pointer_, size);
                pointer_ += size;
                return true;
            }

            bool skip(size_t size) {
                if (remaining() < size) {
                    return false;
                }
                pointer_ += size;
                return true;
            }

            bool read_string(std::string& value) {
                uint32_t size = 0;
                if (!read_le32(size) || remaining() < size) {
                    return false;
                }
                value.assign(pointer_, size);
                pointer_ += size;
                return true;
            }

            const char* current() const { return pointer_; }
            size_t remaining() const { return static_cast<size_t>(end_ - pointer_); }
            bool done() const { return pointer_ == end_; }

        private:
            const char* pointer_;
            const char* end_;
        };

        void write_full_type(services::wal::buffer_t& buffer, const types::complex_logical_type& type) {
            append_u8(buffer, static_cast<uint8_t>(type.type()));
            append_string(buffer, type.has_alias() ? std::string_view(type.alias()) : std::string_view());

            switch (type.type()) {
                case types::logical_type::DECIMAL: {
                    const auto* decimal_extension =
                        static_cast<const types::decimal_logical_type_extension*>(type.extension());
                    append_u8(buffer, decimal_extension->width());
                    append_u8(buffer, decimal_extension->scale());
                    break;
                }
                case types::logical_type::ENUM: {
                    append_string(buffer, type.type_name());
                    const auto* enum_extension =
                        static_cast<const types::enum_logical_type_extension*>(type.extension());
                    const auto& entries = enum_extension->entries();
                    append_le32(buffer, static_cast<uint32_t>(entries.size()));
                    for (const auto& entry : entries) {
                        append_string(buffer,
                                      entry.type().has_alias() ? std::string_view(entry.type().alias())
                                                               : std::string_view());
                        append_trivial(buffer, entry.value<int32_t>());
                    }
                    break;
                }
                case types::logical_type::UNKNOWN:
                    append_string(buffer, type.type_name());
                    break;
                case types::logical_type::LIST:
                    write_full_type(buffer, type.child_type());
                    break;
                case types::logical_type::ARRAY: {
                    const auto* array_extension =
                        static_cast<const types::array_logical_type_extension*>(type.extension());
                    append_le64(buffer, static_cast<uint64_t>(array_extension->size()));
                    write_full_type(buffer, array_extension->internal_type());
                    break;
                }
                case types::logical_type::MAP: {
                    const auto* map_extension = static_cast<const types::map_logical_type_extension*>(type.extension());
                    write_full_type(buffer, map_extension->key());
                    write_full_type(buffer, map_extension->value());
                    break;
                }
                case types::logical_type::STRUCT: {
                    append_string(buffer, type.type_name());
                    const auto& children = type.child_types();
                    append_le32(buffer, static_cast<uint32_t>(children.size()));
                    for (const auto& child : children) {
                        write_full_type(buffer, child);
                    }
                    break;
                }
                case types::logical_type::UNION: {
                    const auto& children = type.child_types();
                    const auto member_count = children.empty() ? 0 : children.size() - 1;
                    append_le32(buffer, static_cast<uint32_t>(member_count));
                    for (size_t i = 1; i < children.size(); ++i) {
                        write_full_type(buffer, children[i]);
                    }
                    break;
                }
                default:
                    break;
            }
        }

        types::complex_logical_type
        read_full_type(binary_reader_t& reader, std::pmr::memory_resource* resource, bool& ok) {
            uint8_t type_byte = 0;
            std::string alias;
            if (!reader.read_u8(type_byte) || !reader.read_string(alias)) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }

            auto logical_type_value = static_cast<types::logical_type>(type_byte);
            auto finish_with_alias = [&alias](types::complex_logical_type type) {
                if (!alias.empty()) {
                    type.set_alias(alias);
                }
                return type;
            };

            switch (logical_type_value) {
                case types::logical_type::DECIMAL: {
                    uint8_t width = 0;
                    uint8_t scale = 0;
                    if (!reader.read_u8(width) || !reader.read_u8(scale)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    return types::complex_logical_type::create_decimal(width, scale, std::move(alias));
                }
                case types::logical_type::ENUM: {
                    std::string type_name;
                    uint32_t entry_count = 0;
                    if (!reader.read_string(type_name) || !reader.read_le32(entry_count)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    std::vector<types::logical_value_t> entries;
                    entries.reserve(entry_count);
                    for (uint32_t i = 0; i < entry_count; ++i) {
                        std::string entry_alias;
                        int32_t entry_value = 0;
                        if (!reader.read_string(entry_alias) || !reader.read_raw(&entry_value, sizeof(entry_value))) {
                            ok = false;
                            return types::complex_logical_type{types::logical_type::INVALID};
                        }
                        types::logical_value_t value(resource, entry_value);
                        if (!entry_alias.empty()) {
                            value.set_alias(entry_alias);
                        }
                        entries.emplace_back(std::move(value));
                    }
                    return types::complex_logical_type::create_enum(type_name, std::move(entries), std::move(alias));
                }
                case types::logical_type::UNKNOWN: {
                    std::string type_name;
                    if (!reader.read_string(type_name)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    return types::complex_logical_type::create_unknown(type_name, std::move(alias));
                }
                case types::logical_type::LIST: {
                    auto child = read_full_type(reader, resource, ok);
                    if (!ok) {
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    return types::complex_logical_type::create_list(child, std::move(alias));
                }
                case types::logical_type::ARRAY: {
                    uint64_t array_size = 0;
                    if (!reader.read_le64(array_size)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    auto child = read_full_type(reader, resource, ok);
                    if (!ok) {
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    return types::complex_logical_type::create_array(child,
                                                                     static_cast<size_t>(array_size),
                                                                     std::move(alias));
                }
                case types::logical_type::MAP: {
                    auto key = read_full_type(reader, resource, ok);
                    auto value = read_full_type(reader, resource, ok);
                    if (!ok) {
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    return types::complex_logical_type::create_map(resource, key, value, std::move(alias));
                }
                case types::logical_type::STRUCT: {
                    std::string type_name;
                    uint32_t child_count = 0;
                    if (!reader.read_string(type_name) || !reader.read_le32(child_count)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    std::pmr::vector<types::complex_logical_type> children(resource);
                    children.reserve(child_count);
                    for (uint32_t i = 0; i < child_count; ++i) {
                        children.emplace_back(read_full_type(reader, resource, ok));
                        if (!ok) {
                            return types::complex_logical_type{types::logical_type::INVALID};
                        }
                    }
                    return types::complex_logical_type::create_struct(type_name, children, std::move(alias));
                }
                case types::logical_type::UNION: {
                    uint32_t child_count = 0;
                    if (!reader.read_le32(child_count)) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    std::pmr::vector<types::complex_logical_type> children(resource);
                    children.reserve(child_count);
                    for (uint32_t i = 0; i < child_count; ++i) {
                        children.emplace_back(read_full_type(reader, resource, ok));
                        if (!ok) {
                            return types::complex_logical_type{types::logical_type::INVALID};
                        }
                    }
                    return types::complex_logical_type::create_union(std::move(children), std::move(alias));
                }
                case types::logical_type::VARIANT:
                    return types::complex_logical_type::create_variant(resource, std::move(alias));
                default:
                    return finish_with_alias(types::complex_logical_type{logical_type_value});
            }
        }

        void write_nullable_value(services::wal::buffer_t& buffer,
                                  const types::complex_logical_type& type,
                                  const types::logical_value_t& value);

        types::logical_value_t read_nullable_value(binary_reader_t& reader,
                                                   std::pmr::memory_resource* resource,
                                                   const types::complex_logical_type& type,
                                                   bool& ok);

        void write_value_payload(services::wal::buffer_t& buffer,
                                 const types::complex_logical_type& type,
                                 const types::logical_value_t& value) {
            switch (type.type()) {
                case types::logical_type::BOOLEAN:
                    append_u8(buffer, value.value<bool>() ? 1 : 0);
                    break;
                case types::logical_type::TINYINT:
                    append_trivial(buffer, value.value<int8_t>());
                    break;
                case types::logical_type::UTINYINT:
                    append_trivial(buffer, value.value<uint8_t>());
                    break;
                case types::logical_type::SMALLINT:
                    append_trivial(buffer, value.value<int16_t>());
                    break;
                case types::logical_type::USMALLINT:
                    append_trivial(buffer, value.value<uint16_t>());
                    break;
                case types::logical_type::INTEGER:
                    append_trivial(buffer, value.value<int32_t>());
                    break;
                case types::logical_type::UINTEGER:
                    append_trivial(buffer, value.value<uint32_t>());
                    break;
                case types::logical_type::BIGINT:
                    append_trivial(buffer, value.value<int64_t>());
                    break;
                case types::logical_type::UBIGINT:
                    append_trivial(buffer, value.value<uint64_t>());
                    break;
                case types::logical_type::HUGEINT:
                    append_trivial(buffer, value.value<types::int128_t>());
                    break;
                case types::logical_type::UHUGEINT:
                    append_trivial(buffer, value.value<types::uint128_t>());
                    break;
                case types::logical_type::FLOAT:
                    append_trivial(buffer, value.value<float>());
                    break;
                case types::logical_type::DOUBLE:
                    append_trivial(buffer, value.value<double>());
                    break;
                case types::logical_type::STRING_LITERAL:
                    append_string(buffer, value.value<std::string_view>());
                    break;
                case types::logical_type::DATE:
                    append_trivial(buffer, value.value<core::date::date_t>().value.count());
                    break;
                case types::logical_type::TIME:
                    append_trivial(buffer, value.value<core::date::time_t>().value.count());
                    break;
                case types::logical_type::TIMESTAMP:
                    append_trivial(buffer, value.value<core::date::timestamp_t>().value.count());
                    break;
                case types::logical_type::TIMESTAMP_TZ:
                    append_trivial(buffer, value.value<core::date::timestamptz_t>().value.count());
                    break;
                case types::logical_type::TIME_TZ: {
                    auto timetz = value.value<core::date::timetz_t>();
                    append_trivial(buffer, timetz.time.count());
                    append_trivial(buffer, timetz.zone.count());
                    break;
                }
                case types::logical_type::INTERVAL: {
                    auto interval = value.value<core::date::interval_t>();
                    append_trivial(buffer, interval.time.count());
                    append_trivial(buffer, interval.day.count());
                    append_trivial(buffer, interval.month.count());
                    break;
                }
                case types::logical_type::DECIMAL:
                    switch (type.to_physical_type()) {
                        case types::physical_type::INT16:
                            append_trivial(buffer, value.value<int16_t>());
                            break;
                        case types::physical_type::INT32:
                            append_trivial(buffer, value.value<int32_t>());
                            break;
                        case types::physical_type::INT64:
                            append_trivial(buffer, value.value<int64_t>());
                            break;
                        case types::physical_type::INT128:
                            append_trivial(buffer, value.value<types::int128_t>());
                            break;
                        default:
                            throw std::runtime_error("data_chunk_binary: unsupported DECIMAL physical type");
                    }
                    break;
                case types::logical_type::ENUM:
                    append_trivial(buffer, value.value<int32_t>());
                    break;
                case types::logical_type::LIST: {
                    const auto& children = value.children();
                    append_le32(buffer, static_cast<uint32_t>(children.size()));
                    for (const auto& child : children) {
                        write_nullable_value(buffer, type.child_type(), child);
                    }
                    break;
                }
                case types::logical_type::ARRAY: {
                    const auto& children = value.children();
                    append_le32(buffer, static_cast<uint32_t>(children.size()));
                    for (const auto& child : children) {
                        write_nullable_value(buffer, type.child_type(), child);
                    }
                    break;
                }
                case types::logical_type::STRUCT: {
                    const auto& children = value.children();
                    const auto& child_types = type.child_types();
                    if (children.size() != child_types.size()) {
                        throw std::runtime_error("data_chunk_binary: STRUCT value does not match type");
                    }
                    for (size_t i = 0; i < children.size(); ++i) {
                        write_nullable_value(buffer, child_types[i], children[i]);
                    }
                    break;
                }
                case types::logical_type::UNION: {
                    const auto& children = value.children();
                    if (children.empty()) {
                        throw std::runtime_error("data_chunk_binary: UNION value without tag");
                    }
                    const auto tag = children[0].value<uint8_t>();
                    const auto& child_types = type.child_types();
                    if (static_cast<size_t>(tag) + 1 >= child_types.size() ||
                        static_cast<size_t>(tag) + 1 >= children.size()) {
                        throw std::runtime_error("data_chunk_binary: UNION tag out of range");
                    }
                    append_u8(buffer, tag);
                    write_nullable_value(buffer, child_types[static_cast<size_t>(tag) + 1],
                                         children[static_cast<size_t>(tag) + 1]);
                    break;
                }
                default:
                    throw std::runtime_error("data_chunk_binary: unsupported logical type in extended WAL chunk");
            }
        }

        void write_nullable_value(services::wal::buffer_t& buffer,
                                  const types::complex_logical_type& type,
                                  const types::logical_value_t& value) {
            append_u8(buffer, value.is_null() ? 0 : 1);
            if (!value.is_null()) {
                write_value_payload(buffer, type, value);
            }
        }

        template<typename T>
        bool read_trivial(binary_reader_t& reader, T& value) {
            return reader.read_raw(&value, sizeof(T));
        }

        types::logical_value_t read_value_payload(binary_reader_t& reader,
                                                  std::pmr::memory_resource* resource,
                                                  const types::complex_logical_type& type,
                                                  bool& ok) {
            switch (type.type()) {
                case types::logical_type::BOOLEAN: {
                    uint8_t value = 0;
                    ok = reader.read_u8(value);
                    return ok ? types::logical_value_t(resource, value != 0)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::TINYINT: {
                    int8_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::UTINYINT: {
                    uint8_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::SMALLINT: {
                    int16_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::USMALLINT: {
                    uint16_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::INTEGER: {
                    int32_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::UINTEGER: {
                    uint32_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::BIGINT: {
                    int64_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::UBIGINT: {
                    uint64_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::HUGEINT: {
                    types::int128_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::UHUGEINT: {
                    types::uint128_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::FLOAT: {
                    float value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::DOUBLE: {
                    double value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t(resource, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::STRING_LITERAL: {
                    std::string value;
                    ok = reader.read_string(value);
                    return ok ? types::logical_value_t(resource, std::move(value))
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::DATE: {
                    int32_t days = 0;
                    ok = read_trivial(reader, days);
                    return ok ? types::logical_value_t(resource, core::date::date_t{core::date::days{days}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::TIME: {
                    int64_t micros = 0;
                    ok = read_trivial(reader, micros);
                    return ok ? types::logical_value_t(resource, core::date::time_t{core::date::microseconds{micros}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::TIMESTAMP: {
                    int64_t micros = 0;
                    ok = read_trivial(reader, micros);
                    return ok ? types::logical_value_t(resource,
                                                       core::date::timestamp_t{core::date::microseconds{micros}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::TIMESTAMP_TZ: {
                    int64_t micros = 0;
                    ok = read_trivial(reader, micros);
                    return ok ? types::logical_value_t(resource,
                                                       core::date::timestamptz_t{core::date::microseconds{micros}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::TIME_TZ: {
                    int64_t micros = 0;
                    int32_t zone = 0;
                    ok = read_trivial(reader, micros) && read_trivial(reader, zone);
                    return ok ? types::logical_value_t(
                                    resource,
                                    core::date::timetz_t{core::date::microseconds{micros},
                                                         core::date::seconds_i32{zone}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::INTERVAL: {
                    int64_t micros = 0;
                    int32_t days = 0;
                    int32_t months = 0;
                    ok = read_trivial(reader, micros) && read_trivial(reader, days) && read_trivial(reader, months);
                    return ok ? types::logical_value_t(
                                    resource,
                                    core::date::interval_t{core::date::microseconds{micros},
                                                           core::date::days{days},
                                                           core::date::months{months}})
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::DECIMAL:
                    switch (type.to_physical_type()) {
                        case types::physical_type::INT16: {
                            int16_t value = 0;
                            ok = read_trivial(reader, value);
                            return ok ? types::logical_value_t::create_decimal(resource,
                                                                               type,
                                                                               static_cast<int64_t>(value))
                                      : types::logical_value_t(resource, types::logical_type::NA);
                        }
                        case types::physical_type::INT32: {
                            int32_t value = 0;
                            ok = read_trivial(reader, value);
                            return ok ? types::logical_value_t::create_decimal(resource,
                                                                               type,
                                                                               static_cast<int64_t>(value))
                                      : types::logical_value_t(resource, types::logical_type::NA);
                        }
                        case types::physical_type::INT64: {
                            int64_t value = 0;
                            ok = read_trivial(reader, value);
                            return ok ? types::logical_value_t::create_decimal(resource, type, value)
                                      : types::logical_value_t(resource, types::logical_type::NA);
                        }
                        case types::physical_type::INT128: {
                            types::int128_t value = 0;
                            ok = read_trivial(reader, value);
                            return ok ? types::logical_value_t::create_decimal(resource, type, value)
                                      : types::logical_value_t(resource, types::logical_type::NA);
                        }
                        default:
                            ok = false;
                            return types::logical_value_t(resource, types::logical_type::NA);
                    }
                case types::logical_type::ENUM: {
                    int32_t value = 0;
                    ok = read_trivial(reader, value);
                    return ok ? types::logical_value_t::create_enum(resource, type, value)
                              : types::logical_value_t(resource, types::logical_type::NA);
                }
                case types::logical_type::LIST: {
                    uint32_t child_count = 0;
                    if (!reader.read_le32(child_count)) {
                        ok = false;
                        return types::logical_value_t(resource, types::logical_type::NA);
                    }
                    std::vector<types::logical_value_t> children;
                    children.reserve(child_count);
                    for (uint32_t i = 0; i < child_count; ++i) {
                        children.emplace_back(read_nullable_value(reader, resource, type.child_type(), ok));
                        if (!ok) {
                            return types::logical_value_t(resource, types::logical_type::NA);
                        }
                    }
                    return types::logical_value_t::create_list(resource, type.child_type(), children);
                }
                case types::logical_type::ARRAY: {
                    uint32_t child_count = 0;
                    if (!reader.read_le32(child_count)) {
                        ok = false;
                        return types::logical_value_t(resource, types::logical_type::NA);
                    }
                    std::vector<types::logical_value_t> children;
                    children.reserve(child_count);
                    for (uint32_t i = 0; i < child_count; ++i) {
                        children.emplace_back(read_nullable_value(reader, resource, type.child_type(), ok));
                        if (!ok) {
                            return types::logical_value_t(resource, types::logical_type::NA);
                        }
                    }
                    return types::logical_value_t::create_array(resource, type.child_type(), children);
                }
                case types::logical_type::STRUCT: {
                    const auto& child_types = type.child_types();
                    std::vector<types::logical_value_t> children;
                    children.reserve(child_types.size());
                    for (const auto& child_type : child_types) {
                        children.emplace_back(read_nullable_value(reader, resource, child_type, ok));
                        if (!ok) {
                            return types::logical_value_t(resource, types::logical_type::NA);
                        }
                    }
                    return types::logical_value_t::create_struct(resource, type, children);
                }
                case types::logical_type::UNION: {
                    uint8_t tag = 0;
                    if (!reader.read_u8(tag)) {
                        ok = false;
                        return types::logical_value_t(resource, types::logical_type::NA);
                    }
                    const auto& encoded_children = type.child_types();
                    if (static_cast<size_t>(tag) + 1 >= encoded_children.size()) {
                        ok = false;
                        return types::logical_value_t(resource, types::logical_type::NA);
                    }
                    auto child_value =
                        read_nullable_value(reader, resource, encoded_children[static_cast<size_t>(tag) + 1], ok);
                    if (!ok) {
                        return types::logical_value_t(resource, types::logical_type::NA);
                    }
                    std::pmr::vector<types::complex_logical_type> members(resource);
                    members.reserve(encoded_children.size() - 1);
                    for (size_t i = 1; i < encoded_children.size(); ++i) {
                        members.emplace_back(encoded_children[i]);
                    }
                    return types::logical_value_t::create_union(resource,
                                                                std::move(members),
                                                                tag,
                                                                std::move(child_value));
                }
                default:
                    ok = false;
                    return types::logical_value_t(resource, types::logical_type::NA);
            }
        }

        types::logical_value_t read_nullable_value(binary_reader_t& reader,
                                                   std::pmr::memory_resource* resource,
                                                   const types::complex_logical_type& type,
                                                   bool& ok) {
            uint8_t valid = 0;
            if (!reader.read_u8(valid)) {
                ok = false;
                return types::logical_value_t(resource, types::logical_type::NA);
            }
            if (valid == 0) {
                return types::logical_value_t(resource, types::logical_type::NA);
            }
            return read_value_payload(reader, resource, type, ok);
        }

        void serialize_binary_extended(const data_chunk_t& chunk, services::wal::buffer_t& buffer) {
            if (chunk.column_count() > std::numeric_limits<uint32_t>::max() ||
                chunk.size() > std::numeric_limits<uint32_t>::max()) {
                throw std::runtime_error("data_chunk_binary: data chunk is too large to serialize");
            }

            append_le16(buffer, kExtendedFormatMagic);
            append_le16(buffer, kExtendedFormatVersion);
            append_le32(buffer, static_cast<uint32_t>(chunk.column_count()));
            append_le32(buffer, static_cast<uint32_t>(chunk.size()));

            for (uint64_t column_index = 0; column_index < chunk.column_count(); ++column_index) {
                const auto& column = chunk.data[column_index];
                write_full_type(buffer, column.type());

                const auto size_offset = buffer.size();
                append_le32(buffer, 0);
                const auto payload_offset = buffer.size();
                for (uint64_t row_index = 0; row_index < chunk.size(); ++row_index) {
                    write_nullable_value(buffer, column.type(), column.value(row_index));
                }
                const auto payload_size = buffer.size() - payload_offset;
                if (payload_size > std::numeric_limits<uint32_t>::max()) {
                    throw std::runtime_error("data_chunk_binary: column payload is too large to serialize");
                }
                patch_le32(buffer, size_offset, static_cast<uint32_t>(payload_size));
            }
        }

        data_chunk_t
        deserialize_binary_extended(const char* data, size_t len, std::pmr::memory_resource* resource, bool& ok) {
            binary_reader_t reader(data, len);
            uint16_t magic = 0;
            uint16_t version = 0;
            uint32_t num_columns = 0;
            uint32_t num_rows = 0;
            if (!reader.read_le16(magic) || !reader.read_le16(version) || magic != kExtendedFormatMagic ||
                version != kExtendedFormatVersion || !reader.read_le32(num_columns) || !reader.read_le32(num_rows)) {
                ok = false;
                return make_empty_error_chunk(resource);
            }

            std::pmr::vector<types::complex_logical_type> column_types(resource);
            column_types.reserve(num_columns);
            std::vector<std::pair<const char*, uint32_t>> payloads;
            payloads.reserve(num_columns);

            for (uint32_t column_index = 0; column_index < num_columns; ++column_index) {
                column_types.emplace_back(read_full_type(reader, resource, ok));
                if (!ok) {
                    return make_empty_error_chunk(resource);
                }
                uint32_t payload_size = 0;
                if (!reader.read_le32(payload_size) || reader.remaining() < payload_size) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                payloads.emplace_back(reader.current(), payload_size);
                if (!reader.skip(payload_size)) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
            }

            data_chunk_t chunk(resource, column_types, num_rows);
            chunk.set_cardinality(num_rows);

            for (uint32_t column_index = 0; column_index < num_columns; ++column_index) {
                binary_reader_t payload_reader(payloads[column_index].first, payloads[column_index].second);
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    auto value = read_nullable_value(payload_reader, resource, column_types[column_index], ok);
                    if (!ok) {
                        return make_empty_error_chunk(resource);
                    }
                    chunk.set_value(column_index, row_index, value);
                }
                if (!payload_reader.done()) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
            }

            return chunk;
        }

        // Compute the size of the type header for a single column.
        // Format: [logical_type:1][alias_length:2][alias:N][extension_type:1][extension_data:0-5]
        uint32_t compute_type_header_size(const types::complex_logical_type& column_type) {
            uint32_t header_size = 1 + 2; // logical_type + alias_length
            if (column_type.has_alias()) {
                header_size += static_cast<uint32_t>(column_type.alias().size());
            }
            header_size += 1; // extension_type byte
            auto* extension = column_type.extension();
            if (extension) {
                switch (extension->type()) {
                    case types::logical_type_extension::extension_type::ARRAY:
                        header_size += 5; // inner_logical_type(1) + array_size(4)
                        break;
                    case types::logical_type_extension::extension_type::DECIMAL:
                        header_size += 2; // width(1) + scale(1)
                        break;
                    default:
                        break;
                }
            }
            return header_size;
        }

        // Write the type header for a single column. Returns pointer past written data.
        char* write_type_header(char* output, const types::complex_logical_type& column_type) {
            // Logical type
            *reinterpret_cast<uint8_t*>(output) = static_cast<uint8_t>(column_type.type());
            output += 1;

            // Alias
            if (column_type.has_alias()) {
                auto alias_length = static_cast<uint16_t>(column_type.alias().size());
                write_le16(output, alias_length);
                output += 2;
                std::memcpy(output, column_type.alias().data(), alias_length);
                output += alias_length;
            } else {
                write_le16(output, 0);
                output += 2;
            }

            // Extension
            auto* extension = column_type.extension();
            if (!extension) {
                *reinterpret_cast<uint8_t*>(output) = 0; // no extension
                output += 1;
            } else {
                switch (extension->type()) {
                    case types::logical_type_extension::extension_type::ARRAY: {
                        *reinterpret_cast<uint8_t*>(output) = 1;
                        output += 1;
                        auto* array_extension = static_cast<const types::array_logical_type_extension*>(extension);
                        *reinterpret_cast<uint8_t*>(output) =
                            static_cast<uint8_t>(array_extension->internal_type().type());
                        output += 1;
                        write_le32(output, static_cast<uint32_t>(array_extension->size()));
                        output += 4;
                        break;
                    }
                    case types::logical_type_extension::extension_type::DECIMAL: {
                        *reinterpret_cast<uint8_t*>(output) = 2;
                        output += 1;
                        auto* decimal_extension = static_cast<const types::decimal_logical_type_extension*>(extension);
                        *reinterpret_cast<uint8_t*>(output) = decimal_extension->width();
                        output += 1;
                        *reinterpret_cast<uint8_t*>(output) = decimal_extension->scale();
                        output += 1;
                        break;
                    }
                    default:
                        *reinterpret_cast<uint8_t*>(output) = 0; // unknown extension → none
                        output += 1;
                        break;
                }
            }

            return output;
        }

        // Read the type header for a single column. Advances scan pointer. On any
        // buffer-overflow sets ok=false and returns an INVALID-typed placeholder
        // (caller must check ok before using the result).
        types::complex_logical_type read_type_header(const char*& scan, const char* end, bool& ok) {
            if (scan + 4 > end) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }

            // Logical type
            auto logical_type_value = static_cast<types::logical_type>(*reinterpret_cast<const uint8_t*>(scan));
            scan += 1;

            // Alias
            uint16_t alias_length = read_le16(scan);
            scan += 2;
            std::string alias;
            if (alias_length > 0) {
                if (scan + alias_length > end) {
                    ok = false;
                    return types::complex_logical_type{types::logical_type::INVALID};
                }
                alias.assign(scan, alias_length);
                scan += alias_length;
            }

            // Extension type
            if (scan >= end) {
                ok = false;
                return types::complex_logical_type{types::logical_type::INVALID};
            }
            uint8_t extension_type = *reinterpret_cast<const uint8_t*>(scan);
            scan += 1;

            switch (extension_type) {
                case 1: { // ARRAY
                    if (scan + 5 > end) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    auto inner_logical_type = static_cast<types::logical_type>(*reinterpret_cast<const uint8_t*>(scan));
                    scan += 1;
                    uint32_t array_size = read_le32(scan);
                    scan += 4;
                    return types::complex_logical_type::create_array(inner_logical_type,
                                                                     static_cast<size_t>(array_size),
                                                                     std::move(alias));
                }
                case 2: { // DECIMAL
                    if (scan + 2 > end) {
                        ok = false;
                        return types::complex_logical_type{types::logical_type::INVALID};
                    }
                    uint8_t width = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    uint8_t scale = *reinterpret_cast<const uint8_t*>(scan);
                    scan += 1;
                    return types::complex_logical_type::create_decimal(width, scale, std::move(alias));
                }
                default: // no extension
                    return types::complex_logical_type(logical_type_value, std::move(alias));
            }
        }

        // Empty/sentinel chunk returned on deserialize failure. Caller must check
        // the ok flag and discard the chunk on failure.
        data_chunk_t make_empty_error_chunk(std::pmr::memory_resource* resource) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return data_chunk_t(resource, empty_types, 1);
        }

    } // anonymous namespace

    // -----------------------------------------------------------------------
    // serialize_binary
    // -----------------------------------------------------------------------
    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer) {
        if (!uses_legacy_binary_format(chunk)) {
            serialize_binary_extended(chunk, buffer);
            return;
        }

        const auto num_columns = static_cast<uint16_t>(chunk.column_count());
        const auto num_rows = static_cast<uint32_t>(chunk.size());

        // ----- Build null mask (row-major, 1 bit per cell, bit=1 means valid) -----
        const uint64_t total_cells = static_cast<uint64_t>(num_columns) * num_rows;
        const uint32_t null_mask_bytes = (total_cells > 0) ? static_cast<uint32_t>((total_cells + 7) / 8) : 0;

        bool has_nulls = false;
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            if (!column.validity().all_valid()) {
                has_nulls = true;
                break;
            }
        }

        const uint32_t actual_mask_bytes = has_nulls ? null_mask_bytes : 0;

        // ----- Pre-compute total size to minimise reallocations -----
        // header: 2 (num_columns) + 4 (num_rows) + 4 (null_mask_size) + actual_mask_bytes
        size_t total = 2 + 4 + 4 + actual_mask_bytes;

        // Per-column: type_header + 4 (data_size) + data_size
        std::vector<uint32_t> column_data_sizes(num_columns);
        std::vector<uint32_t> column_type_header_sizes(num_columns);

        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            auto physical_type = column.type().to_physical_type();

            if (is_variable_type(physical_type)) {
                uint32_t offsets_size = (num_rows + 1) * 4;
                uint32_t string_data_size = 0;
                const auto* views = reinterpret_cast<const std::string_view*>(column.data());
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    string_data_size += static_cast<uint32_t>(views[row_index].size());
                }
                column_data_sizes[column_index] = offsets_size + string_data_size;
            } else {
                size_t element_size = fixed_type_size(physical_type);
                column_data_sizes[column_index] = static_cast<uint32_t>(element_size * num_rows);
            }

            column_type_header_sizes[column_index] = compute_type_header_size(column.type());
            total += column_type_header_sizes[column_index] + 4 + column_data_sizes[column_index];
        }

        const size_t base = buffer.size();
        buffer.resize(base + total);
        char* output = buffer.data() + base;

        // ----- Write header -----
        write_le16(output, num_columns);
        output += 2;
        write_le32(output, num_rows);
        output += 4;
        write_le32(output, actual_mask_bytes);
        output += 4;

        // ----- Write null mask -----
        if (has_nulls) {
            std::memset(output, 0, actual_mask_bytes);
            for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
                const auto& column = chunk.data[column_index];
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint64_t bit_index = static_cast<uint64_t>(row_index) * num_columns + column_index;
                    if (column.validity().all_valid() || column.validity().row_is_valid(row_index)) {
                        output[bit_index / 8] |= static_cast<char>(1u << (bit_index % 8));
                    }
                }
            }
            output += actual_mask_bytes;
        }

        // ----- Write columns -----
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            const auto& column = chunk.data[column_index];
            auto physical_type = column.type().to_physical_type();

            // Write type header (logical_type + alias + extension)
            output = write_type_header(output, column.type());

            // Write data_size
            write_le32(output, column_data_sizes[column_index]);
            output += 4;

            // Write data
            if (is_variable_type(physical_type)) {
                const auto* views = reinterpret_cast<const std::string_view*>(column.data());
                uint32_t running_offset = 0;
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    write_le32(output, running_offset);
                    output += 4;
                    running_offset += static_cast<uint32_t>(views[row_index].size());
                }
                write_le32(output, running_offset);
                output += 4;
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    std::memcpy(output, views[row_index].data(), views[row_index].size());
                    output += views[row_index].size();
                }
            } else {
                std::memcpy(output, column.data(), column_data_sizes[column_index]);
                output += column_data_sizes[column_index];
            }
        }

        assert(static_cast<size_t>(output - buffer.data()) - base == total);
    }

    // -----------------------------------------------------------------------
    // deserialize_binary
    // -----------------------------------------------------------------------
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource, bool& ok) {
        ok = true;
        if (len < 10) {
            ok = false;
            return make_empty_error_chunk(resource);
        }

        if (read_le16(data) == kExtendedFormatMagic) {
            return deserialize_binary_extended(data, len, resource, ok);
        }

        const char* pointer = data;
        const char* end = data + len;

        uint16_t num_columns = read_le16(pointer);
        pointer += 2;
        uint32_t num_rows = read_le32(pointer);
        pointer += 4;
        uint32_t null_mask_size = read_le32(pointer);
        pointer += 4;

        const char* null_mask = nullptr;
        if (null_mask_size > 0) {
            if (pointer + null_mask_size > end) {
                ok = false;
                return make_empty_error_chunk(resource);
            }
            null_mask = pointer;
            pointer += null_mask_size;
        }

        // First pass: read column types (peek ahead).
        std::pmr::vector<types::complex_logical_type> column_types(resource);
        column_types.reserve(num_columns);

        {
            const char* scan = pointer;
            for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
                // Read type header
                auto column_type = read_type_header(scan, end, ok);
                if (!ok) {
                    return make_empty_error_chunk(resource);
                }
                column_types.push_back(std::move(column_type));

                // Skip data_size + data
                if (scan + 4 > end) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                uint32_t data_size = read_le32(scan);
                scan += 4;
                if (scan + data_size > end) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                scan += data_size;
            }
        }

        data_chunk_t chunk(resource, column_types, num_rows);
        chunk.set_cardinality(num_rows);

        // Second pass: populate column data.
        for (uint16_t column_index = 0; column_index < num_columns; ++column_index) {
            // Skip type header (already parsed in first pass)
            read_type_header(pointer, end, ok);
            if (!ok) {
                return make_empty_error_chunk(resource);
            }

            uint32_t data_size = read_le32(pointer);
            pointer += 4;

            auto& column = chunk.data[column_index];
            auto physical_type = column_types[column_index].to_physical_type();

            if (is_variable_type(physical_type)) {
                if (data_size < (num_rows + 1) * 4) {
                    ok = false;
                    return make_empty_error_chunk(resource);
                }
                const char* offsets_pointer = pointer;
                const char* string_data = pointer + (num_rows + 1) * 4;

                auto* views = reinterpret_cast<std::string_view*>(column.data());
                auto string_buffer = std::make_shared<string_vector_buffer_t>(resource);

                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint32_t offset_begin = read_le32(offsets_pointer + row_index * 4);
                    uint32_t offset_end = read_le32(offsets_pointer + (row_index + 1) * 4);
                    uint32_t string_length = offset_end - offset_begin;

                    if (string_length > 0) {
                        void* heap_pointer = string_buffer->insert(
                            const_cast<void*>(static_cast<const void*>(string_data + offset_begin)),
                            string_length);
                        views[row_index] = std::string_view(reinterpret_cast<const char*>(heap_pointer), string_length);
                    } else {
                        views[row_index] = std::string_view();
                    }
                }

                column.set_auxiliary(std::move(string_buffer));
            } else {
                std::memcpy(column.data(), pointer, data_size);
            }
            pointer += data_size;

            // Apply null mask for this column.
            if (null_mask) {
                for (uint32_t row_index = 0; row_index < num_rows; ++row_index) {
                    uint64_t bit_index = static_cast<uint64_t>(row_index) * num_columns + column_index;
                    bool valid = (static_cast<unsigned char>(null_mask[bit_index / 8]) >> (bit_index % 8)) & 1u;
                    if (!valid) {
                        column.validity().set_invalid(row_index);
                    }
                }
            }
        }

        return chunk;
    }

} // namespace components::vector
