#include "type_spec.hpp"

#include "logical_value.hpp"

#include <charconv>
#include <cstdint>
#include <string>
#include <vector>

namespace components::types {
    namespace {

        std::string_view scalar_type_to_name(logical_type type) {
            switch (type) {
                case logical_type::BOOLEAN:
                    return "bool";
                case logical_type::TINYINT:
                    return "int1";
                case logical_type::UTINYINT:
                    return "uint1";
                case logical_type::SMALLINT:
                    return "int2";
                case logical_type::USMALLINT:
                    return "uint2";
                case logical_type::INTEGER:
                    return "int4";
                case logical_type::UINTEGER:
                    return "uint4";
                case logical_type::BIGINT:
                    return "int8";
                case logical_type::UBIGINT:
                    return "uint8";
                case logical_type::HUGEINT:
                    return "hugeint";
                case logical_type::UHUGEINT:
                    return "uhugeint";
                case logical_type::FLOAT:
                    return "float4";
                case logical_type::DOUBLE:
                    return "float8";
                case logical_type::STRING_LITERAL:
                    return "text";
                case logical_type::TIMESTAMP:
                    return "timestamp";
                case logical_type::TIMESTAMP_TZ:
                    return "timestamp with time zone";
                case logical_type::DATE:
                    return "date";
                case logical_type::TIME:
                    return "time";
                case logical_type::TIME_TZ:
                    return "time with time zone";
                case logical_type::INTERVAL:
                    return "interval";
                case logical_type::BLOB:
                    return "bytea";
                case logical_type::UUID:
                    return "uuid";
                default:
                    return "";
            }
        }

        logical_type scalar_name_to_type(std::string_view name) {
            if (name == "bool")
                return logical_type::BOOLEAN;
            if (name == "int1")
                return logical_type::TINYINT;
            if (name == "uint1")
                return logical_type::UTINYINT;
            if (name == "int2")
                return logical_type::SMALLINT;
            if (name == "uint2")
                return logical_type::USMALLINT;
            if (name == "int4")
                return logical_type::INTEGER;
            if (name == "uint4")
                return logical_type::UINTEGER;
            if (name == "int8")
                return logical_type::BIGINT;
            if (name == "uint8")
                return logical_type::UBIGINT;
            if (name == "hugeint")
                return logical_type::HUGEINT;
            if (name == "uhugeint")
                return logical_type::UHUGEINT;
            if (name == "float4")
                return logical_type::FLOAT;
            if (name == "float8")
                return logical_type::DOUBLE;
            if (name == "text")
                return logical_type::STRING_LITERAL;
            if (name == "timestamp")
                return logical_type::TIMESTAMP;
            if (name == "timestamp with time zone")
                return logical_type::TIMESTAMP_TZ;
            if (name == "date")
                return logical_type::DATE;
            if (name == "time")
                return logical_type::TIME;
            if (name == "time with time zone")
                return logical_type::TIME_TZ;
            if (name == "interval")
                return logical_type::INTERVAL;
            if (name == "bytea")
                return logical_type::BLOB;
            if (name == "uuid")
                return logical_type::UUID;

            if (name == "int16")
                return logical_type::SMALLINT;
            if (name == "int32")
                return logical_type::INTEGER;
            if (name == "int64")
                return logical_type::BIGINT;
            if (name == "float32")
                return logical_type::FLOAT;
            if (name == "float64")
                return logical_type::DOUBLE;
            if (name == "string")
                return logical_type::STRING_LITERAL;
            if (name == "blob")
                return logical_type::BLOB;
            if (name == "boolean")
                return logical_type::BOOLEAN;
            if (name == "integer")
                return logical_type::INTEGER;
            if (name == "bigint")
                return logical_type::BIGINT;

            if (name == "double")
                return logical_type::DOUBLE;
            if (name == "float")
                return logical_type::FLOAT;
            if (name == "smallint")
                return logical_type::SMALLINT;
            if (name == "tinyint")
                return logical_type::TINYINT;
            if (name == "varchar")
                return logical_type::STRING_LITERAL;
            if (name == "int8_t")
                return logical_type::BIGINT;
            return logical_type::UNKNOWN;
        }

        std::string encode_type_nested(const complex_logical_type& type) {
            auto scalar_name = scalar_type_to_name(type.type());
            if (!scalar_name.empty()) {
                return std::string(scalar_name);
            }

            if (type.type() == logical_type::DECIMAL) {
                const auto* ext = static_cast<const decimal_logical_type_extension*>(type.extension());
                return "numeric(" + std::to_string(static_cast<unsigned>(ext->width())) + "," +
                       std::to_string(static_cast<unsigned>(ext->scale())) + ")";
            }
            if (type.type() == logical_type::ENUM) {
                std::string out = "ENUM(";
                out += type.type_name();
                const auto* ext = type.extension();
                if (ext != nullptr) {
                    const auto* enum_ext = static_cast<const enum_logical_type_extension*>(ext);
                    for (const auto& entry : enum_ext->entries()) {
                        out += ',';
                        const auto& entry_type = entry.type();
                        out += entry_type.has_alias() ? entry_type.alias() : std::string{};
                        out += '=';
                        out += std::to_string(entry.value<std::int32_t>());
                    }
                }
                out += ')';
                return out;
            }
            if (type.type() == logical_type::UNKNOWN) {
                return "UNKNOWN(" + type.type_name() + ")";
            }
            if (type.type() == logical_type::LIST) {
                return "LIST(" + encode_type_nested(type.child_type()) + ")";
            }
            if (type.type() == logical_type::ARRAY) {
                const auto* ext = static_cast<const array_logical_type_extension*>(type.extension());
                return "ARRAY(" + encode_type_nested(ext->internal_type()) + "," + std::to_string(ext->size()) + ")";
            }
            if (type.type() == logical_type::MAP) {
                const auto* ext = static_cast<const map_logical_type_extension*>(type.extension());
                return "MAP(" + encode_type_nested(ext->key()) + "," + encode_type_nested(ext->value()) + ")";
            }
            if (type.type() == logical_type::STRUCT) {
                const auto* ext = static_cast<const struct_logical_type_extension*>(type.extension());
                std::string out = "STRUCT(" + ext->type_name();
                for (const auto& field : ext->child_types()) {
                    out += ',';
                    out += field.alias();
                    out += ':';
                    out += encode_type_nested(field);
                }
                out += ')';
                return out;
            }
            if (type.type() == logical_type::UNION) {
                const auto& children = type.child_types();
                std::string out = "UNION(";
                bool first = true;
                for (size_t i = 1; i < children.size(); ++i) {
                    if (!first) {
                        out += ',';
                    }
                    first = false;
                    out += children[i].alias();
                    out += ':';
                    out += encode_type_nested(children[i]);
                }
                out += ')';
                return out;
            }
            if (type.type() == logical_type::VARIANT) {
                return "VARIANT";
            }
            return "UNKNOWN(" + std::to_string(static_cast<int>(type.type())) + ")";
        }

        complex_logical_type parse_flat_type(std::pmr::memory_resource* resource, std::string_view spec, size_t& pos);

        std::string read_token(std::string_view spec, size_t& pos) {
            const size_t start = pos;
            while (pos < spec.size() && spec[pos] != '(' && spec[pos] != ')' && spec[pos] != ',' &&
                   spec[pos] != ':') {
                ++pos;
            }
            return std::string{spec.substr(start, pos - start)};
        }

        complex_logical_type parse_flat_type(std::pmr::memory_resource* resource, std::string_view spec, size_t& pos) {
            std::string name = read_token(spec, pos);

            if (pos >= spec.size() || spec[pos] != '(') {
                if (name == "VARIANT") {
                    return complex_logical_type::create_variant(resource);
                }
                auto type = scalar_name_to_type(name);
                if (type != logical_type::UNKNOWN) {
                    return complex_logical_type{type};
                }
                return complex_logical_type::create_unknown(name);
            }
            ++pos;

            if (name == "numeric" || name == "DECIMAL") {
                std::string width = read_token(spec, pos);
                ++pos;
                std::string scale = read_token(spec, pos);
                ++pos;
                int parsed_width{};
                int parsed_scale{};
                auto width_result = std::from_chars(width.data(), width.data() + width.size(), parsed_width);
                auto scale_result = std::from_chars(scale.data(), scale.data() + scale.size(), parsed_scale);
                if (width_result.ec != std::errc{} || scale_result.ec != std::errc{}) {
                    return complex_logical_type{logical_type::UNKNOWN};
                }
                return complex_logical_type::create_decimal(static_cast<uint8_t>(parsed_width),
                                                            static_cast<uint8_t>(parsed_scale));
            }
            if (name == "ENUM") {
                std::string enum_name = read_token(spec, pos);
                std::vector<logical_value_t> entries;
                while (pos < spec.size() && spec[pos] == ',') {
                    ++pos;
                    std::string token = read_token(spec, pos);
                    auto eq = token.find('=');
                    if (eq != std::string::npos) {
                        std::string label = token.substr(0, eq);
                        auto value_spec = token.substr(eq + 1);
                        int value{};
                        auto value_result =
                            std::from_chars(value_spec.data(), value_spec.data() + value_spec.size(), value);
                        if (value_result.ec != std::errc{}) {
                            return complex_logical_type{logical_type::UNKNOWN};
                        }
                        logical_value_t logical_value(resource, value);
                        logical_value.set_alias(label);
                        entries.push_back(std::move(logical_value));
                    }
                }
                if (pos < spec.size() && spec[pos] == ')') {
                    ++pos;
                }
                return complex_logical_type::create_enum(enum_name, std::move(entries));
            }
            if (name == "UNKNOWN") {
                std::string type_name = read_token(spec, pos);
                ++pos;
                return complex_logical_type::create_unknown(type_name);
            }
            if (name == "LIST") {
                auto inner = parse_flat_type(resource, spec, pos);
                ++pos;
                return complex_logical_type::create_list(inner);
            }
            if (name == "ARRAY") {
                auto inner = parse_flat_type(resource, spec, pos);
                ++pos;
                std::string size = read_token(spec, pos);
                ++pos;
                unsigned long long parsed_size{};
                auto size_result = std::from_chars(size.data(), size.data() + size.size(), parsed_size);
                if (size_result.ec != std::errc{}) {
                    return complex_logical_type{logical_type::UNKNOWN};
                }
                return complex_logical_type::create_array(inner, parsed_size);
            }
            if (name == "MAP") {
                auto key = parse_flat_type(resource, spec, pos);
                ++pos;
                auto value = parse_flat_type(resource, spec, pos);
                ++pos;
                return complex_logical_type::create_map(key, value);
            }
            if (name == "STRUCT") {
                std::string struct_name = read_token(spec, pos);
                std::pmr::vector<complex_logical_type> fields(resource);
                while (pos < spec.size() && spec[pos] == ',') {
                    ++pos;
                    std::string field_name = read_token(spec, pos);
                    ++pos;
                    auto field_type = parse_flat_type(resource, spec, pos);
                    field_type.set_alias(field_name);
                    fields.push_back(std::move(field_type));
                }
                if (pos < spec.size() && spec[pos] == ')') {
                    ++pos;
                }
                auto struct_type = complex_logical_type::create_struct(struct_name, fields);
                struct_type.set_alias(struct_name);
                return struct_type;
            }
            if (name == "UNION") {
                std::pmr::vector<complex_logical_type> fields(resource);
                if (pos < spec.size() && spec[pos] != ')') {
                    std::string field_name = read_token(spec, pos);
                    ++pos;
                    auto field_type = parse_flat_type(resource, spec, pos);
                    field_type.set_alias(field_name);
                    fields.push_back(std::move(field_type));
                }
                while (pos < spec.size() && spec[pos] == ',') {
                    ++pos;
                    std::string field_name = read_token(spec, pos);
                    ++pos;
                    auto field_type = parse_flat_type(resource, spec, pos);
                    field_type.set_alias(field_name);
                    fields.push_back(std::move(field_type));
                }
                if (pos < spec.size() && spec[pos] == ')') {
                    ++pos;
                }
                return complex_logical_type::create_union(std::move(fields));
            }

            int depth = 1;
            while (pos < spec.size() && depth > 0) {
                if (spec[pos] == '(') {
                    ++depth;
                } else if (spec[pos] == ')') {
                    --depth;
                }
                ++pos;
            }
            return complex_logical_type::create_unknown(name);
        }

    } // namespace

    std::string encode_type_spec(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
            case logical_type::TINYINT:
            case logical_type::UTINYINT:
            case logical_type::SMALLINT:
            case logical_type::USMALLINT:
            case logical_type::INTEGER:
            case logical_type::UINTEGER:
            case logical_type::BIGINT:
            case logical_type::UBIGINT:
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
            case logical_type::STRING_LITERAL:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
            case logical_type::DATE:
            case logical_type::TIME:
            case logical_type::TIME_TZ:
            case logical_type::INTERVAL:
            case logical_type::BLOB:
            case logical_type::UUID:
                return "";
            default:
                break;
        }

        if (type.type() == logical_type::ENUM) {
            std::string out = "ENUM:";
            out += type.type_name();
            out += ':';
            const auto* ext = type.extension();
            if (ext != nullptr) {
                const auto* enum_ext = static_cast<const enum_logical_type_extension*>(ext);
                bool first = true;
                for (const auto& entry : enum_ext->entries()) {
                    if (!first) {
                        out += ',';
                    }
                    first = false;
                    const auto& entry_type = entry.type();
                    out += entry_type.has_alias() ? entry_type.alias() : std::string{};
                    out += '=';
                    out += std::to_string(entry.value<std::int32_t>());
                }
            }
            return out;
        }

        return encode_type_nested(type);
    }

    complex_logical_type decode_type_spec(std::pmr::memory_resource* resource, std::string_view spec) {
        if (spec.empty()) {
            return complex_logical_type{logical_type::UNKNOWN};
        }

        if (spec.size() >= 5 && spec.compare(0, 5, "ENUM:") == 0) {
            auto rest = spec.substr(5);
            auto colon = rest.find(':');
            std::string name =
                (colon == std::string_view::npos) ? std::string{rest} : std::string{rest.substr(0, colon)};
            std::vector<logical_value_t> entries;
            if (colon != std::string_view::npos) {
                auto entries_spec = rest.substr(colon + 1);
                std::size_t i = 0;
                while (i < entries_spec.size()) {
                    std::size_t comma = entries_spec.find(',', i);
                    std::string token{
                        entries_spec.substr(i,
                                            comma == std::string_view::npos ? std::string_view::npos : comma - i)};
                    std::size_t eq = token.find('=');
                    if (eq != std::string::npos) {
                        std::string label = token.substr(0, eq);
                        const auto value_spec = token.substr(eq + 1);
                        int value{};
                        auto value_result =
                            std::from_chars(value_spec.data(), value_spec.data() + value_spec.size(), value);
                        if (value_result.ec != std::errc{}) {
                            return complex_logical_type{logical_type::UNKNOWN};
                        }
                        logical_value_t logical_value(resource, value);
                        logical_value.set_alias(label);
                        entries.push_back(std::move(logical_value));
                    }
                    if (comma == std::string_view::npos) {
                        break;
                    }
                    i = comma + 1;
                }
            }
            return complex_logical_type::create_enum(name, std::move(entries));
        }

        try {
            size_t pos = 0;
            return parse_flat_type(resource, spec, pos);
        } catch (...) {
            return complex_logical_type{logical_type::UNKNOWN};
        }
    }

} // namespace components::types
