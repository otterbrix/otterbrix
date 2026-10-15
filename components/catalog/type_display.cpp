#include "type_display.hpp"

#include "system_table_schemas.hpp"

namespace components::catalog {

    using namespace types;

    std::string describe_type(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::ENUM:
            case logical_type::STRUCT:
            case logical_type::UNKNOWN:
                if (type.extension() != nullptr && !type.type_name().empty()) {
                    return type.type_name();
                }
                if (type.type() == logical_type::UNKNOWN) {
                    return "unknown";
                }
                break;
            case logical_type::NA:
                return "unknown";
            case logical_type::LIST:
            case logical_type::ARRAY:
                return describe_type(type.child_type()) + "[]";
            default:
                break;
        }
        return std::string{logical_type_to_pg_name(type.type())};
    }

    std::string format_type_be(const complex_logical_type& type, bool with_typmod) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                return "boolean";
            case logical_type::SMALLINT:
                return "smallint";
            case logical_type::INTEGER:
                return "integer";
            case logical_type::BIGINT:
                return "bigint";
            case logical_type::FLOAT:
                return "real";
            case logical_type::DOUBLE:
                return "double precision";
            case logical_type::TIMESTAMP:
                return "timestamp without time zone";
            case logical_type::TIME:
                return "time without time zone";
            case logical_type::DECIMAL: {
                const auto* decimal = type.extension_as<decimal_logical_type_extension>();
                if (decimal == nullptr || !with_typmod) {
                    return "numeric";
                }
                return "numeric(" + std::to_string(decimal->width()) + "," + std::to_string(decimal->scale()) + ")";
            }
            case logical_type::LIST:
                return format_type_be(type.child_type(), with_typmod) + "[]";
            case logical_type::ARRAY: {
                const auto* array = type.extension_as<array_logical_type_extension>();
                const bool sized = with_typmod && array != nullptr;
                return format_type_be(type.child_type(), with_typmod) + "[" +
                       (sized ? std::to_string(array->size()) : std::string{}) + "]";
            }
            default:
                return describe_type(type);
        }
    }

} // namespace components::catalog
