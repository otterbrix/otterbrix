#include "convert_value.hpp"

#include <native/python_objects.hpp>
#include <core/typedefs.hpp>
#include <core/types/string.hpp>

#include <stdexcept>

using components::types::logical_type;
using components::types::logical_value_t;
using components::types::complex_logical_type;

namespace otterbrix {

    namespace util {

        namespace {

            [[nodiscard]] static bool convertible_numeric_physical_to_double(logical_type t) {
                switch (t) {
                    case logical_type::TINYINT:
                    case logical_type::SMALLINT:
                    case logical_type::INTEGER:
                    case logical_type::BIGINT:
                    case logical_type::UTINYINT:
                    case logical_type::USMALLINT:
                    case logical_type::UINTEGER:
                    case logical_type::UBIGINT:
                    case logical_type::FLOAT:
                    case logical_type::DOUBLE:
                        return true;
                    default:
                        return false;
                }
            }

            [[nodiscard]] static double numeric_logical_value_as_double(const logical_value_t& v) {
                switch (v.type().type()) {
                    case logical_type::TINYINT:
                        return static_cast<double>(v.value<int8_t>());
                    case logical_type::SMALLINT:
                        return static_cast<double>(v.value<int16_t>());
                    case logical_type::INTEGER:
                        return static_cast<double>(v.value<int32_t>());
                    case logical_type::BIGINT:
                        return static_cast<double>(v.value<int64_t>());
                    case logical_type::UTINYINT:
                        return static_cast<double>(v.value<uint8_t>());
                    case logical_type::USMALLINT:
                        return static_cast<double>(v.value<uint16_t>());
                    case logical_type::UINTEGER:
                        return static_cast<double>(v.value<uint32_t>());
                    case logical_type::UBIGINT:
                        return static_cast<double>(v.value<uint64_t>());
                    case logical_type::FLOAT:
                        return static_cast<double>(v.value<float>());
                    case logical_type::DOUBLE:
                        return v.value<double>();
                    default:
                        throw std::runtime_error("numeric_logical_value_as_double: unsupported type");
                }
            }

        } // namespace

        py::object LogicalValueToPython(const logical_value_t& value,
                const complex_logical_type& type) {
            if (value.is_null()) {
                return py::none();
            }
            const auto declared = type.type();
            const auto physical = value.type().type();
            if (declared == logical_type::DOUBLE && physical != logical_type::DOUBLE &&
                convertible_numeric_physical_to_double(physical)) {
                return py::cast(numeric_logical_value_as_double(value));
            }
            return PythonObject::FromValue(value, type);
        }

        py::dict CursorRowToPythonDict(components::cursor::cursor_t_ptr& cursor,
                uint64_t row_idx,
                const vector<components::table::column_definition_t>& col_defs) {
            py::dict result;
            for (idx_t i = 0; i < col_defs.size(); i++) {
                auto val = cursor->value(i, row_idx);
                result[py::str(col_defs[i].name())] = LogicalValueToPython(val, col_defs[i].type());
            }
            return result;
        }

        py::dict DataChunkRowToPythonDict(const components::vector::data_chunk_t& chunk,
                uint64_t row_idx,
                const vector<components::table::column_definition_t>& col_defs) {
            py::dict result;
            for (idx_t i = 0; i < col_defs.size(); i++) {
                auto val = chunk.value(i, row_idx);
                result[py::str(col_defs[i].name())] = LogicalValueToPython(val, col_defs[i].type());
            }
            return result;
        }

    } // namespace util

} // namespace otterbrix
