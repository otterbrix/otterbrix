#include "util.hpp"

#include <components/catalog/type_display.hpp>

#include <algorithm>
#include <stdexcept>
#include <string>

using namespace components::types;

namespace otterbrix { namespace util {

    int64_t rowcount_of(const components::cursor::cursor_t& cursor) {
        if (cursor.is_write()) {
            return static_cast<int64_t>(cursor.affected_rows());
        }
        return static_cast<int64_t>(cursor.size());
    }

    py::object created_oid_of(const components::cursor::cursor_t& cursor) {
        const auto oid = cursor.created_oid();
        if (oid == components::catalog::INVALID_OID) {
            return py::none();
        }
        return py::int_(oid);
    }

    core::result_wrapper_t<std::string> vector_to_string(const components::vector::vector_t& value) {
        switch (value.type().to_physical_type()) {
            case physical_type::NA:
                return std::string("NULL");
            case physical_type::BOOL:
                return std::to_string(static_cast<unsigned int>(value.get_value<bool>(0)));
            case physical_type::UINT8:
                return std::to_string(static_cast<unsigned int>(value.get_value<uint8_t>(0)));
            case physical_type::INT8:
                return std::to_string(static_cast<int>(value.get_value<int8_t>(0)));
            case physical_type::UINT16:
                return std::to_string(static_cast<unsigned int>(value.get_value<uint16_t>(0)));
            case physical_type::INT16:
                return std::to_string(value.get_value<int16_t>(0));
            case physical_type::UINT32:
                return std::to_string(value.get_value<uint32_t>(0));
            case physical_type::INT32:
                return std::to_string(value.get_value<int32_t>(0));
            case physical_type::UINT64:
                return std::to_string(value.get_value<uint64_t>(0));
            case physical_type::INT64:
                return std::to_string(value.get_value<int64_t>(0));
            case physical_type::FLOAT:
                return std::to_string(value.get_value<float>(0));
            case physical_type::DOUBLE:
                return std::to_string(value.get_value<double>(0));
            case physical_type::STRING:
                return std::string(value.get_value<std::string_view>(0));
            default:
                return core::error_t{core::error_code_t::conversion_failure,
                                     std::pmr::string{"cannot convert a value of type " +
                                                          components::catalog::format_type_be(value.type(), false) +
                                                          " to string",
                                                      value.resource()}};
        }
    }

    components::cursor::cell_location_t locate_row(const components::cursor::cursor_t& cursor, uint64_t row) {
        auto located = cursor.locate(row);
        if (located.has_error()) {
            throw std::runtime_error(std::string(located.error().what));
        }
        return located.value();
    }

    std::string parse_numeric_to_string(absl::int128 num) {
        std::string result;
        std::string sign;
        if (num < 0) {
            sign = "-";
            num = -num;
        }
        while (num > 0) {
            result += std::to_string(static_cast<int>(num % 10));
            num /= 10;
        }
        if (result.empty()) {
            result = "0";
        }
        std::reverse(result.begin(), result.end());
        return sign + result;
    }

}} // namespace otterbrix::util
