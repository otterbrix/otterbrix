#include "util.hpp"

#include <algorithm>
#include <string>

using namespace components::types;

namespace otterbrix {

    namespace util {

        std::string logical_value_to_string(const components::types::logical_value_t& value) {
            switch (value.type().to_physical_type()) {
                case physical_type::NA:
                    return "NULL";
                case physical_type::BOOL:
                    return std::to_string(static_cast<unsigned int>(value.value<bool>()));
                case physical_type::UINT8:
                    return std::to_string(static_cast<unsigned int>(value.value<uint8_t>()));
                case physical_type::INT8:
                    return std::to_string(static_cast<int>(value.value<int8_t>()));
                case physical_type::UINT16:
                    return std::to_string(static_cast<unsigned int>(value.value<uint16_t>()));
                case physical_type::INT16:
                    return std::to_string(value.value<int16_t>());
                case physical_type::UINT32:
                    return std::to_string(value.value<uint32_t>());
                case physical_type::INT32:
                    return std::to_string(value.value<int32_t>());
                case physical_type::UINT64:
                    return std::to_string(value.value<uint64_t>());
                case physical_type::INT64:
                    return std::to_string(value.value<int64_t>());
                case physical_type::FLOAT:
                    return std::to_string(value.value<float>());
                case physical_type::DOUBLE:
                    return std::to_string(value.value<double>());
                case physical_type::STRING:
                    return std::string(value.value<std::string_view>());
                default:
                    throw std::runtime_error("Util function could't convert logical_value_t to string");

            }
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


    } // namespace util
} // namespace otterbrix
