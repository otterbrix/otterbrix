#include "util.hpp"

using namespace components::types;
using namespace components::document;

namespace otterbrix {


    namespace util {
        string DocumentValueToString(const components::document::value_t& value) {
            switch (value.physical_type()) {
                case physical_type::NA:
                    return "NULL";
                case physical_type::BOOL:
                    return to_string(value.as_bool());
                case physical_type::UINT8:
                    return to_string(value.as_unsigned());
                case physical_type::INT8:
                    return to_string(value.as_int());
                case physical_type::UINT16:
                    return to_string(value.as_unsigned());
                case physical_type::INT16:
                    return to_string(value.as_int());
                case physical_type::UINT32:
                    return to_string(value.as_unsigned());
                case physical_type::INT32:
                    return to_string(value.as_int());
                case physical_type::UINT64:
                    return to_string(value.as_unsigned());
                case physical_type::INT64:
                    return to_string(value.as_int());
                case physical_type::FLOAT: 
                    return to_string(value.as_float());
                case physical_type::DOUBLE:
                    return to_string(value.as_double());
                case physical_type::INT128: 
                case physical_type::UINT128: 
                    throw std::runtime_error("OtterBrix can\'t transform absl::uint128/abst::int128");
                case physical_type::STRING:
                    return string(value.as_string());
                default:
                    throw std::runtime_error("Util function could\'t convert document::value_t to string "+to_string(int(value.physical_type())));

            }
        }


        string LogicalValueToString(const components::types::logical_value_t& value) { 
            switch (value.type().to_physical_type()) {
                case physical_type::NA:
                    return "NULL";
                case physical_type::BOOL:
                    return to_string(value.value<bool>());
                case physical_type::UINT8:
                    return to_string(value.value<uint8_t>());
                case physical_type::INT8:
                    return to_string(value.value<int8_t>());
                case physical_type::UINT16:
                    return to_string(value.value<uint16_t>());
                case physical_type::INT16:
                    return to_string(value.value<int16_t>());
                case physical_type::UINT32:
                    return to_string(value.value<uint32_t>());
                case physical_type::INT32:
                    return to_string(value.value<int32_t>());
                case physical_type::UINT64:
                    return to_string(value.value<uint64_t>());
                case physical_type::INT64:
                    return to_string(value.value<int64_t>());
                case physical_type::FLOAT: 
                    return to_string(value.value<int64_t>());
                case physical_type::DOUBLE:
                    return to_string(value.value<float>());
                case physical_type::STRING:
                    return string(value.value<std::string_view>());
                default:
                    throw std::runtime_error("Util function could\'t convert logical_value_t to string");

            }
        }

        string ParseNumericToString(absl::int128 num) {
            string result;
            string sign;
            if (num < 0) {
                sign = "-";
                num = -num;
            }
            while (num > 0) {
                result += to_string(static_cast<int>(num % 10));
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
