#pragma once

#include <common/typedefs.hpp>
#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <absl/numeric/int128.h>
#include <cstdint>
#include <string>

namespace otterbrix { namespace util {
    std::string logical_value_to_string(const components::types::logical_value_t& value);

    // DB-API rowcount: the rows an INSERT / UPDATE / DELETE wrote, else the rows of the result.
    int64_t rowcount_of(const components::cursor::cursor_t& cursor);

    template<class T>
    T parse_to_numeric(const std::string& numeric_string) {
        bool is_neg = false;
        idx_t i = 0;
        T res = 0;
        if (numeric_string.length() > 0 && numeric_string[0] == '-') {
            is_neg = true;
            i++;
        }
        if (numeric_string.length() > 0 && numeric_string[0] == '+') {
            i++;
        }
        for (; i < numeric_string.length(); i++) {
            res *= 10;
            res += (numeric_string[i] - '0');
        }
        if (is_neg) {
            return -res;
        }
        return res;
    }

    std::string parse_numeric_to_string(absl::int128 num);
}} // namespace otterbrix::util
