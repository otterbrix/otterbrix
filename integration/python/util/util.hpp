#pragma once

#include <common/typedefs.hpp>
#include <components/cursor/cursor.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <absl/numeric/int128.h>
#include <cstdint>
#include <string>

namespace otterbrix { namespace util {
    // The one-row constant as the text of its value; a type with no such text is an error.
    core::result_wrapper_t<std::string> vector_to_string(const components::vector::vector_t& value);

    // Where a global result row lives; a row past the result is a RuntimeError at the python boundary.
    components::cursor::cell_location_t locate_row(const components::cursor::cursor_t& cursor, uint64_t row);

    // DB-API rowcount: the rows an INSERT / UPDATE / DELETE wrote, else the rows of the result.
    int64_t rowcount_of(const components::cursor::cursor_t& cursor);

    // The pg_class oid of the table a CREATE TABLE created, else None.
    py::object created_oid_of(const components::cursor::cursor_t& cursor);

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
