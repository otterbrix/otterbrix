#pragma once

#include "date_types.hpp"

#include <string>
#include <string_view>

namespace core::date {

    // A lowercase fixed-offset form: "utc", "gmt", "utc±HH[:MM]", "gmt±HH[:MM]", "±HH[:MM]", as seconds east of
    // UTC; false for any other text.
    bool fixed_offset_of(std::string_view name, timezone_offset_t* offset);

    // A lowercase abbreviation ("cest", "msk"): the one offset it stands for; false when it is none.
    bool abbreviation_offset(std::string_view name, timezone_offset_t* offset);

    // Formats a UTC offset as "UTC", "UTC+HH:MM", or "UTC-HH:MM".
    std::string format_timezone(timezone_offset_t offset);

} // namespace core::date
