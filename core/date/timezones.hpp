#pragma once

#include "date_types.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace core::date {

    // Returns the UTC offset (seconds east of UTC) for the given timezone name.
    // Accepts lowercase IANA names ("america/new_york"), fixed-offset forms
    // ("utc+05:30", "+05:30"), and the aliases "utc" and "gmt".
    // Returns nullopt if the name is not recognised.
    std::optional<timezone_offset_t> timezone_to_offset(std::string_view name);

    // Formats a UTC offset as "UTC", "UTC+HH:MM", or "UTC-HH:MM".
    std::string format_timezone(timezone_offset_t offset);

} // namespace core::date
