#pragma once

#include "date_types.hpp"

#include <charconv>
#include <string>

namespace core::date {

    // Renders the core date/time types back to their canonical ISO 8601 /
    // PostgreSQL textual form -- the inverse of date_parse.hpp. Kept here (not in the
    // cast layer) so every consumer shares one formatter, mirroring parse_*.

    namespace detail {

        // Appends `value` (non-negative) in base 10, zero-padded to at least
        // `min_width` digits.
        inline void append_uint(std::string& out, uint64_t value, int min_width) {
            char buffer[20];
            auto [end, error] = std::to_chars(buffer, buffer + sizeof(buffer), value);
            (void) error; // buffer is large enough for any uint64_t
            const int digits = static_cast<int>(end - buffer);
            for (int pad = digits; pad < min_width; ++pad) {
                out.push_back('0');
            }
            out.append(buffer, end);
        }

        // Appends the calendar date of a sys_days as YYYY-MM-DD (year padded to at
        // least 4 digits, month/day to 2). A year before 1 (BC) gets a leading '-'.
        inline void append_date(std::string& out, std::chrono::sys_days days_value) {
            const std::chrono::year_month_day ymd{days_value};
            int year = static_cast<int>(ymd.year());
            if (year < 0) {
                out.push_back('-');
                year = -year;
            }
            append_uint(out, static_cast<uint64_t>(year), 4);
            out.push_back('-');
            append_uint(out, static_cast<unsigned>(ymd.month()), 2);
            out.push_back('-');
            append_uint(out, static_cast<unsigned>(ymd.day()), 2);
        }

        // Appends a sub-day time as HH:MM:SS[.ffffff]. The fractional part is shown
        // only when non-zero, with trailing zeros trimmed (PostgreSQL style).
        inline void append_time(std::string& out, microseconds time_of_day) {
            int64_t remaining = time_of_day.count();
            const int64_t hours = remaining / 3'600'000'000;
            remaining %= 3'600'000'000;
            const int64_t minutes = remaining / 60'000'000;
            remaining %= 60'000'000;
            const int64_t seconds = remaining / 1'000'000;
            int64_t fraction = remaining % 1'000'000;
            append_uint(out, static_cast<uint64_t>(hours), 2);
            out.push_back(':');
            append_uint(out, static_cast<uint64_t>(minutes), 2);
            out.push_back(':');
            append_uint(out, static_cast<uint64_t>(seconds), 2);
            if (fraction != 0) {
                out.push_back('.');
                // Six digits, then drop trailing zeros.
                char digits[6];
                for (int index = 5; index >= 0; --index) {
                    digits[index] = static_cast<char>('0' + fraction % 10);
                    fraction /= 10;
                }
                int length = 6;
                while (length > 1 && digits[length - 1] == '0') {
                    --length;
                }
                out.append(digits, static_cast<size_t>(length));
            }
        }

        // Appends a UTC offset as +HH, -HH, or +HH:MM (minutes shown only when
        // non-zero) -- the form parse_tz_offset accepts.
        inline void append_zone(std::string& out, timezone_offset_t zone) {
            int32_t seconds = zone.count();
            out.push_back(seconds < 0 ? '-' : '+');
            seconds = seconds < 0 ? -seconds : seconds;
            append_uint(out, static_cast<uint64_t>(seconds / 3600), 2);
            const int32_t minutes = (seconds % 3600) / 60;
            if (minutes != 0) {
                out.push_back(':');
                append_uint(out, static_cast<uint64_t>(minutes), 2);
            }
        }

    } // namespace detail

    inline std::string to_string(date_t value) {
        std::string out;
        detail::append_date(out, to_sys_days(value));
        return out;
    }

    inline std::string to_string(time_t value) {
        std::string out;
        detail::append_time(out, value.value);
        return out;
    }

    inline std::string to_string(timetz_t value) {
        std::string out;
        detail::append_time(out, value.time);
        detail::append_zone(out, value.zone);
        return out;
    }

    inline std::string to_string(timestamp_t value) {
        std::string out;
        const auto [day_part, time_part] = split_timestamp(value.value);
        detail::append_date(out, pg_epoch + std::chrono::days{day_part.count()});
        out.push_back(' ');
        detail::append_time(out, time_part);
        return out;
    }

    // timestamptz is stored as UTC; it renders in UTC with a '+00' suffix. Shifting
    // into a session zone for display is a caller concern (the offset is not part of
    // the stored value).
    inline std::string to_string(timestamptz_t value) {
        std::string out;
        const auto [day_part, time_part] = split_timestamp(value.value);
        detail::append_date(out, pg_epoch + std::chrono::days{day_part.count()});
        out.push_back(' ');
        detail::append_time(out, time_part);
        detail::append_zone(out, timezone_offset_t{0});
        return out;
    }

    // PostgreSQL-style interval: "<years> years <mons> mons <days> days HH:MM:SS".
    // Zero calendar components are omitted; the clock part is shown when non-zero, or
    // when the whole interval is zero (so an empty interval prints "00:00:00").
    inline std::string to_string(interval_t value) {
        std::string out;
        const int32_t total_months = value.month.count();
        const int32_t years = total_months / 12;
        const int32_t remaining_months = total_months % 12;
        const int32_t day_count = value.day.count();
        auto append_component = [&](int32_t component, const char* unit) {
            if (component == 0) {
                return;
            }
            if (!out.empty()) {
                out.push_back(' ');
            }
            if (component < 0) {
                out.push_back('-');
                component = -component;
            }
            detail::append_uint(out, static_cast<uint64_t>(component), 1);
            out.push_back(' ');
            out.append(unit);
        };
        append_component(years, years == 1 || years == -1 ? "year" : "years");
        append_component(remaining_months, "mons");
        append_component(day_count, day_count == 1 || day_count == -1 ? "day" : "days");
        if (value.time.count() != 0 || out.empty()) {
            if (!out.empty()) {
                out.push_back(' ');
            }
            microseconds clock = value.time;
            if (clock.count() < 0) {
                out.push_back('-');
                clock = microseconds{-clock.count()};
            }
            detail::append_time(out, clock);
        }
        return out;
    }

} // namespace core::date