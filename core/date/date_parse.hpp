#pragma once

#include "date_types.hpp"
#include <charconv>
#include <optional>
#include <string_view>

namespace core::date {

    namespace detail {

        // Parse exactly 'width' decimal digits at s[pos], advance pos.
        // Rejects the string if fewer than width digits are present.
        inline bool parse_digits(std::string_view s, size_t& pos, int width, int& out) {
            if (pos + static_cast<size_t>(width) > s.size())
                return false;
            auto [ptr, ec] = std::from_chars(s.data() + pos, s.data() + pos + width, out);
            if (ec != std::errc{} || ptr != s.data() + pos + width)
                return false;
            pos += static_cast<size_t>(width);
            return true;
        }

        // Parse one or two decimal digits at s[pos], advance pos.
        inline bool parse_1or2(std::string_view s, size_t& pos, int& out) {
            if (pos >= s.size())
                return false;
            auto [ptr, ec] = std::from_chars(s.data() + pos, s.data() + s.size(), out);
            if (ec != std::errc{})
                return false;
            pos = static_cast<size_t>(ptr - s.data());
            return true;
        }

        // Parse an unsigned decimal integer of arbitrary length, advance pos.
        inline bool parse_uint(std::string_view s, size_t& pos, int& out) {
            if (pos >= s.size() || s[pos] < '0' || s[pos] > '9')
                return false;
            auto [ptr, ec] = std::from_chars(s.data() + pos, s.data() + s.size(), out);
            if (ec != std::errc{})
                return false;
            pos = static_cast<size_t>(ptr - s.data());
            return true;
        }

        inline bool expect(std::string_view s, size_t& pos, char c) {
            if (pos >= s.size() || s[pos] != c)
                return false;
            ++pos;
            return true;
        }

        // Parse optional fractional seconds '.ddddddd' → microseconds.
        inline microseconds parse_frac_us(std::string_view s, size_t& pos) {
            if (pos >= s.size() || s[pos] != '.')
                return microseconds{0};
            ++pos;
            size_t start = pos;
            while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') ++pos;
            size_t digits = pos - start;
            if (digits == 0)
                return microseconds{0};
            int64_t frac = 0;
            for (size_t i = start; i < pos; ++i) frac = frac * 10 + (s[i] - '0');
            int pad = 6 - static_cast<int>(digits);
            for (int i = 0; i < pad; ++i) frac *= 10;
            for (int i = pad; i < 0; ++i) frac /= 10;
            return microseconds{frac};
        }

        // Parse timezone offset '+HH:MM' or '-HH:MM' into seconds east of UTC.
        inline timezone_offset_t parse_tz_offset(std::string_view s, size_t& pos) {
            if (pos >= s.size() || (s[pos] != '+' && s[pos] != '-'))
                return seconds_i32{0};
            char sign_ch = s[pos++];
            int hh = 0, mm = 0;
            if (!parse_1or2(s, pos, hh))
                return seconds_i32{0};
            if (pos < s.size() && s[pos] == ':')
                ++pos;
            if (pos < s.size() && s[pos] >= '0' && s[pos] <= '9')
                parse_1or2(s, pos, mm);
            int32_t secs = hh * 3600 + mm * 60;
            return seconds_i32{sign_ch == '+' ? secs : -secs};
        }

        // Parse 'HH:MM:SS[.ffffff]' into microseconds from midnight.
        inline std::optional<microseconds> parse_time_us(std::string_view s, size_t& pos) {
            int hh = 0, mm = 0, ss = 0;
            if (!parse_1or2(s, pos, hh))
                return std::nullopt;
            if (!expect(s, pos, ':'))
                return std::nullopt;
            if (!parse_1or2(s, pos, mm))
                return std::nullopt;
            if (!expect(s, pos, ':'))
                return std::nullopt;
            if (!parse_1or2(s, pos, ss))
                return std::nullopt;
            auto frac = parse_frac_us(s, pos);
            int64_t total = (int64_t(hh) * 3600 + int64_t(mm) * 60 + int64_t(ss)) * 1'000'000 + frac.count();
            return microseconds{total};
        }

        // Try to match keyword kw at s[pos] (case-sensitive), advance pos.
        // Trailing 's' is consumed if present.
        inline bool try_keyword(std::string_view s, size_t& pos, std::string_view kw) {
            if (s.size() - pos < kw.size())
                return false;
            if (s.substr(pos, kw.size()) != kw)
                return false;
            pos += kw.size();
            if (pos < s.size() && s[pos] == 's')
                ++pos;
            return true;
        }

    } // namespace detail

    // --- Public parse functions ---
    // All functions accept only ISO 8601 / PostgreSQL standard formats.
    // The year field is required to be exactly 4 digits, which makes
    // ambiguous formats like DD-MM-YYYY and MM-DD-YYYY fail immediately.

    // DATE: 'YYYY-MM-DD'
    inline std::optional<date_t> parse_date(std::string_view s) {
        size_t pos = 0;
        int y = 0, m = 0, d = 0;
        if (!detail::parse_digits(s, pos, 4, y))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, m))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, d))
            return std::nullopt;
        auto ymd = std::chrono::year{y} / std::chrono::month{static_cast<unsigned>(m)} /
                   std::chrono::day{static_cast<unsigned>(d)};
        if (!ymd.ok())
            return std::nullopt;
        return from_sys_days(std::chrono::sys_days{ymd});
    }

    // TIME: 'HH:MM:SS[.ffffff]'
    inline std::optional<time_t> parse_time(std::string_view s) {
        size_t pos = 0;
        auto us = detail::parse_time_us(s, pos);
        if (!us)
            return std::nullopt;
        return time_t{*us};
    }

    // TIMETZ: 'HH:MM:SS[.ffffff][+HH:MM|-HH:MM]'
    inline std::optional<timetz_t> parse_timetz(std::string_view s) {
        size_t pos = 0;
        auto us = detail::parse_time_us(s, pos);
        if (!us)
            return std::nullopt;
        auto zone = detail::parse_tz_offset(s, pos);
        return timetz_t{*us, zone};
    }

    // TIMESTAMP: 'YYYY-MM-DD[T ]HH:MM:SS[.ffffff]'
    inline std::optional<timestamp_t> parse_timestamp(std::string_view s) {
        size_t pos = 0;
        int y = 0, mo = 0, d = 0;
        if (!detail::parse_digits(s, pos, 4, y))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, mo))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, d))
            return std::nullopt;
        if (pos >= s.size() || (s[pos] != ' ' && s[pos] != 'T'))
            return std::nullopt;
        ++pos;
        auto time_us = detail::parse_time_us(s, pos);
        if (!time_us)
            return std::nullopt;
        auto ymd = std::chrono::year{y} / std::chrono::month{static_cast<unsigned>(mo)} /
                   std::chrono::day{static_cast<unsigned>(d)};
        if (!ymd.ok())
            return std::nullopt;
        auto date_us = std::chrono::duration_cast<microseconds>(std::chrono::sys_days{ymd} - pg_epoch);
        return timestamp_t{date_us + *time_us};
    }

    // TIMESTAMP WITH TIME ZONE: 'YYYY-MM-DD[T ]HH:MM:SS[.ffffff][+HH:MM|-HH:MM]'
    // Stored as UTC: utc = local - zone_east
    inline std::optional<timestamptz_t> parse_timestamptz(std::string_view s) {
        size_t pos = 0;
        int y = 0, mo = 0, d = 0;
        if (!detail::parse_digits(s, pos, 4, y))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, mo))
            return std::nullopt;
        if (!detail::expect(s, pos, '-'))
            return std::nullopt;
        if (!detail::parse_1or2(s, pos, d))
            return std::nullopt;
        if (pos >= s.size() || (s[pos] != ' ' && s[pos] != 'T'))
            return std::nullopt;
        ++pos;
        auto time_us = detail::parse_time_us(s, pos);
        if (!time_us)
            return std::nullopt;
        auto zone = detail::parse_tz_offset(s, pos);
        auto ymd = std::chrono::year{y} / std::chrono::month{static_cast<unsigned>(mo)} /
                   std::chrono::day{static_cast<unsigned>(d)};
        if (!ymd.ok())
            return std::nullopt;
        auto date_us = std::chrono::duration_cast<microseconds>(std::chrono::sys_days{ymd} - pg_epoch);
        auto local_us = date_us + *time_us;
        auto zone_us = std::chrono::duration_cast<microseconds>(zone);
        return timestamptz_t{local_us - zone_us};
    }

    // INTERVAL: 'N unit [N unit ...] [HH:MM:SS[.ffffff]]'
    // Supported units: year/yr, month/mon, week, day, hour, minute/min, second/sec
    // Examples: '2 years 3 months', '10 days 04:30:00', '1 hour 30 minutes'
    inline std::optional<interval_t> parse_interval(std::string_view s) {
        int32_t total_months = 0, total_days = 0;
        microseconds total_time{0};
        size_t pos = 0;
        bool parsed_any = false;

        while (pos < s.size()) {
            while (pos < s.size() && s[pos] == ' ') ++pos;
            if (pos >= s.size())
                break;

            if (s[pos] < '0' || s[pos] > '9')
                break;

            int val = 0;
            size_t save = pos;
            if (!detail::parse_uint(s, pos, val))
                break;

            while (pos < s.size() && s[pos] == ' ') ++pos;

            if (pos < s.size() && s[pos] == ':') {
                pos = save;
                auto tu = detail::parse_time_us(s, pos);
                if (!tu)
                    return std::nullopt;
                total_time += *tu;
                parsed_any = true;
                break;
            }

            if (detail::try_keyword(s, pos, "year") || detail::try_keyword(s, pos, "yr")) {
                total_months += val * 12;
            } else if (detail::try_keyword(s, pos, "month") || detail::try_keyword(s, pos, "mon")) {
                total_months += val;
            } else if (detail::try_keyword(s, pos, "week")) {
                total_days += val * 7;
            } else if (detail::try_keyword(s, pos, "day")) {
                total_days += val;
            } else if (detail::try_keyword(s, pos, "hour")) {
                total_time += microseconds{int64_t(val) * 3600 * 1'000'000};
            } else if (detail::try_keyword(s, pos, "minute") || detail::try_keyword(s, pos, "min")) {
                total_time += microseconds{int64_t(val) * 60 * 1'000'000};
            } else if (detail::try_keyword(s, pos, "second") || detail::try_keyword(s, pos, "sec")) {
                total_time += microseconds{int64_t(val) * 1'000'000};
                total_time += detail::parse_frac_us(s, pos);
            } else {
                return std::nullopt;
            }
            parsed_any = true;
        }

        if (!parsed_any)
            return std::nullopt;
        return interval_t{total_time, days{total_days}, months{total_months}};
    }

} // namespace core::date
