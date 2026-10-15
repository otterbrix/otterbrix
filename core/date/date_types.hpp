#pragma once

#include <chrono>
#include <cmath>
#include <compare>
#include <cstdint>
#include <utility>

#include <absl/numeric/int128.h>

namespace core::date {

    // Fixed offset a.k.a epoch (2000-01-01).
    inline constexpr std::chrono::sys_days pg_epoch{std::chrono::year{2000} / std::chrono::January / 1};

    using days = std::chrono::duration<int32_t, std::ratio<86400>>;
    using seconds_i32 = std::chrono::duration<int32_t, std::ratio<1>>;
    using months = std::chrono::duration<int32_t, std::ratio<2629746>>;
    using microseconds = std::chrono::duration<int64_t, std::micro>;
    using timezone_offset_t = seconds_i32;

    static_assert(sizeof(days) == 4);
    static_assert(sizeof(seconds_i32) == 4);
    static_assert(sizeof(months) == 4);
    static_assert(sizeof(microseconds) == 8);

    // Days since epoch (2000-01-01).
    struct date_t {
        days value{};
    };

    static_assert(sizeof(date_t) == 4);

    // Microseconds since midnight.
    struct time_t {
        microseconds value{};
    };

    static_assert(sizeof(time_t) == 8);

    // zone: seconds east of UTC (UTC+5 => zone = +18000, UTC-5 => zone = -18000).
    struct timetz_t {
        microseconds time{};
        timezone_offset_t zone{};
    };

    static_assert(sizeof(timetz_t) == 16);

    // Microseconds since epoch (2000-01-01 00:00:00).
    struct timestamp_t {
        microseconds value{};
    };

    static_assert(sizeof(timestamp_t) == 8);

    // Same storage as timestamp_t but always interpreted as UTC.
    struct timestamptz_t {
        microseconds value{};
    };

    static_assert(sizeof(timestamptz_t) == 8);

    // Layout: time (µs), day, month.
    struct interval_t {
        microseconds time{};
        days day{};
        months month{};
    };

    static_assert(sizeof(interval_t) == 16);

    inline std::chrono::sys_days to_sys_days(date_t d) noexcept {
        return pg_epoch + std::chrono::days{d.value.count()};
    }

    inline date_t from_sys_days(std::chrono::sys_days sd) noexcept {
        return {days{static_cast<int32_t>((sd - pg_epoch).count())}};
    }

    // Splits a PG timestamp into (whole days, sub-day microseconds).
    // Uses floor division so the time-of-day component is always non-negative.
    inline std::pair<days, microseconds> split_timestamp(microseconds us) noexcept {
        auto d = std::chrono::duration_cast<days>(us);
        auto tod = us - std::chrono::duration_cast<microseconds>(d);
        if (tod.count() < 0) {
            d -= days{1};
            tod += std::chrono::duration_cast<microseconds>(days{1});
        }
        return {d, tod};
    }

    // Applies the month component of an interval to a sys_days value using the
    // Gregorian calendar, clamping the day to the last valid day of the resulting
    // month (PostgreSQL behaviour: Jan 31 + 1 month = Feb 28/29).
    inline std::chrono::sys_days apply_months(std::chrono::sys_days sd, int32_t month_count) noexcept {
        auto ymd = std::chrono::year_month_day{sd} + std::chrono::months{month_count};
        if (!ymd.ok())
            ymd = ymd.year() / ymd.month() / std::chrono::last;
        return std::chrono::sys_days{ymd};
    }

    // Converts a sys_days back to PG microseconds and adds a sub-day offset.
    inline microseconds from_sys_days_us(std::chrono::sys_days sd, microseconds tod) noexcept {
        return std::chrono::duration_cast<microseconds>(sd - pg_epoch) + tod;
    }

    // --- Comparison operators ---
    // Semantics follow PostgreSQL: timetz comparisons are UTC-normalised;
    // interval comparisons use total approximate microseconds (30 days/month),
    // so interval '1 month' == interval '30 days' is true, matching PostgreSQL.

    inline auto operator<=>(date_t a, date_t b) noexcept { return a.value <=> b.value; }
    inline bool operator==(date_t a, date_t b) noexcept { return a.value == b.value; }

    inline auto operator<=>(time_t a, time_t b) noexcept { return a.value <=> b.value; }
    inline bool operator==(time_t a, time_t b) noexcept { return a.value == b.value; }

    inline auto operator<=>(timestamp_t a, timestamp_t b) noexcept { return a.value <=> b.value; }
    inline bool operator==(timestamp_t a, timestamp_t b) noexcept { return a.value == b.value; }

    inline auto operator<=>(timestamptz_t a, timestamptz_t b) noexcept { return a.value <=> b.value; }
    inline bool operator==(timestamptz_t a, timestamptz_t b) noexcept { return a.value == b.value; }

    // timetz, as PostgreSQL's timetz_cmp_internal: by the UTC instant (utc = local - zone_east), and for the same
    // instant by zone. PostgreSQL stores the zone seconds WEST of UTC and sorts by that, so here, with the zone
    // east, the larger zone comes first. Equal only when both the time and the zone are equal.
    inline std::strong_ordering operator<=>(timetz_t a, timetz_t b) noexcept {
        auto a_utc = a.time - std::chrono::duration_cast<microseconds>(a.zone);
        auto b_utc = b.time - std::chrono::duration_cast<microseconds>(b.zone);
        if (auto by_instant = a_utc <=> b_utc; by_instant != 0) {
            return by_instant;
        }
        return b.zone <=> a.zone;
    }
    inline bool operator==(timetz_t a, timetz_t b) noexcept { return a.time == b.time && a.zone == b.zone; }

    // interval, as PostgreSQL's interval_cmp_value: a month is 30 days and a day is 24 hours, in microseconds.
    // 128 bits, since a month count near the int32 limit overflows 64 bits of microseconds.
    inline absl::int128 interval_total(const interval_t& i) noexcept {
        constexpr int64_t microseconds_per_day = 86'400'000'000;
        return absl::int128{i.time.count()} + absl::int128{i.day.count()} * microseconds_per_day +
               absl::int128{i.month.count()} * 30 * microseconds_per_day;
    }
    inline auto operator<=>(const interval_t& a, const interval_t& b) noexcept {
        return interval_total(a) <=> interval_total(b);
    }
    inline bool operator==(const interval_t& a, const interval_t& b) noexcept {
        return interval_total(a) == interval_total(b);
    }

    inline interval_t operator*(interval_t iv, double f) noexcept {
        return {microseconds{std::llround(static_cast<double>(iv.time.count()) * f)},
                days{static_cast<int32_t>(std::llround(static_cast<double>(iv.day.count()) * f))},
                months{static_cast<int32_t>(std::llround(static_cast<double>(iv.month.count()) * f))}};
    }

} // namespace core::date
