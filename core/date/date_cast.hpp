#pragma once

#include "date_types.hpp"

#include <type_traits>

namespace core::date {

    namespace detail {

        template<typename>
        inline constexpr bool always_false = false;

        // Primary template — fires a compile-time error for unsupported pairs.
        template<typename To, typename From>
        struct date_time_cast_impl {
            static_assert(always_false<From>, "unsupported date/time cast combination");
        };

        // ── Identity ─────────────────────────────────────────────────────────
        template<typename T>
        struct date_time_cast_impl<T, T> {
            static T cast(T from, [[maybe_unused]] seconds_i32 session_tz) { return from; }
        };

        // ── date_t → timestamp_t : midnight on that date ──────────────────
        template<>
        struct date_time_cast_impl<timestamp_t, date_t> {
            static timestamp_t cast(date_t from, [[maybe_unused]] seconds_i32 session_tz) {
                return timestamp_t{std::chrono::duration_cast<microseconds>(from.value)};
            }
        };

        // ── date_t → timestamptz_t : midnight UTC on that date ────────────
        template<>
        struct date_time_cast_impl<timestamptz_t, date_t> {
            static timestamptz_t cast(date_t from, [[maybe_unused]] seconds_i32 session_tz) {
                return timestamptz_t{std::chrono::duration_cast<microseconds>(from.value)};
            }
        };

        // ── timestamp_t → date_t : extract local date ─────────────────────
        template<>
        struct date_time_cast_impl<date_t, timestamp_t> {
            static date_t cast(timestamp_t from, [[maybe_unused]] seconds_i32 session_tz) {
                auto [day_part, time_part] = split_timestamp(from.value);
                return date_t{day_part};
            }
        };

        // ── timestamp_t → time_t : extract local time-of-day ─────────────
        template<>
        struct date_time_cast_impl<time_t, timestamp_t> {
            static time_t cast(timestamp_t from, [[maybe_unused]] seconds_i32 session_tz) {
                auto [day_part, time_part] = split_timestamp(from.value);
                return time_t{time_part};
            }
        };

        // ── timestamp_t → timestamptz_t : local → UTC ────────────────────
        // UTC = local − session_tz
        template<>
        struct date_time_cast_impl<timestamptz_t, timestamp_t> {
            static timestamptz_t cast(timestamp_t from, seconds_i32 session_tz) {
                auto offset = std::chrono::duration_cast<microseconds>(session_tz);
                return timestamptz_t{from.value - offset};
            }
        };

        // ── timestamptz_t → timestamp_t : UTC → local ────────────────────
        // local = UTC + session_tz
        template<>
        struct date_time_cast_impl<timestamp_t, timestamptz_t> {
            static timestamp_t cast(timestamptz_t from, seconds_i32 session_tz) {
                auto offset = std::chrono::duration_cast<microseconds>(session_tz);
                return timestamp_t{from.value + offset};
            }
        };

        // ── timestamptz_t → date_t : session-local date ───────────────────
        template<>
        struct date_time_cast_impl<date_t, timestamptz_t> {
            static date_t cast(timestamptz_t from, seconds_i32 session_tz) {
                auto offset = std::chrono::duration_cast<microseconds>(session_tz);
                auto local_us = from.value + offset;
                auto [day_part, time_part] = split_timestamp(local_us);
                return date_t{day_part};
            }
        };

        // ── timestamptz_t → time_t : session-local time-of-day ───────────
        template<>
        struct date_time_cast_impl<time_t, timestamptz_t> {
            static time_t cast(timestamptz_t from, seconds_i32 session_tz) {
                auto offset = std::chrono::duration_cast<microseconds>(session_tz);
                auto local_us = from.value + offset;
                auto [day_part, time_part] = split_timestamp(local_us);
                return time_t{time_part};
            }
        };

        // ── timestamptz_t → timetz_t : session-local time with zone ───────
        template<>
        struct date_time_cast_impl<timetz_t, timestamptz_t> {
            static timetz_t cast(timestamptz_t from, seconds_i32 session_tz) {
                auto offset = std::chrono::duration_cast<microseconds>(session_tz);
                auto local_us = from.value + offset;
                auto [day_part, time_part] = split_timestamp(local_us);
                return timetz_t{time_part, session_tz};
            }
        };

        // ── time_t → timetz_t : attach session zone ───────────────────────
        template<>
        struct date_time_cast_impl<timetz_t, time_t> {
            static timetz_t cast(time_t from, seconds_i32 session_tz) { return timetz_t{from.value, session_tz}; }
        };

        // ── timetz_t → time_t : drop zone, keep local time ───────────────
        template<>
        struct date_time_cast_impl<time_t, timetz_t> {
            static time_t cast(timetz_t from, [[maybe_unused]] seconds_i32 session_tz) { return time_t{from.time}; }
        };

    } // namespace detail

    // Public interface.
    // session_tz: UTC offset in seconds east of UTC (catalog/session timezone).
    template<typename To, typename From>
    To convert_date_time(From from, seconds_i32 session_tz) {
        return detail::date_time_cast_impl<To, From>::cast(from, session_tz);
    }

} // namespace core::date
