#pragma once

#include <chrono>

#include "durations.hpp"

namespace core::date {

    namespace detail {
        template<class Duration>
        using time_point = std::chrono::time_point<std::chrono::system_clock, Duration>; /// C++ 20

        template<class Duration>
        using timestamp = std::chrono::time_point<Duration>;
    } // namespace detail

    using timestamp_day = detail::timestamp<date::duration_day>;
    using timestamp_h = detail::timestamp<date::duration_h>;
    using timestamp_m = detail::timestamp<date::duration_m>;
    using timestamp_s = detail::timestamp<date::duration_s>;
    using timestamp_ms = detail::timestamp<date::duration_ms>;
    using timestamp_us = detail::timestamp<date::duration_us>;
    using timestamp_ns = detail::timestamp<date::duration_ns>;

    static_assert(sizeof(timestamp_day) == sizeof(typename timestamp_day::rep), "");
    static_assert(sizeof(timestamp_h) == sizeof(typename timestamp_h::rep), "");
    static_assert(sizeof(timestamp_m) == sizeof(typename timestamp_m::rep), "");
    static_assert(sizeof(timestamp_s) == sizeof(typename timestamp_s::rep), "");
    static_assert(sizeof(timestamp_ms) == sizeof(typename timestamp_ms::rep), "");
    static_assert(sizeof(timestamp_us) == sizeof(typename timestamp_us::rep), "");
    static_assert(sizeof(timestamp_ns) == sizeof(typename timestamp_ns::rep), "");

} // namespace core::date
