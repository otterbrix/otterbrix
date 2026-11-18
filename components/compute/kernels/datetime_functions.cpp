#include "../function.hpp"
#include "../kernel_utils.hpp"

#include <core/date/date_to_string.hpp>
#include <core/date/date_types.hpp>
#include <core/date/time_zone.hpp>

#include <absl/time/civil_time.h>

#include <cassert>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    constexpr int64_t micros_per_second = 1'000'000;
    // 2000-01-01 00:00:00 UTC, the epoch timestamp_t / timestamptz_t count from, in unix seconds.
    constexpr int64_t pg_epoch_unix_seconds = 946'684'800;
    // MySQL 8.4 CONVERT_TZ: an offset is [+|-]HH:MM in [-13:59, +14:00].
    constexpr int32_t min_offset_seconds = -(13 * 3600 + 59 * 60);
    constexpr int32_t max_offset_seconds = 14 * 3600;
    // A value whose UTC instant is outside TIMESTAMP's range, [1970-01-01 00:00:01, 2038-01-19 03:14:07], is not
    // converted (MySQL 8.4 CONVERT_TZ).
    constexpr int64_t first_converted_second = 1;
    constexpr int64_t last_converted_second = 2'147'483'647;

    bool parse_two_digits(std::string_view digits, int32_t* value) {
        if (digits.empty() || digits.size() > 2) {
            return false;
        }
        int32_t parsed = 0;
        for (const char digit : digits) {
            if (digit < '0' || digit > '9') {
                return false;
            }
            parsed = parsed * 10 + (digit - '0');
        }
        *value = parsed;
        return true;
    }

    // [+|-]H[H]:MM, as seconds east of UTC.
    bool parse_offset(std::string_view text, int32_t* seconds) {
        if (text.empty() || (text.front() != '+' && text.front() != '-')) {
            return false;
        }
        const auto colon = text.find(':');
        if (colon == std::string_view::npos || text.size() - colon != 3) {
            return false;
        }
        int32_t hours = 0;
        int32_t minutes = 0;
        if (!parse_two_digits(text.substr(1, colon - 1), &hours) ||
            !parse_two_digits(text.substr(colon + 1), &minutes) || minutes > 59) {
            return false;
        }
        const int32_t magnitude = hours * 3600 + minutes * 60;
        const int32_t offset = text.front() == '-' ? -magnitude : magnitude;
        if (offset < min_offset_seconds || offset > max_offset_seconds) {
            return false;
        }
        *seconds = offset;
        return true;
    }

    // An offset, or a zone of the IANA database; false for any other text, as MySQL answers NULL for it.
    bool load_zone(std::string_view name, core::date::time_zone_t* zone) {
        int32_t offset = 0;
        if (parse_offset(name, &offset)) {
            *zone = core::date::time_zone_t::fixed(core::date::timezone_offset_t{offset});
            return true;
        }
        return core::date::named_time_zone(name, zone);
    }

    // The zone a row names, loaded once for a run of rows naming the same one.
    struct zone_cache_t {
        std::string name;
        core::date::time_zone_t zone;
        bool loaded{false};
        bool valid{false};

        bool load(std::string_view requested) {
            if (!loaded || requested != name) {
                name.assign(requested);
                valid = load_zone(requested, &zone);
                loaded = true;
            }
            return valid;
        }
    };

    int64_t unix_seconds_of(core::date::timestamptz_t instant) noexcept {
        int64_t seconds = instant.value.count() / micros_per_second;
        if (instant.value.count() % micros_per_second < 0) {
            seconds -= 1;
        }
        return seconds + pg_epoch_unix_seconds;
    }

    core::date::timestamp_t convert_wall_time(core::date::timestamp_t value,
                                              const core::date::time_zone_t& from,
                                              const core::date::time_zone_t& to) {
        const auto instant = from.to_utc(value);
        const int64_t unix_seconds = unix_seconds_of(instant);
        if (unix_seconds < first_converted_second || unix_seconds > last_converted_second) {
            return value;
        }
        return to.to_local(instant);
    }

    // CONVERT_TZ(dt, from_tz, to_tz): the wall time `dt` of from_tz as the wall time of to_tz; NULL when an argument
    // is NULL or names no zone.
    core::error_t vector_convert_tz(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& values = inputs.data[0];
        const auto& from_names = inputs.data[1];
        const auto& to_names = inputs.data[2];
        const auto* source = values.data<core::date::timestamp_t>();
        const auto* from_source = from_names.data<std::string_view>();
        const auto* to_source = to_names.data<std::string_view>();
        auto* destination = output.data<core::date::timestamp_t>();
        const bool all_valid = all_inputs_valid(inputs);
        zone_cache_t from;
        zone_cache_t to;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (values.is_null(row) || from_names.is_null(row) || to_names.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            if (!from.load(from_source[row]) || !to.load(to_source[row])) {
                output.set_null(row, true);
                continue;
            }
            destination[row] = convert_wall_time(source[row], from.zone, to.zone);
        }
        return core::error_t::no_error();
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_convert_tz_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "Converts a timestamp from one time zone to another",
                         "convert_tz(timestamp, from_tz text, to_tz text) -> timestamp; a zone is an IANA name or "
                         "[+|-]HH:MM in [-13:59, +14:00]; NULL when an argument is NULL or names no zone",
                         {"dt", "from_tz", "to_tz"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "convert_tz",
                                                                      arity::ternary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::immutable);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::TIMESTAMP),
                                parameter_type::exact(logical_type::TEXT),
                                parameter_type::exact(logical_type::TEXT)},
                               {output_type::fixed(logical_type::TIMESTAMP)});
        vector_kernel k(std::move(sig), vector_convert_tz);
        [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(k));
        assert(!added.contains_error() && "datetime kernel must fit the declared slots and arity");
        return fn;
    }

    // ── PostgreSQL 18 timestamp.c: timezone(), date_trunc(), date_part() ─────────────────────────────────────

    constexpr std::string_view timestamp_name = "timestamp without time zone";
    constexpr std::string_view timestamptz_name = "timestamp with time zone";
    constexpr std::string_view time_name = "time without time zone";
    constexpr std::string_view interval_name = "interval";

    const absl::CivilSecond civil_epoch{2000, 1, 1, 0, 0, 0};
    constexpr int64_t micros_per_minute = 60 * micros_per_second;
    constexpr int64_t micros_per_hour = 60 * micros_per_minute;
    constexpr int64_t seconds_per_day = 86'400;
    // Julian day number of 2000-01-01.
    constexpr int64_t julian_epoch_day = 2'451'545;

    std::string lowered(std::string_view text) {
        std::string out(text);
        for (auto& symbol : out) {
            symbol = static_cast<char>(std::tolower(static_cast<unsigned char>(symbol)));
        }
        return out;
    }

    // The units of PostgreSQL 18 deltatktbl / datetktbl that date_trunc and date_part read.
    enum class unit_t : uint8_t
    {
        invalid,
        microseconds,
        milliseconds,
        second,
        minute,
        hour,
        day,
        week,
        month,
        quarter,
        year,
        decade,
        century,
        millennium,
        epoch,
        dow,
        isodow,
        doy,
        isoyear,
        julian,
        timezone,
        timezone_hour,
        timezone_minute
    };

    struct unit_name_t {
        std::string_view name;
        unit_t unit;
    };

    constexpr unit_name_t unit_names[] = {{"microsecond", unit_t::microseconds},
                                          {"microseconds", unit_t::microseconds},
                                          {"microsecon", unit_t::microseconds},
                                          {"us", unit_t::microseconds},
                                          {"usec", unit_t::microseconds},
                                          {"usecs", unit_t::microseconds},
                                          {"useconds", unit_t::microseconds},
                                          {"millisecond", unit_t::milliseconds},
                                          {"milliseconds", unit_t::milliseconds},
                                          {"millisecon", unit_t::milliseconds},
                                          {"ms", unit_t::milliseconds},
                                          {"msec", unit_t::milliseconds},
                                          {"msecs", unit_t::milliseconds},
                                          {"mseconds", unit_t::milliseconds},
                                          {"second", unit_t::second},
                                          {"seconds", unit_t::second},
                                          {"s", unit_t::second},
                                          {"sec", unit_t::second},
                                          {"secs", unit_t::second},
                                          {"minute", unit_t::minute},
                                          {"minutes", unit_t::minute},
                                          {"m", unit_t::minute},
                                          {"min", unit_t::minute},
                                          {"mins", unit_t::minute},
                                          {"hour", unit_t::hour},
                                          {"hours", unit_t::hour},
                                          {"h", unit_t::hour},
                                          {"hr", unit_t::hour},
                                          {"hrs", unit_t::hour},
                                          {"day", unit_t::day},
                                          {"days", unit_t::day},
                                          {"d", unit_t::day},
                                          {"week", unit_t::week},
                                          {"weeks", unit_t::week},
                                          {"w", unit_t::week},
                                          {"month", unit_t::month},
                                          {"months", unit_t::month},
                                          {"mon", unit_t::month},
                                          {"mons", unit_t::month},
                                          {"quarter", unit_t::quarter},
                                          {"qtr", unit_t::quarter},
                                          {"year", unit_t::year},
                                          {"years", unit_t::year},
                                          {"y", unit_t::year},
                                          {"yr", unit_t::year},
                                          {"yrs", unit_t::year},
                                          {"decade", unit_t::decade},
                                          {"decades", unit_t::decade},
                                          {"dec", unit_t::decade},
                                          {"decs", unit_t::decade},
                                          {"century", unit_t::century},
                                          {"centuries", unit_t::century},
                                          {"c", unit_t::century},
                                          {"cent", unit_t::century},
                                          {"millennium", unit_t::millennium},
                                          {"millennia", unit_t::millennium},
                                          {"mil", unit_t::millennium},
                                          {"mils", unit_t::millennium},
                                          {"epoch", unit_t::epoch},
                                          {"dow", unit_t::dow},
                                          {"isodow", unit_t::isodow},
                                          {"doy", unit_t::doy},
                                          {"isoyear", unit_t::isoyear},
                                          {"julian", unit_t::julian},
                                          {"j", unit_t::julian},
                                          {"timezone", unit_t::timezone},
                                          {"timezone_h", unit_t::timezone_hour},
                                          {"timezone_hour", unit_t::timezone_hour},
                                          {"timezone_m", unit_t::timezone_minute},
                                          {"timezone_minute", unit_t::timezone_minute}};

    unit_t unit_of(std::string_view text) {
        const auto name = lowered(text);
        for (const auto& entry : unit_names) {
            if (entry.name == name) {
                return entry.unit;
            }
        }
        return unit_t::invalid;
    }

    core::error_t
    unit_refusal(std::pmr::memory_resource* resource, std::string_view unit, std::string_view type, bool recognized) {
        std::pmr::string what{"unit \"", resource};
        what.append(unit);
        what.append(recognized ? "\" not supported for type " : "\" not recognized for type ");
        what.append(type);
        return core::error_t{recognized ? core::error_code_t::unimplemented_yet : core::error_code_t::invalid_parameter,
                             std::move(what)};
    }

    core::error_t zone_refusal(std::pmr::memory_resource* resource, std::string_view zone) {
        std::pmr::string what{"time zone \"", resource};
        what.append(zone);
        what.append("\" not recognized");
        return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
    }

    core::error_t no_settings(std::pmr::memory_resource* resource) {
        return core::error_t{
            core::error_code_t::internal_error,
            std::pmr::string{"a session-zone function ran without the statement's settings", resource}};
    }

    // The unit a run of rows names, read once.
    struct unit_cache_t {
        std::string name;
        unit_t unit{unit_t::invalid};
        bool loaded{false};

        unit_t load(std::string_view requested) {
            if (!loaded || requested != name) {
                name.assign(requested);
                unit = unit_of(requested);
                loaded = true;
            }
            return unit;
        }
    };

    // The zone timezone() / date_trunc(..., zone) names, read once for a run of rows naming the same one.
    struct conversion_zone_cache_t {
        std::string name;
        core::date::time_zone_t zone;
        bool loaded{false};
        bool valid{false};

        bool load(std::string_view requested) {
            if (!loaded || requested != name) {
                name.assign(requested);
                valid = core::date::conversion_time_zone(requested, &zone);
                loaded = true;
            }
            return valid;
        }
    };

    struct civil_t {
        absl::CivilSecond second;
        int64_t fraction;
    };

    civil_t civil_of(int64_t micros) {
        int64_t seconds = micros / micros_per_second;
        int64_t fraction = micros % micros_per_second;
        if (fraction < 0) {
            seconds -= 1;
            fraction += micros_per_second;
        }
        return {civil_epoch + seconds, fraction};
    }

    int64_t micros_of(const absl::CivilSecond& second, int64_t fraction) {
        return (second - civil_epoch) * micros_per_second + fraction;
    }

    // Monday 0 .. Sunday 6.
    int weekday_from_monday(absl::CivilDay day) {
        switch (absl::GetWeekday(day)) {
            case absl::Weekday::monday:
                return 0;
            case absl::Weekday::tuesday:
                return 1;
            case absl::Weekday::wednesday:
                return 2;
            case absl::Weekday::thursday:
                return 3;
            case absl::Weekday::friday:
                return 4;
            case absl::Weekday::saturday:
                return 5;
            case absl::Weekday::sunday:
                return 6;
        }
        return 0;
    }

    struct iso_week_t {
        int64_t year;
        int64_t week;
    };

    // The ISO 8601 week a day is in: the week of its Thursday.
    iso_week_t iso_week_of(absl::CivilDay day) {
        const absl::CivilDay thursday = day + (3 - weekday_from_monday(day));
        return {thursday.year(), (absl::GetYearDay(thursday) - 1) / 7 + 1};
    }

    bool is_truncation_unit(unit_t unit) {
        switch (unit) {
            case unit_t::microseconds:
            case unit_t::milliseconds:
            case unit_t::second:
            case unit_t::minute:
            case unit_t::hour:
            case unit_t::day:
            case unit_t::week:
            case unit_t::month:
            case unit_t::quarter:
            case unit_t::year:
            case unit_t::decade:
            case unit_t::century:
            case unit_t::millennium:
                return true;
            default:
                return false;
        }
    }

    // A unit whose truncation moves the wall time across days, so a zone's offset is read again (redotz).
    bool truncates_to_a_day(unit_t unit) {
        switch (unit) {
            case unit_t::day:
            case unit_t::week:
            case unit_t::month:
            case unit_t::quarter:
            case unit_t::year:
            case unit_t::decade:
            case unit_t::century:
            case unit_t::millennium:
                return true;
            default:
                return false;
        }
    }

    // timestamp_trunc on a wall time; `unit` is a truncation unit.
    int64_t truncate_wall_time(unit_t unit, int64_t micros) {
        auto [civil, fraction] = civil_of(micros);
        if (unit == unit_t::week) {
            const absl::CivilDay day{civil};
            return micros_of(absl::CivilSecond{day - weekday_from_monday(day)}, 0);
        }
        int64_t year = civil.year();
        int month = civil.month();
        int day = civil.day();
        int hour = civil.hour();
        int minute = civil.minute();
        int second = civil.second();
        switch (unit) {
            case unit_t::millennium:
                year = year > 0 ? ((year + 999) / 1000) * 1000 - 999 : -((999 - (year - 1)) / 1000) * 1000 + 1;
                [[fallthrough]];
            case unit_t::century:
                year = year > 0 ? ((year + 99) / 100) * 100 - 99 : -((99 - (year - 1)) / 100) * 100 + 1;
                [[fallthrough]];
            case unit_t::decade:
                if (unit == unit_t::decade) {
                    year = year > 0 ? (year / 10) * 10 : -((8 - (year - 1)) / 10) * 10;
                }
                [[fallthrough]];
            case unit_t::year:
                month = 1;
                [[fallthrough]];
            case unit_t::quarter:
                month = 3 * ((month - 1) / 3) + 1;
                [[fallthrough]];
            case unit_t::month:
                day = 1;
                [[fallthrough]];
            case unit_t::day:
                hour = 0;
                [[fallthrough]];
            case unit_t::hour:
                minute = 0;
                [[fallthrough]];
            case unit_t::minute:
                second = 0;
                [[fallthrough]];
            case unit_t::second:
                fraction = 0;
                break;
            case unit_t::milliseconds:
                fraction = (fraction / 1000) * 1000;
                break;
            default:
                break;
        }
        return micros_of(absl::CivilSecond{year, month, day, hour, minute, second}, fraction);
    }

    // timestamptz_trunc_internal: truncated as the zone's wall time; a day or more reads the zone's offset again.
    core::date::timestamptz_t
    truncate_instant(unit_t unit, core::date::timestamptz_t instant, const core::date::time_zone_t& zone) {
        const auto offset = std::chrono::duration_cast<core::date::microseconds>(zone.offset_at(instant));
        const int64_t truncated = truncate_wall_time(unit, (instant.value + offset).count());
        if (truncates_to_a_day(unit)) {
            return zone.to_utc(core::date::timestamp_t{core::date::microseconds{truncated}});
        }
        return core::date::timestamptz_t{core::date::microseconds{truncated} - offset};
    }

    // timezone(zone, value): a timestamp read as the zone's wall time is an instant, an instant is the zone's wall
    // time.
    template<bool from_timestamp>
    core::error_t vector_timezone_named(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& zones = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* zone_source = zones.data<std::string_view>();
        const bool all_valid = all_inputs_valid(inputs);
        conversion_zone_cache_t zone;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (zones.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            if (!zone.load(zone_source[row])) {
                return zone_refusal(ctx.exec_context().resource(), zone_source[row]);
            }
            if constexpr (from_timestamp) {
                output.data<core::date::timestamptz_t>()[row] =
                    zone.zone.to_utc(values.data<core::date::timestamp_t>()[row]);
            } else {
                output.data<core::date::timestamp_t>()[row] =
                    zone.zone.to_local(values.data<core::date::timestamptz_t>()[row]);
            }
        }
        return core::error_t::no_error();
    }

    // timezone(interval, value): the interval's time is the offset east of UTC; months or days are refused.
    template<bool from_timestamp>
    core::error_t vector_timezone_interval(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& zones = inputs.data[0];
        const auto& values = inputs.data[1];
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (zones.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto zone = zones.get_value<core::date::interval_t>(row);
            if (zone.month.count() != 0 || zone.day.count() != 0) {
                std::pmr::string what{"interval time zone \"", ctx.exec_context().resource()};
                what.append(core::date::to_string(zone));
                what.append("\" must not include months or days");
                return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
            }
            const core::date::microseconds offset{(zone.time.count() / micros_per_second) * micros_per_second};
            if constexpr (from_timestamp) {
                output.data<core::date::timestamptz_t>()[row] =
                    core::date::timestamptz_t{values.data<core::date::timestamp_t>()[row].value - offset};
            } else {
                output.data<core::date::timestamp_t>()[row] =
                    core::date::timestamp_t{values.data<core::date::timestamptz_t>()[row].value + offset};
            }
        }
        return core::error_t::no_error();
    }

    constexpr int64_t micros_per_day = seconds_per_day * micros_per_second;

    // timetz_zone / timetz_izone: the same instant's time of day at the offset `zone_offset`, wrapped into the day.
    core::date::timetz_t shift_timetz(core::date::timetz_t value, int64_t zone_offset) {
        int64_t time = value.time.count() + (zone_offset - int64_t{value.zone.count()}) * micros_per_second;
        time %= micros_per_day;
        if (time < 0) {
            time += micros_per_day;
        }
        return core::date::timetz_t{core::date::microseconds{time},
                                    core::date::timezone_offset_t{static_cast<int32_t>(zone_offset)}};
    }

    // timezone(zone, timetz): a zone's offset is the one in effect now (PostgreSQL 18 timetz_zone), so the
    // function is volatile.
    core::error_t vector_timezone_timetz_named(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& zones = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* zone_source = zones.data<std::string_view>();
        const bool all_valid = all_inputs_valid(inputs);
        conversion_zone_cache_t zone;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (zones.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            if (!zone.load(zone_source[row])) {
                return zone_refusal(ctx.exec_context().resource(), zone_source[row]);
            }
            output.set_value(
                row,
                shift_timetz(values.get_value<core::date::timetz_t>(row), int64_t{zone.zone.current_offset().count()}));
        }
        return core::error_t::no_error();
    }

    core::error_t vector_timezone_timetz_interval(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& zones = inputs.data[0];
        const auto& values = inputs.data[1];
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (zones.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto zone = zones.get_value<core::date::interval_t>(row);
            if (zone.month.count() != 0 || zone.day.count() != 0) {
                std::pmr::string what{"interval time zone \"", ctx.exec_context().resource()};
                what.append(core::date::to_string(zone));
                what.append("\" must not include months or days");
                return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
            }
            output.set_value(
                row,
                shift_timetz(values.get_value<core::date::timetz_t>(row), zone.time.count() / micros_per_second));
        }
        return core::error_t::no_error();
    }

    core::error_t vector_date_trunc_timestamp(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<core::date::timestamp_t>();
        auto* destination = output.data<core::date::timestamp_t>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto parsed = unit.load(unit_source[row]);
            if (!is_truncation_unit(parsed)) {
                return unit_refusal(ctx.exec_context().resource(),
                                    unit_source[row],
                                    timestamp_name,
                                    parsed != unit_t::invalid);
            }
            destination[row] = core::date::timestamp_t{
                core::date::microseconds{truncate_wall_time(parsed, source[row].value.count())}};
        }
        return core::error_t::no_error();
    }

    // date_trunc(unit, timestamptz[, zone]): truncated in the session's zone, or the one named.
    template<bool named_zone>
    core::error_t vector_date_trunc_timestamptz(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<core::date::timestamptz_t>();
        auto* destination = output.data<core::date::timestamptz_t>();
        const auto* settings = ctx.exec_context().settings();
        if constexpr (!named_zone) {
            if (settings == nullptr) {
                return no_settings(resource);
            }
        }
        const std::string_view* zone_source = nullptr;
        if constexpr (named_zone) {
            zone_source = inputs.data[2].data<std::string_view>();
        }
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        conversion_zone_cache_t zone;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid &&
                (units.is_null(row) || values.is_null(row) || (named_zone && inputs.data[2].is_null(row)))) {
                output.set_null(row, true);
                continue;
            }
            const auto parsed = unit.load(unit_source[row]);
            if (!is_truncation_unit(parsed)) {
                return unit_refusal(resource, unit_source[row], timestamptz_name, parsed != unit_t::invalid);
            }
            if constexpr (named_zone) {
                if (!zone.load(zone_source[row])) {
                    return zone_refusal(resource, zone_source[row]);
                }
                destination[row] = truncate_instant(parsed, source[row], zone.zone);
            } else {
                destination[row] = truncate_instant(parsed, source[row], settings->timezone_offset);
            }
        }
        return core::error_t::no_error();
    }

    // date_trunc(unit, date): the date is its midnight in the session's zone, as PostgreSQL 18 reaches
    // date_trunc(text, timestamptz) through the implicit date -> timestamptz cast.
    core::error_t vector_date_trunc_date(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto* settings = ctx.exec_context().settings();
        if (settings == nullptr) {
            return no_settings(resource);
        }
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<core::date::date_t>();
        auto* destination = output.data<core::date::timestamptz_t>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto parsed = unit.load(unit_source[row]);
            if (!is_truncation_unit(parsed)) {
                return unit_refusal(resource, unit_source[row], timestamptz_name, parsed != unit_t::invalid);
            }
            const core::date::timestamp_t midnight{
                core::date::microseconds{int64_t{source[row].value.count()} * micros_per_day}};
            destination[row] =
                truncate_instant(parsed, settings->timezone_offset.to_utc(midnight), settings->timezone_offset);
        }
        return core::error_t::no_error();
    }

    struct interval_fields_t {
        int64_t year;
        int64_t month;
        int64_t day;
        int64_t hour;
        int64_t minute;
        int64_t second;
        int64_t fraction;
    };

    // interval2tm: each field truncated toward zero, so a negative interval has negative fields.
    interval_fields_t fields_of(const core::date::interval_t& value) {
        const int64_t months = value.month.count();
        int64_t time = value.time.count();
        interval_fields_t fields{};
        fields.year = months / 12;
        fields.month = months % 12;
        fields.day = value.day.count();
        fields.hour = time / micros_per_hour;
        time -= fields.hour * micros_per_hour;
        fields.minute = time / micros_per_minute;
        time -= fields.minute * micros_per_minute;
        fields.second = time / micros_per_second;
        fields.fraction = time - fields.second * micros_per_second;
        return fields;
    }

    core::error_t vector_date_trunc_interval(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto parsed = unit.load(unit_source[row]);
            if (!is_truncation_unit(parsed) || parsed == unit_t::week) {
                return unit_refusal(ctx.exec_context().resource(),
                                    unit_source[row],
                                    interval_name,
                                    parsed != unit_t::invalid);
            }
            auto fields = fields_of(values.get_value<core::date::interval_t>(row));
            switch (parsed) {
                case unit_t::millennium:
                    fields.year = (fields.year / 1000) * 1000;
                    [[fallthrough]];
                case unit_t::century:
                    fields.year = (fields.year / 100) * 100;
                    [[fallthrough]];
                case unit_t::decade:
                    fields.year = (fields.year / 10) * 10;
                    [[fallthrough]];
                case unit_t::year:
                    fields.month = 0;
                    [[fallthrough]];
                case unit_t::quarter:
                    fields.month = 3 * (fields.month / 3);
                    [[fallthrough]];
                case unit_t::month:
                    fields.day = 0;
                    [[fallthrough]];
                case unit_t::day:
                    fields.hour = 0;
                    [[fallthrough]];
                case unit_t::hour:
                    fields.minute = 0;
                    [[fallthrough]];
                case unit_t::minute:
                    fields.second = 0;
                    [[fallthrough]];
                case unit_t::second:
                    fields.fraction = 0;
                    break;
                case unit_t::milliseconds:
                    fields.fraction = (fields.fraction / 1000) * 1000;
                    break;
                default:
                    break;
            }
            core::date::interval_t truncated{};
            truncated.month = core::date::months{static_cast<int32_t>(fields.year * 12 + fields.month)};
            truncated.day = core::date::days{static_cast<int32_t>(fields.day)};
            truncated.time =
                core::date::microseconds{fields.hour * micros_per_hour + fields.minute * micros_per_minute +
                                         fields.second * micros_per_second + fields.fraction};
            output.set_value(row, std::move(truncated));
        }
        return core::error_t::no_error();
    }

    // timestamp_part_common over a wall time; false for a unit the wall time has no field for.
    bool wall_time_field(unit_t unit, int64_t micros, double* field) {
        const auto [civil, fraction] = civil_of(micros);
        const absl::CivilDay day{civil};
        const int64_t year = civil.year();
        switch (unit) {
            case unit_t::microseconds:
                *field = static_cast<double>(civil.second() * micros_per_second + fraction);
                return true;
            case unit_t::milliseconds:
                *field = civil.second() * 1000.0 + static_cast<double>(fraction) / 1000.0;
                return true;
            case unit_t::second:
                *field = civil.second() + static_cast<double>(fraction) / 1'000'000.0;
                return true;
            case unit_t::minute:
                *field = civil.minute();
                return true;
            case unit_t::hour:
                *field = civil.hour();
                return true;
            case unit_t::day:
                *field = civil.day();
                return true;
            case unit_t::month:
                *field = civil.month();
                return true;
            case unit_t::quarter:
                *field = (civil.month() - 1) / 3 + 1;
                return true;
            case unit_t::week:
                *field = static_cast<double>(iso_week_of(day).week);
                return true;
            case unit_t::year:
                *field = static_cast<double>(year > 0 ? year : year - 1);
                return true;
            case unit_t::decade:
                *field = static_cast<double>(year >= 0 ? year / 10 : -((8 - (year - 1)) / 10));
                return true;
            case unit_t::century:
                *field = static_cast<double>(year > 0 ? (year + 99) / 100 : -((99 - (year - 1)) / 100));
                return true;
            case unit_t::millennium:
                *field = static_cast<double>(year > 0 ? (year + 999) / 1000 : -((999 - (year - 1)) / 1000));
                return true;
            case unit_t::julian: {
                const int64_t seconds_of_day = civil.hour() * 3600 + civil.minute() * 60 + civil.second();
                *field = static_cast<double>(julian_epoch_day + (day - absl::CivilDay{2000, 1, 1})) +
                         (static_cast<double>(seconds_of_day) + static_cast<double>(fraction) / 1'000'000.0) /
                             static_cast<double>(seconds_per_day);
                return true;
            }
            case unit_t::isoyear: {
                const int64_t iso_year = iso_week_of(day).year;
                *field = static_cast<double>(iso_year > 0 ? iso_year : iso_year - 1);
                return true;
            }
            case unit_t::dow:
                *field = (weekday_from_monday(day) + 1) % 7;
                return true;
            case unit_t::isodow:
                *field = weekday_from_monday(day) + 1;
                return true;
            case unit_t::doy:
                *field = absl::GetYearDay(day);
                return true;
            default:
                return false;
        }
    }

    double epoch_of(int64_t micros) {
        return static_cast<double>(micros) / 1'000'000.0 + static_cast<double>(pg_epoch_unix_seconds);
    }

    // date_part over a timestamp, and a date read as its midnight (PostgreSQL 18 date_part(text, date) casts it).
    template<typename Value>
    core::error_t vector_date_part_wall_time(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<Value>();
        auto* destination = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            int64_t micros = 0;
            if constexpr (std::is_same_v<Value, core::date::date_t>) {
                micros = int64_t{source[row].value.count()} * seconds_per_day * micros_per_second;
            } else {
                micros = source[row].value.count();
            }
            const auto parsed = unit.load(unit_source[row]);
            if (parsed == unit_t::epoch) {
                destination[row] = epoch_of(micros);
                continue;
            }
            if (!wall_time_field(parsed, micros, &destination[row])) {
                return unit_refusal(ctx.exec_context().resource(),
                                    unit_source[row],
                                    timestamp_name,
                                    parsed != unit_t::invalid);
            }
        }
        return core::error_t::no_error();
    }

    // date_part over an instant: its fields in the session's zone, its offset, and its epoch.
    core::error_t vector_date_part_timestamptz(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto* settings = ctx.exec_context().settings();
        if (settings == nullptr) {
            return no_settings(resource);
        }
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<core::date::timestamptz_t>();
        auto* destination = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto parsed = unit.load(unit_source[row]);
            const auto offset = settings->timezone_offset.offset_at(source[row]).count();
            switch (parsed) {
                case unit_t::epoch:
                    destination[row] = epoch_of(source[row].value.count());
                    continue;
                case unit_t::timezone:
                    destination[row] = offset;
                    continue;
                case unit_t::timezone_hour:
                    destination[row] = offset / 3600;
                    continue;
                case unit_t::timezone_minute:
                    destination[row] = (offset / 60) % 60;
                    continue;
                default:
                    break;
            }
            const int64_t local = source[row].value.count() + int64_t{offset} * micros_per_second;
            if (!wall_time_field(parsed, local, &destination[row])) {
                return unit_refusal(resource, unit_source[row], timestamptz_name, parsed != unit_t::invalid);
            }
        }
        return core::error_t::no_error();
    }

    core::error_t vector_date_part_time(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        const auto* source = values.data<core::date::time_t>();
        auto* destination = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const int64_t micros = source[row].value.count();
            const int64_t of_minute = micros % micros_per_minute;
            const auto parsed = unit.load(unit_source[row]);
            switch (parsed) {
                case unit_t::microseconds:
                    destination[row] = static_cast<double>(of_minute);
                    break;
                case unit_t::milliseconds:
                    destination[row] = static_cast<double>(of_minute) / 1000.0;
                    break;
                case unit_t::second:
                    destination[row] = static_cast<double>(of_minute) / 1'000'000.0;
                    break;
                case unit_t::minute:
                    destination[row] = static_cast<double>((micros / micros_per_minute) % 60);
                    break;
                case unit_t::hour:
                    destination[row] = static_cast<double>(micros / micros_per_hour);
                    break;
                case unit_t::epoch:
                    destination[row] = static_cast<double>(micros) / 1'000'000.0;
                    break;
                default:
                    return unit_refusal(ctx.exec_context().resource(),
                                        unit_source[row],
                                        time_name,
                                        parsed != unit_t::invalid);
            }
        }
        return core::error_t::no_error();
    }

    core::error_t vector_date_part_interval(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        constexpr double days_per_year = 365.25;
        constexpr double days_per_month = 30.0;
        const auto& units = inputs.data[0];
        const auto& values = inputs.data[1];
        const auto* unit_source = units.data<std::string_view>();
        auto* destination = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        unit_cache_t unit;
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && (units.is_null(row) || values.is_null(row))) {
                output.set_null(row, true);
                continue;
            }
            const auto value = values.get_value<core::date::interval_t>(row);
            const auto fields = fields_of(value);
            const auto parsed = unit.load(unit_source[row]);
            switch (parsed) {
                case unit_t::microseconds:
                    destination[row] = static_cast<double>(fields.second * micros_per_second + fields.fraction);
                    break;
                case unit_t::milliseconds:
                    destination[row] =
                        static_cast<double>(fields.second) * 1000.0 + static_cast<double>(fields.fraction) / 1000.0;
                    break;
                case unit_t::second:
                    destination[row] =
                        static_cast<double>(fields.second) + static_cast<double>(fields.fraction) / 1'000'000.0;
                    break;
                case unit_t::minute:
                    destination[row] = static_cast<double>(fields.minute);
                    break;
                case unit_t::hour:
                    destination[row] = static_cast<double>(fields.hour);
                    break;
                case unit_t::day:
                    destination[row] = static_cast<double>(fields.day);
                    break;
                case unit_t::month:
                    destination[row] = static_cast<double>(fields.month);
                    break;
                case unit_t::quarter:
                    destination[row] = static_cast<double>(fields.month / 3 + 1);
                    break;
                case unit_t::year:
                    destination[row] = static_cast<double>(fields.year);
                    break;
                case unit_t::decade:
                    destination[row] = static_cast<double>(fields.year / 10);
                    break;
                case unit_t::century:
                    destination[row] = static_cast<double>(fields.year / 100);
                    break;
                case unit_t::millennium:
                    destination[row] = static_cast<double>(fields.year / 1000);
                    break;
                case unit_t::epoch:
                    destination[row] =
                        days_per_year * static_cast<double>(seconds_per_day) * static_cast<double>(fields.year) +
                        days_per_month * static_cast<double>(seconds_per_day) * static_cast<double>(fields.month) +
                        static_cast<double>(seconds_per_day) * static_cast<double>(fields.day) +
                        static_cast<double>(value.time.count()) / 1'000'000.0;
                    break;
                default:
                    return unit_refusal(ctx.exec_context().resource(),
                                        unit_source[row],
                                        interval_name,
                                        parsed != unit_t::invalid);
            }
        }
        return core::error_t::no_error();
    }

    template<typename Function>
    void add(std::pmr::memory_resource* resource,
             Function& fn,
             std::pmr::vector<parameter_type> inputs,
             logical_type result,
             vector_exec_fn exec) {
        kernel_signature_t sig(function_type_t::vector, std::move(inputs), {output_type::fixed(result)});
        [[maybe_unused]] const auto added = fn->add_kernel(resource, vector_kernel{std::move(sig), exec});
        assert(!added.contains_error() && "datetime kernel must fit the declared slots and arity");
    }

    parameter_type exact(logical_type type) { return parameter_type::exact(type); }

    core::pmr::polymorphic_unique_ptr<vector_function> make_timezone_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "Converts between a timestamp and an instant in a time zone",
                         "timezone(zone, timestamp) -> timestamptz; timezone(zone, timestamptz) -> timestamp; zone a "
                         "name, an abbreviation, an offset, or an interval; what AT TIME ZONE lowers to",
                         {"zone", "value"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "timezone",
                                                                      arity::binary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{5},
                                                                      volatility_t::immutable);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP)}, resource},
            logical_type::TIMESTAMP_TZ,
            vector_timezone_named<true>);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP_TZ)}, resource},
            logical_type::TIMESTAMP,
            vector_timezone_named<false>);
        add(resource,
            fn,
            {{exact(logical_type::INTERVAL), exact(logical_type::TIMESTAMP)}, resource},
            logical_type::TIMESTAMP_TZ,
            vector_timezone_interval<true>);
        add(resource,
            fn,
            {{exact(logical_type::INTERVAL), exact(logical_type::TIMESTAMP_TZ)}, resource},
            logical_type::TIMESTAMP,
            vector_timezone_interval<false>);
        add(resource,
            fn,
            {{exact(logical_type::INTERVAL), exact(logical_type::TIME_TZ)}, resource},
            logical_type::TIME_TZ,
            vector_timezone_timetz_interval);
        return fn;
    }

    // timezone(text, timetz) reads the zone's offset now: volatile, unlike the other forms, so a builtin of its own.
    core::pmr::polymorphic_unique_ptr<vector_function> make_timezone_timetz_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "Converts a time with time zone to a time zone",
                         "timezone(zone text, timetz) -> timetz at the offset the zone has now; what AT TIME ZONE "
                         "lowers to",
                         {"zone", "value"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "timezone",
                                                                      arity::binary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::volatile_);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIME_TZ)}, resource},
            logical_type::TIME_TZ,
            vector_timezone_timetz_named);
        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_date_trunc_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "Truncates a timestamp or an interval to a unit",
                         "date_trunc(unit, timestamp | timestamptz [, zone] | date | interval); a timestamptz, and a "
                         "date as its midnight, is truncated in the session's time zone, or the zone named",
                         {"unit", "value", "zone"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "date_trunc",
                                                                      arity::var_args(2),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{5},
                                                                      volatility_t::stable);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP)}, resource},
            logical_type::TIMESTAMP,
            vector_date_trunc_timestamp);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP_TZ)}, resource},
            logical_type::TIMESTAMP_TZ,
            vector_date_trunc_timestamptz<false>);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP_TZ), exact(logical_type::TEXT)}, resource},
            logical_type::TIMESTAMP_TZ,
            vector_date_trunc_timestamptz<true>);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::INTERVAL)}, resource},
            logical_type::INTERVAL,
            vector_date_trunc_interval);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::DATE)}, resource},
            logical_type::TIMESTAMP_TZ,
            vector_date_trunc_date);
        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_date_part_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "A field of a timestamp, a date, a time or an interval",
                         "date_part(unit, timestamp | timestamptz | date | time | interval) -> double precision; a "
                         "timestamptz's fields are read in the session's time zone; what EXTRACT lowers to",
                         {"unit", "value"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "date_part",
                                                                      arity::binary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{5},
                                                                      volatility_t::stable);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP)}, resource},
            logical_type::DOUBLE,
            vector_date_part_wall_time<core::date::timestamp_t>);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIMESTAMP_TZ)}, resource},
            logical_type::DOUBLE,
            vector_date_part_timestamptz);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::DATE)}, resource},
            logical_type::DOUBLE,
            vector_date_part_wall_time<core::date::date_t>);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::TIME)}, resource},
            logical_type::DOUBLE,
            vector_date_part_time);
        add(resource,
            fn,
            {{exact(logical_type::TEXT), exact(logical_type::INTERVAL)}, resource},
            logical_type::DOUBLE,
            vector_date_part_interval);
        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: the uids must mirror DEFAULT_FUNCTIONS entries 24..28 in function.hpp — a uid is the registration
    // order.
    void register_datetime_functions(function_registry_t& r) {
        r.add_builtin(make_convert_tz_func(r.resource()));
        r.add_builtin(make_timezone_func(r.resource()));
        r.add_builtin(make_date_trunc_func(r.resource()));
        r.add_builtin(make_date_part_func(r.resource()));
        r.add_builtin(make_timezone_timetz_func(r.resource()));
    }

} // namespace components::compute
