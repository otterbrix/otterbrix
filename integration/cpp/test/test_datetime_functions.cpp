#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/connection.hpp>
#include <unistd.h>

#include <cmath>
#include <string>

// timezone() / AT TIME ZONE, date_trunc() and date_part() / EXTRACT, as PostgreSQL 18 answers them, daylight saving
// time included.

namespace {

    using otterbrix::connection_t;

    void require_ok(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
    }

    // The one boolean `expression` answers.
    bool holds(connection_t& session, const std::string& expression) {
        const std::string sql = "SELECT " + expression + ";";
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        auto value = cursor->get<bool>(0, 0);
        REQUIRE_FALSE(value.has_error());
        return value.value();
    }

    // Whether the one double precision `expression` answers is `expected`.
    bool is(connection_t& session, const std::string& expression, double expected) {
        const std::string sql = "SELECT " + expression + ";";
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        auto value = cursor->get<double>(0, 0);
        REQUIRE_FALSE(value.has_error());
        INFO(sql << " = " << value.value());
        return std::abs(value.value() - expected) < 1e-6;
    }

    bool refused(connection_t& session, const std::string& expression) {
        return session.execute("SELECT " + expression + ";")->is_error();
    }

    configuration::config fresh_config(const std::string& name) {
        auto config =
            test_create_config(integration_fixture_path("datetime_functions") / name / std::to_string(::getpid()));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    const std::string moment = "'2026-07-15 13:45:30.123456'::timestamp";

} // namespace

TEST_CASE("integration::cpp::datetime_functions::timezone") {
    auto instance = test_make_otterbrix(fresh_config("timezone"));
    connection_t session(instance);

    SECTION("a timestamp read as a zone's wall time") {
        CHECK(holds(session,
                    "timezone('Europe/Berlin', '2026-07-01 10:00:00'::timestamp) = "
                    "'2026-07-01 08:00:00+00'::timestamptz"));
        CHECK(holds(session,
                    "'2026-01-01 10:00:00'::timestamp AT TIME ZONE 'Europe/Berlin' = "
                    "'2026-01-01 09:00:00+00'::timestamptz"));
        CHECK(holds(session,
                    "'2026-01-01 10:00:00'::timestamp AT TIME ZONE 'europe/berlin' = "
                    "'2026-01-01 09:00:00+00'::timestamptz"));
    }
    SECTION("an instant as a zone's wall time") {
        CHECK(holds(session,
                    "('2026-07-01 08:00:00+00'::timestamptz AT TIME ZONE 'Europe/Berlin') = "
                    "'2026-07-01 10:00:00'::timestamp"));
        CHECK(holds(session,
                    "('2026-01-01 08:00:00+00'::timestamptz AT TIME ZONE 'America/New_York') = "
                    "'2026-01-01 03:00:00'::timestamp"));
    }
    SECTION("a time a transition skips or repeats") {
        // Spring forward 02:00 -> 03:00: 02:30 reads in the offset before it. Fall back 03:00 -> 02:00: 02:30 is
        // the later of its two instants.
        CHECK(holds(session,
                    "timezone('Europe/Berlin', '2026-03-29 02:30:00'::timestamp) = "
                    "'2026-03-29 01:30:00+00'::timestamptz"));
        CHECK(holds(session,
                    "timezone('Europe/Berlin', '2026-10-25 02:30:00'::timestamp) = "
                    "'2026-10-25 01:30:00+00'::timestamptz"));
    }
    SECTION("an interval and an abbreviation are fixed offsets") {
        CHECK(holds(session,
                    "timezone(INTERVAL '05:00:00', '2026-01-01 10:00:00'::timestamp) = "
                    "'2026-01-01 05:00:00+00'::timestamptz"));
        CHECK(holds(session,
                    "timezone(INTERVAL '05:00:00', '2026-01-01 05:00:00+00'::timestamptz) = "
                    "'2026-01-01 10:00:00'::timestamp"));
        CHECK(holds(session,
                    "timezone('CEST', '2026-01-01 10:00:00'::timestamp) = '2026-01-01 08:00:00+00'::timestamptz"));
    }
    SECTION("a time with time zone") {
        CHECK(holds(session, "timezone(INTERVAL '03:00:00', '10:00:00+00'::timetz) = '13:00:00+03'::timetz"));
        CHECK(holds(session, "timezone('UTC', '10:00:00+03'::timetz) = '07:00:00+00'::timetz"));
    }
    SECTION("what names no zone") {
        CHECK(refused(session, "timezone('Mars/Phobos', '2026-01-01 10:00:00'::timestamp)"));
        CHECK(refused(session, "timezone(INTERVAL '1 day', '2026-01-01 10:00:00'::timestamp)"));
    }
}

TEST_CASE("integration::cpp::datetime_functions::date_trunc") {
    auto instance = test_make_otterbrix(fresh_config("date_trunc"));
    connection_t session(instance);

    SECTION("every unit of a timestamp") {
        const auto truncated = [&](const std::string& unit, const std::string& expected) {
            INFO(unit);
            CHECK(holds(session, "date_trunc('" + unit + "', " + moment + ") = '" + expected + "'::timestamp"));
        };
        truncated("microseconds", "2026-07-15 13:45:30.123456");
        truncated("milliseconds", "2026-07-15 13:45:30.123");
        truncated("second", "2026-07-15 13:45:30");
        truncated("minute", "2026-07-15 13:45:00");
        truncated("hour", "2026-07-15 13:00:00");
        truncated("day", "2026-07-15 00:00:00");
        truncated("week", "2026-07-13 00:00:00");
        truncated("month", "2026-07-01 00:00:00");
        truncated("quarter", "2026-07-01 00:00:00");
        truncated("year", "2026-01-01 00:00:00");
        truncated("decade", "2020-01-01 00:00:00");
        truncated("century", "2001-01-01 00:00:00");
        truncated("millennium", "2001-01-01 00:00:00");
        truncated("HOURS", "2026-07-15 13:00:00");
    }
    SECTION("an instant is truncated in the session's zone") {
        require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
        CHECK(
            holds(session,
                  "date_trunc('day', '2026-07-15 23:30:00+00'::timestamptz) = '2026-07-15 22:00:00+00'::timestamptz"));
        // 30 October is CET, 1 October CEST: a day or more reads the zone's offset again.
        CHECK(holds(session,
                    "date_trunc('month', '2026-10-30 12:00:00+00'::timestamptz) = "
                    "'2026-09-30 22:00:00+00'::timestamptz"));
        CHECK(holds(session,
                    "date_trunc('hour', '2026-10-25 00:30:00+00'::timestamptz) = "
                    "'2026-10-25 00:00:00+00'::timestamptz"));
        CHECK(holds(session,
                    "date_trunc('day', '2026-07-15 23:30:00+00'::timestamptz, 'Asia/Tokyo') = "
                    "'2026-07-15 15:00:00+00'::timestamptz"));
        CHECK(holds(session, "date_trunc('month', '2026-07-15'::date) = '2026-06-30 22:00:00+00'::timestamptz"));
    }
    SECTION("an interval") {
        CHECK(holds(session, "date_trunc('hour', INTERVAL '1 day 03:25:00') = INTERVAL '1 day 03:00:00'"));
        CHECK(holds(session, "date_trunc('year', INTERVAL '14 months 3 days') = INTERVAL '1 year'"));
    }
    SECTION("a unit that is none, or not one for the type") {
        CHECK(refused(session, "date_trunc('fortnight', " + moment + ")"));
        CHECK(refused(session, "date_trunc('dow', " + moment + ")"));
        CHECK(refused(session, "date_trunc('week', INTERVAL '3 days')"));
        CHECK(refused(session, "date_trunc('day', '2026-07-15 23:30:00+00'::timestamptz, 'Mars/Phobos')"));
    }
}

TEST_CASE("integration::cpp::datetime_functions::extract") {
    auto instance = test_make_otterbrix(fresh_config("extract"));
    connection_t session(instance);
    const std::string timestamp = "'2026-07-15 13:45:30.5'::timestamp";

    SECTION("every field of a timestamp") {
        const auto field = [&](const std::string& name, double expected) {
            INFO(name);
            CHECK(is(session, "EXTRACT(" + name + " FROM " + timestamp + ")", expected));
        };
        field("year", 2026);
        field("quarter", 3);
        field("month", 7);
        field("week", 29);
        field("day", 15);
        field("hour", 13);
        field("minute", 45);
        field("second", 30.5);
        field("milliseconds", 30500);
        field("microseconds", 30500000);
        field("dow", 3);
        field("isodow", 3);
        field("doy", 196);
        field("isoyear", 2026);
        field("decade", 202);
        field("century", 21);
        field("millennium", 3);
        field("epoch", 1784123130.5);
        CHECK(is(session, "date_part('day', " + timestamp + ")", 15));
    }
    SECTION("an instant's fields are those of the session's zone") {
        require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
        CHECK(is(session, "EXTRACT(hour FROM '2026-07-15 10:00:00+00'::timestamptz)", 12));
        CHECK(is(session, "EXTRACT(hour FROM '2026-01-15 10:00:00+00'::timestamptz)", 11));
        CHECK(is(session, "EXTRACT(timezone FROM '2026-07-15 10:00:00+00'::timestamptz)", 7200));
        CHECK(is(session, "EXTRACT(timezone_hour FROM '2026-01-15 10:00:00+00'::timestamptz)", 1));
        CHECK(is(session, "EXTRACT(epoch FROM '2026-07-15 10:00:00+00'::timestamptz)", 1784109600));
    }
    SECTION("a date, a time and an interval") {
        CHECK(is(session, "EXTRACT(doy FROM '2026-03-01'::date)", 60));
        CHECK(is(session, "EXTRACT(minute FROM '13:45:30'::time)", 45));
        CHECK(is(session, "EXTRACT(epoch FROM '13:45:30'::time)", 49530));
        CHECK(is(session, "EXTRACT(epoch FROM INTERVAL '1 day 01:00:00')", 90000));
        CHECK(is(session, "EXTRACT(hour FROM INTERVAL '25:00:00')", 25));
        CHECK(is(session, "EXTRACT(year FROM INTERVAL '14 months')", 1));
    }
    SECTION("a field that is none, or not one for the type") {
        CHECK(refused(session, "EXTRACT(fortnight FROM " + timestamp + ")"));
        CHECK(refused(session, "EXTRACT(timezone FROM " + timestamp + ")"));
        CHECK(refused(session, "EXTRACT(dow FROM '13:45:30'::time)"));
        CHECK(refused(session, "EXTRACT(week FROM INTERVAL '3 days')"));
    }
}
