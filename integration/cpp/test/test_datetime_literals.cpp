#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/connection.hpp>
#include <unistd.h>

#include <set>
#include <string>

// A quoted literal compared with, cast to or inserted into a date/time column takes the column's type; a timestamptz
// one without an offset reads in the session's time zone, daylight saving time included.

namespace {

    using otterbrix::connection_t;
    using ids_t = std::set<int64_t>;

    void require_ok(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
    }

    ids_t ids(connection_t& session, const std::string& where) {
        const std::string sql = "SELECT id FROM m.t WHERE " + where + ";";
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        ids_t out;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            auto id = cursor->get<int64_t>(0, row);
            REQUIRE_FALSE(id.has_error());
            out.insert(id.value());
        }
        return out;
    }

    configuration::config fresh_config(const std::string& name) {
        auto config =
            test_create_config(integration_fixture_path("datetime_literals") / name / std::to_string(::getpid()));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    // Row 1 is 10:00 of a winter day and row 2 10:00 of a summer day in Berlin (tstz 09:00Z and 08:00Z); row 3 is
    // 10:00Z, which is 10:00 only in UTC.
    void seed(connection_t& session) {
        require_ok(session, "CREATE SCHEMA m;");
        require_ok(session, "CREATE TABLE m.t (id BIGINT, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, tm TIME, s TEXT);");
        require_ok(session,
                   "INSERT INTO m.t (id, ts, tstz, d, tm, s) VALUES "
                   "(1, '2026-01-01 10:00:00', '2026-01-01 09:00:00+00', '2026-01-01', '10:00:00', "
                   "'2026-01-01 10:00:00'), "
                   "(2, '2026-07-01 10:00:00', '2026-07-01 08:00:00+00', '2026-07-01', '10:00:00.5', "
                   "'2026-07-01 10:00:00'), "
                   "(3, '2026-01-01 10:00:01', '2026-01-01 10:00:00+00', '2026-01-02', '23:59:59', "
                   "'2026-01-01 10:00:01'), "
                   "(4, NULL, NULL, NULL, NULL, NULL);");
    }

    void literal_cases(connection_t& session) {
        SECTION("timestamp") {
            CHECK(ids(session, "ts = '2026-01-01 10:00:00'") == ids_t{1});
            CHECK(ids(session, "'2026-01-01 10:00:00' = ts") == ids_t{1});
            CHECK(ids(session, "ts > '2026-01-01 10:00:00'") == ids_t{2, 3});
            CHECK(ids(session, "ts <> '2026-01-01 10:00:00'") == ids_t{2, 3});
            CHECK(ids(session, "ts BETWEEN '2026-01-01' AND '2026-01-01 23:59:59'") == ids_t{1, 3});
            CHECK(ids(session, "ts IN ('2026-07-01 10:00:00', '2026-01-01 10:00:01')") == ids_t{2, 3});
        }
        SECTION("timestamptz in the session's zone") {
            require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
            CHECK(ids(session, "tstz = '2026-01-01 10:00:00'") == ids_t{1});
            CHECK(ids(session, "tstz = '2026-07-01 10:00:00'") == ids_t{2});
            CHECK(ids(session, "tstz = '2026-01-01 10:00:00+00'") == ids_t{3});
            CHECK(ids(session, "tstz < '2026-01-01 11:00:00'") == ids_t{1});
            require_ok(session, "SET TIME ZONE 'UTC';");
            CHECK(ids(session, "tstz = '2026-01-01 10:00:00'") == ids_t{3});
            CHECK(ids(session, "tstz = '2026-07-01 08:00:00'") == ids_t{2});
        }
        SECTION("date") {
            CHECK(ids(session, "d = '2026-01-01'") == ids_t{1});
            CHECK(ids(session, "d >= '2026-01-02'") == ids_t{2, 3});
        }
        SECTION("time") {
            CHECK(ids(session, "tm = '10:00:00'") == ids_t{1});
            CHECK(ids(session, "tm > '10:00:00'") == ids_t{2, 3});
        }
        SECTION("an explicit cast of a literal or of text") {
            CHECK(ids(session, "ts = CAST('2026-01-01 10:00:00' AS TIMESTAMP)") == ids_t{1});
            CHECK(ids(session, "ts = '2026-01-01 10:00:00'::text::timestamp") == ids_t{1});
            CHECK(ids(session, "ts = s::timestamp") == ids_t{1, 2, 3});
            CHECK(ids(session, "d = '2026-01-01'::date") == ids_t{1});
            CHECK(ids(session, "d = s::date") == ids_t{1, 2});
            require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
            CHECK(ids(session, "tstz = s::timestamptz") == ids_t{1, 2});
            CHECK(ids(session, "tstz::date = '2026-07-01'") == ids_t{2});
        }
        SECTION("a literal that is no date/time") {
            CHECK(session.execute("SELECT id FROM m.t WHERE ts = 'not a timestamp';")->is_error());
            CHECK(session.execute("SELECT id FROM m.t WHERE d = '2026-02-30';")->is_error());
        }
    }

} // namespace

TEST_CASE("integration::cpp::datetime_literals::scan") {
    auto instance = test_make_otterbrix(fresh_config("scan"));
    connection_t session(instance);
    seed(session);
    literal_cases(session);
}

TEST_CASE("integration::cpp::datetime_literals::index") {
    auto instance = test_make_otterbrix(fresh_config("index"));
    connection_t session(instance);
    seed(session);
    require_ok(session, "CREATE INDEX t_ts ON m.t (ts);");
    require_ok(session, "CREATE INDEX t_tstz ON m.t (tstz);");
    require_ok(session, "CREATE INDEX t_d ON m.t (d);");
    literal_cases(session);
}

// An offset-less literal inserted into a timestamptz reads in the session's zone; another zone finds the same instant.
TEST_CASE("integration::cpp::datetime_literals::insert_in_a_zone") {
    auto instance = test_make_otterbrix(fresh_config("insert_in_a_zone"));
    connection_t session(instance);
    require_ok(session, "CREATE SCHEMA m;");
    require_ok(session, "CREATE TABLE m.t (id BIGINT, tstz TIMESTAMPTZ);");
    require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
    require_ok(session, "INSERT INTO m.t (id, tstz) VALUES (1, '2026-07-01 12:00:00'), (2, '2026-01-01 12:00:00');");
    require_ok(session, "SET TIME ZONE 'UTC';");
    CHECK(ids(session, "tstz = '2026-07-01 10:00:00'") == ids_t{1});
    CHECK(ids(session, "tstz = '2026-01-01 11:00:00'") == ids_t{2});
}
