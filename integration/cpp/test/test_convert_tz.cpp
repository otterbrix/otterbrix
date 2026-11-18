#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>

// CONVERT_TZ(dt, from_tz, to_tz), as MySQL 8.4 answers it: a zone is an IANA name (with its daylight saving time) or
// [+|-]HH:MM; NULL for a NULL argument or a text that names no zone.

namespace {
    using test_helpers::exec;

    template<typename D>
    void require_ok(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        INFO(sql << " error: " << (c->is_error() ? c->get_error().what : ""));
        REQUIRE(c->is_success());
    }

    // The one boolean cell of `sql`.
    template<typename D>
    bool truth(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        INFO(sql << " error: " << (c->is_error() ? c->get_error().what : ""));
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        auto cell = c->template get<bool>(0, 0);
        REQUIRE_FALSE(cell.has_error());
        return cell.value();
    }

    template<typename D>
    bool null_cell(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        INFO(sql << " error: " << (c->is_error() ? c->get_error().what : ""));
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        auto cell = c->is_null(0, 0);
        REQUIRE_FALSE(cell.has_error());
        return cell.value();
    }

    // Whether convert_tz(ts, from, to) of row `id` is `expected`.
    template<typename D>
    bool converts_to(D* d, int id, const std::string& from, const std::string& to, const std::string& expected) {
        return truth(d,
                     "SELECT convert_tz(ts, '" + from + "', '" + to + "') = '" + expected +
                         "'::timestamp FROM m.t WHERE id = " + std::to_string(id) + ";");
    }

    template<typename D>
    void seed(D* d) {
        require_ok(d, "CREATE SCHEMA m;");
        require_ok(d, "CREATE TABLE m.t (id INT, ts TIMESTAMP);");
        require_ok(d, "INSERT INTO m.t (id, ts) VALUES (1, '2024-01-15 10:00:00');");
        require_ok(d, "INSERT INTO m.t (id, ts) VALUES (2, '2024-07-15 10:00:00');");
        require_ok(d, "INSERT INTO m.t (id, ts) VALUES (3, NULL);");
        require_ok(d, "INSERT INTO m.t (id, ts) VALUES (4, '1969-06-01 10:00:00');");
        require_ok(d, "INSERT INTO m.t (id, ts) VALUES (5, '2024-01-15 10:00:00.25');");
    }
} // namespace

// Django's MySQL backend asks this when it connects: whether the server knows its zones.
TEST_CASE("integration::cpp::convert_tz::django_zoneinfo_probe") {
    auto config = test_helpers::make_test_config(integration_fixture_path("convert_tz/probe"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    CHECK(truth(d, "SELECT CONVERT_TZ('2001-01-01 01:00:00', 'UTC', 'UTC') IS NOT NULL;"));
}

TEST_CASE("integration::cpp::convert_tz::zones") {
    auto config = test_helpers::make_test_config(integration_fixture_path("convert_tz/zones"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("a named zone follows its daylight saving time") {
        CHECK(converts_to(d, 1, "UTC", "Europe/Paris", "2024-01-15 11:00:00"));
        CHECK(converts_to(d, 2, "UTC", "Europe/Paris", "2024-07-15 12:00:00"));
        CHECK(converts_to(d, 2, "Europe/Paris", "UTC", "2024-07-15 08:00:00"));
        CHECK(converts_to(d, 1, "America/New_York", "Europe/Paris", "2024-01-15 16:00:00"));
    }
    SECTION("an offset") {
        CHECK(converts_to(d, 1, "UTC", "+03:00", "2024-01-15 13:00:00"));
        CHECK(converts_to(d, 2, "+05:30", "-2:00", "2024-07-15 02:30:00"));
        CHECK(converts_to(d, 1, "+14:00", "-13:59", "2024-01-14 06:01:00"));
    }
    SECTION("the fraction of a second is kept") { CHECK(converts_to(d, 5, "UTC", "+03:00", "2024-01-15 13:00:00.25")); }
    SECTION("a value before 1970 is not converted") {
        CHECK(converts_to(d, 4, "UTC", "+03:00", "1969-06-01 10:00:00"));
    }
    SECTION("NULL for a NULL argument") {
        CHECK(null_cell(d, "SELECT convert_tz(ts, 'UTC', '+03:00') FROM m.t WHERE id = 3;"));
        CHECK(null_cell(d, "SELECT convert_tz(ts, NULL, '+03:00') FROM m.t WHERE id = 1;"));
        CHECK(null_cell(d, "SELECT convert_tz(ts, 'UTC', NULL) FROM m.t WHERE id = 1;"));
    }
    SECTION("NULL for a text that names no zone") {
        for (const char* zone : {"Mars/Phobos",
                                 "SYSTEM",
                                 "localtime",
                                 "+14:01",
                                 "-14:00",
                                 "+03:60",
                                 "+3",
                                 "",
                                 "../Europe/Paris",
                                 "/etc/localtime"}) {
            INFO(zone);
            CHECK(null_cell(d, std::string{"SELECT convert_tz(ts, 'UTC', '"} + zone + "') FROM m.t WHERE id = 1;"));
        }
    }
}
