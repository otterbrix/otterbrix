#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

// cast_as's `<integer> -> DECIMAL` switch had no TINYINT/UTINYINT arm, falling into an
// `assert(false)` default: SIGABRT in Debug, a silent `return NA` (row stored NULL, statement
// reported success) in Release.

using namespace test_helpers;

TEST_CASE("integration::cpp::test_tinyint_numeric_cast::a_tinyint_column_lands_in_a_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_tinyint_numeric_cast/signed"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE tn;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.src (id BIGINT, a TINYINT);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.dst (id BIGINT, n NUMERIC(10,2));")->is_success());

    REQUIRE(exec(d, "INSERT INTO tn.src (id, a) VALUES (1, 7), (2, -128), (3, 127);")->is_success());

    REQUIRE(exec(d, "INSERT INTO tn.dst (id, n) SELECT id, a FROM tn.src;")->is_success());

    auto cur = exec(d, "SELECT n FROM tn.dst ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    // Not merely "not null": the Release half of the bug stored NULL and reported success, and a
    // scale mistake would store 7 instead of 700 with is_null() false either way.
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK_FALSE(cur->value(0, 1).is_null());
    CHECK_FALSE(cur->value(0, 2).is_null());

    auto probe = exec(d, "SELECT id FROM tn.dst WHERE n = 7.00;");
    REQUIRE(probe->is_success());
    REQUIRE(probe->size() == 1);
    CHECK(probe->value(0, 0).value<int64_t>() == 1);

    auto floor_probe = exec(d, "SELECT id FROM tn.dst WHERE n = -128.00;");
    REQUIRE(floor_probe->is_success());
    REQUIRE(floor_probe->size() == 1);
    CHECK(floor_probe->value(0, 0).value<int64_t>() == 2);

    auto ceiling_probe = exec(d, "SELECT id FROM tn.dst WHERE n = 127.00;");
    REQUIRE(ceiling_probe->is_success());
    REQUIRE(ceiling_probe->size() == 1);
    CHECK(ceiling_probe->value(0, 0).value<int64_t>() == 3);
}

TEST_CASE("integration::cpp::test_tinyint_numeric_cast::a_utinyint_column_lands_in_a_numeric_column") {
    auto config = make_test_config(integration_fixture_path("test_tinyint_numeric_cast/unsigned"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE tn;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.src (id BIGINT, a UTINYINT);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.dst (id BIGINT, n NUMERIC(10,2));")->is_success());

    // 255 is the value that separates the UTINYINT arm from the TINYINT one: read through
    // int8_t it is -1.
    REQUIRE(exec(d, "INSERT INTO tn.src (id, a) VALUES (1, 0), (2, 255);")->is_success());
    REQUIRE(exec(d, "INSERT INTO tn.dst (id, n) SELECT id, a FROM tn.src;")->is_success());

    auto cur = exec(d, "SELECT n FROM tn.dst ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK_FALSE(cur->value(0, 1).is_null());

    auto probe = exec(d, "SELECT id FROM tn.dst WHERE n = 255.00;");
    REQUIRE(probe->is_success());
    REQUIRE(probe->size() == 1);
    CHECK(probe->value(0, 0).value<int64_t>() == 2);

    auto wrong = exec(d, "SELECT id FROM tn.dst WHERE n = -1.00;");
    REQUIRE(wrong->is_success());
    CHECK(wrong->size() == 0);
}
