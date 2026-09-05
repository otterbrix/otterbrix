#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

// A TINYINT VALUE WRITTEN INTO A NUMERIC COLUMN, THROUGH ORDINARY SQL.
//
// components::types::logical_value_t::cast_as routes `<integer> -> DECIMAL` through a switch
// over the SOURCE logical type, and that switch listed USMALLINT..DOUBLE. TINYINT and
// UTINYINT are is_numeric(), so they reached the branch like every other integer width and
// fell into its `default:` -- which, before this change, was an `assert(false)` with NO
// RETURN behind it. Two different failures, one statement:
//   * Debug: SIGABRT, the whole process, on an INSERT;
//   * Release (NDEBUG): the assert is compiled out, control walks off the end of the switch,
//     out of the else-if chain, and into cast_as's trailing `return NA` -- so the row was
//     stored as NULL and the statement reported success.
//
// The unit coverage for the repair is in components/types/tests/test_types.cpp, on cast_as
// itself. THIS file is the other half the unit tests cannot give: that the arms are reached
// by a plain INSERT / SELECT and that the VALUE arrives, because "the statement succeeded"
// is exactly what the Release half of the bug already reported.

using namespace test_helpers;

TEST_CASE("integration::cpp::test_tinyint_numeric_cast::a_tinyint_column_lands_in_a_numeric_column") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_tinyint_numeric_cast/signed", true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE tn;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.src (id BIGINT, a TINYINT);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE tn.dst (id BIGINT, n NUMERIC(10,2));")->is_success());

    // The three values that matter for a scaled int8: a plain one, the floor and the ceiling.
    REQUIRE(exec(d, "INSERT INTO tn.src (id, a) VALUES (1, 7), (2, -128), (3, 127);")->is_success());

    // THE CAST. `a` is TINYINT and `n` is NUMERIC(10,2), so the write path asks cast_as for
    // exactly the arm that was missing.
    REQUIRE(exec(d, "INSERT INTO tn.dst (id, n) SELECT id, a FROM tn.src;")->is_success());

    auto cur = exec(d, "SELECT n FROM tn.dst ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    // NOT MERELY "not null": the Release half of the bug stored NULL and said success, and a
    // scale mistake would store 7 instead of 700 with is_null() just as false either way.
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
    auto config = make_test_config("/tmp/otterbrix/integration/test_tinyint_numeric_cast/unsigned", true);
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

    // And the sign-confusion answer must NOT be there: -1.00 is what int8_t makes of 255.
    auto wrong = exec(d, "SELECT id FROM tn.dst WHERE n = -1.00;");
    REQUIRE(wrong->is_success());
    CHECK(wrong->size() == 0);
}
