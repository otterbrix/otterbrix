#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

// Companion coverage for the operand type guards: paths the bind/runtime checks in the
// dispatcher and arithmetic evaluator do not reach.

TEST_CASE("integration::cpp::arithmetic_type_guards::mixed_bool_int_insert_survives") {
    auto config = test_create_config("/tmp/test_arithmetic_type_guards/mixed_insert");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.c ();")->is_success());
    }

    // The INSERT widening path treated BOOLEAN as numeric and asked the promotion oracle
    // for a (INTEGER, BOOLEAN) common type; the answer poisoned the column vector and the
    // statement crashed. An unpromotable pair takes the plain per-value store instead.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.c (x) VALUES (1), (true);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT x FROM db.c;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
}
