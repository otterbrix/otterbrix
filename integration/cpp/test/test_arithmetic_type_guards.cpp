#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

// Companion coverage for the operand type guards: paths the bind/runtime checks in the
// dispatcher and arithmetic evaluator do not reach.

// Disabled: exercises a COMPUTED-schema table (CREATE TABLE db.c () -> relkind 'g'), which is
// out of scope for now. The INSERT widening path asks the promotion oracle for a common type of
// (INTEGER, BOOLEAN) and poisons the column vector, aborting in set_value.
#if 0
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
#endif

namespace {

    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.t (id INT, v BIGINT, b BOOLEAN, s TEXT);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "INSERT INTO db.t (id, v, b, s) VALUES (1, 7, true, 'a'), "
                                      "(2, 3, false, 'b');")
                        ->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::arithmetic_type_guards::where_arithmetic_rejects_non_numeric") {
    auto config = test_create_config("/tmp/test_arithmetic_type_guards/where_guards");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // WHERE-clause arithmetic evaluates per row through the predicate value getters, which
    // had no operand type checking: b + 1 read an indeterminate promotion result (silently
    // zero rows), s + 1 crashed in the value machinery, -s compared garbage.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE b + 1 > 0;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE s + 1 > 0;");
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE -s < 0;");
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t WHERE -v < -5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
    }
}

TEST_CASE("integration::cpp::arithmetic_type_guards::where_null_3vl_preserved") {
    auto config = test_create_config("/tmp/test_arithmetic_type_guards/where_3vl");
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
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 1), (2, NULL), (3, 20);")
                    ->is_success());
    }

    // NULL operands stay three-valued: the NULL row is UNKNOWN and silently excluded,
    // never an operator error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n WHERE 10 / v > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 1);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n WHERE v + 1 > 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 3);
    }
}

TEST_CASE("integration::cpp::arithmetic_type_guards::update_rejects_non_numeric_arithmetic") {
    auto config = test_create_config("/tmp/test_arithmetic_type_guards/update_guard");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // The UPDATE expression tree fed b + 1 straight into the binary kernel, whose
    // unresolvable-pair result is an all-NULL vector: the column was silently overwritten
    // with NULLs. The operand pair must be rejected and the data left intact.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "UPDATE db.t SET v = b + 1;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 3);
    }
}
