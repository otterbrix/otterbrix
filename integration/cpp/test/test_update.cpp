#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <components/types/logical_value.hpp>

// UPDATE SET used to dispatch operators by their FIRST character: '?' matched
// no case and left a null expression that the executor dereferenced (segfault
// in Release), '->' silently lowered to numeric subtraction, '!=' to
// factorial. Unknown operators must error; arithmetic keeps working, including
// operators that merely share a first character with a rejected one.
TEST_CASE("integration::cpp::test_update::set_unknown_operators") {
    auto config = test_create_config(test_temp_path("otterbrix/integration/test_update/set_unknown_operators"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE t;")->is_success());
    REQUIRE(exec("CREATE TABLE t.u (x BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO t.u (x) VALUES (9);")->is_success());

    CHECK_FALSE(exec("UPDATE t.u SET x = x ? 3;")->is_success());  // was a null-deref segfault
    CHECK_FALSE(exec("UPDATE t.u SET x = x -> 3;")->is_success()); // was silent subtraction
    CHECK_FALSE(exec("UPDATE t.u SET x = x != 3;")->is_success()); // was factorial
    {
        auto cur = exec("SELECT x FROM t.u;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 9); // untouched by the rejects
    }

    REQUIRE(exec("UPDATE t.u SET x = x + 1;")->is_success());  // 10
    REQUIRE(exec("UPDATE t.u SET x = x # 3;")->is_success());  // 10 XOR 3 = 9
    REQUIRE(exec("UPDATE t.u SET x = x << 1;")->is_success()); // 18
    {
        auto cur = exec("SELECT x FROM t.u;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 18);
    }
}

// A prefix unary operator (-x, @ x, ~x) parses with a null lexpr; the update
// executor dereferences its LEFT operand unconditionally and evaluates unary
// ops on it, so these used to segfault at execution (and even well-formed
// prefix spellings had the operand in the wrong slot). Unary operators route
// the operand to the left slot; binary ones require both operands.
TEST_CASE("integration::cpp::test_update::set_unary_operand_arity") {
    auto config = test_create_config(test_temp_path("otterbrix/integration/test_update/set_unary_operand_arity"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE t;")->is_success());
    REQUIRE(exec("CREATE TABLE t.ua (x BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO t.ua (x) VALUES (9);")->is_success());

    // '-' has no unary form in update_expr_type: clean error, not a null-deref.
    CHECK_FALSE(exec("UPDATE t.ua SET x = -x;")->is_success());
    CHECK_FALSE(exec("UPDATE t.ua SET x = +x;")->is_success());
    // Genuinely unary operators work in prefix form.
    REQUIRE(exec("UPDATE t.ua SET x = @ x;")->is_success());
    {
        auto cur = exec("SELECT x FROM t.ua;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 9); // @9 = 9
    }
}

// transform_update_expr's switch fell off for node tags it does not handle
// (function calls, CASE, subqueries) and returned nullptr WITHOUT setting the
// transformer error, so a null child shipped in the plan: nested cases
// segfaulted the executor, a top-level one was silently dropped (success
// reported, nothing updated).
TEST_CASE("integration::cpp::test_update::set_unsupported_expressions") {
    auto config = test_create_config(test_temp_path("otterbrix/integration/test_update/set_unsupported_expressions"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE t;")->is_success());
    REQUIRE(exec("CREATE TABLE t.uf (x BIGINT, s TEXT);")->is_success());
    REQUIRE(exec("INSERT INTO t.uf (x, s) VALUES (9, 'ab');")->is_success());

    CHECK_FALSE(exec("UPDATE t.uf SET s = upper(s);")->is_success());
    CHECK_FALSE(exec("UPDATE t.uf SET x = x + abs(x);")->is_success());
    CHECK_FALSE(exec("UPDATE t.uf SET x = CASE WHEN x > 0 THEN 1 ELSE 0 END;")->is_success());
    {
        auto cur = exec("SELECT x, s FROM t.uf;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 9); // data untouched
        CHECK(cur->value(1, 0).value<std::string_view>() == "ab");
    }
}

// UPDATE SET column = NULL hit the T_A_Const default arm: assert(false) in
// Debug, an UNINITIALIZED parameter id in Release (garbage lookup, segfault).
// The executor also had no NA cast kernel, so an NA-typed constant vector
// crashed cast_vector; nulls are now written directly.
TEST_CASE("integration::cpp::test_update::set_null") {
    auto config = test_create_config(test_temp_path("otterbrix/integration/test_update/set_null"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE t;")->is_success());
    REQUIRE(exec("CREATE TABLE t.un (x BIGINT, y BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO t.un (x, y) VALUES (9, 1), (10, 2);")->is_success());

    REQUIRE(exec("UPDATE t.un SET x = NULL WHERE y = 1;")->is_success());
    {
        auto cur = exec("SELECT x FROM t.un ORDER BY y;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).is_null());
        CHECK(cur->value(0, 1).value<int64_t>() == 10); // other row untouched
    }
    // Nested-element NULL writes have no NA cast kernel: clean transform error.
    CHECK_FALSE(exec("UPDATE t.un SET x[1] = NULL;")->is_success());
}
