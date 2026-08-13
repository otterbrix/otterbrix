#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/operations_helper.hpp>
#include <services/collection/executor.hpp>

namespace {

    int find_column(const components::cursor::cursor_t& cur, std::string_view name) {
        const auto& chunk = cur.chunks().front();
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            if (chunk.data[i].type().alias() == name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    template<typename Int>
    void check_int_array_1_2_3(const components::cursor::cursor_t& cur) {
        REQUIRE(cur.is_success());
        REQUIRE(cur.size() == 1);
        REQUIRE(cur.column_count() == 1);
        auto v = cur.value(0, 0);
        const auto& children = v.children();
        REQUIRE(children.size() == 3);
        REQUIRE(children[0].value<Int>() == static_cast<Int>(1));
        REQUIRE(children[1].value<Int>() == static_cast<Int>(2));
        REQUIRE(children[2].value<Int>() == static_cast<Int>(3));
    }

} // namespace

TEST_CASE("integration::cpp::correctness_bugs::array_int_slot_width") {
    auto config = test_create_config("/tmp/test_correctness_bugs/array_int_slot_width");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.intarr  (xs INT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.smlarr  (xs SMALLINT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.bigarr  (xs BIGINT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.intarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.smlarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.bigarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.intarr;");
        check_int_array_1_2_3<int32_t>(*cur);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.smlarr;");
        check_int_array_1_2_3<int16_t>(*cur);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.bigarr;");
        check_int_array_1_2_3<int64_t>(*cur);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::unsupported_boolean_text_arithmetic") {
    auto config = test_create_config("/tmp/test_correctness_bugs/unsupported_boolean_text_arithmetic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.bad_arith (b BOOLEAN, s TEXT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "INSERT INTO t.bad_arith (b, s) VALUES (true, 'hello');")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT b + 1 FROM t.bad_arith;");
        INFO("BOOLEAN arithmetic error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::schema_error);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -s FROM t.bad_arith;");
        INFO("TEXT unary-minus error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::schema_error);
    }

    // Invalid expressions must not corrupt the engine process or session state.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT b, s FROM t.bad_arith;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<bool>());
        REQUIRE(cur->value(1, 0).value<std::string_view>() == "hello");
    }
}

TEST_CASE("integration::cpp::correctness_bugs::alias_collision") {
    auto config = test_create_config("/tmp/test_correctness_bugs/alias_collision");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.a (name STRING, val INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.b (name STRING, val INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (name, val) VALUES ('A1', 1);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.b (name, val) VALUES ('B1', 1);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.name AS aname, b.name AS bname\n"
                                           "FROM   t.a a INNER JOIN t.b b ON a.val = b.val;");
        INFO("alias_collision error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);

        int ai = find_column(*cur, "aname");
        int bi = find_column(*cur, "bname");
        REQUIRE(ai >= 0);
        REQUIRE(bi >= 0);
        REQUIRE(ai != bi);
        REQUIRE(cur->value(static_cast<uint64_t>(ai), 0).value<std::string_view>() == "A1");
        REQUIRE(cur->value(static_cast<uint64_t>(bi), 0).value<std::string_view>() == "B1");
    }
}

TEST_CASE("integration::cpp::correctness_bugs::star_prefix") {
    SECTION("table-qualified star") {
        auto config = test_create_config("/tmp/test_correctness_bugs/star_prefix_table");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.x (id INT, a STRING, b STRING);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.y (id INT, c STRING);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.x (id, a, b) VALUES (1,'a','b');")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.y (id, c) VALUES (1,'c');")->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT t.x.* FROM t.x INNER JOIN t.y ON t.x.id=t.y.id;");
            INFO("table-qualified star error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->column_count() == 3);

            int id_i = find_column(*cur, "id");
            int a_i = find_column(*cur, "a");
            int b_i = find_column(*cur, "b");
            REQUIRE(id_i >= 0);
            REQUIRE(a_i >= 0);
            REQUIRE(b_i >= 0);
            REQUIRE(cur->value(static_cast<uint64_t>(id_i), 0).value<int32_t>() == 1);
            REQUIRE(cur->value(static_cast<uint64_t>(a_i), 0).value<std::string_view>() == "a");
            REQUIRE(cur->value(static_cast<uint64_t>(b_i), 0).value<std::string_view>() == "b");
        }
    }

    SECTION("struct field wildcard (out of scope, must error)") {
        auto config = test_create_config("/tmp/test_correctness_bugs/star_prefix_struct");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE p_t AS (px INT, py INT);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.s (id INT, p p_t);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.s (id, p) VALUES (1, ROW(10,20));")->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT (s.p).* FROM t.s s;");
            INFO("struct.* error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == core::error_code_t::unimplemented_yet);
        }
    }
}

TEST_CASE("integration::cpp::correctness_bugs::count_case_no_else") {
    auto config = test_create_config("/tmp/test_correctness_bugs/count_case_no_else");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.x (status STRING);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher
                ->execute_sql(session,
                              "INSERT INTO t.x (status) VALUES ('paid'),('paid'),('paid'),('cancelled'),('cancelled');")
                ->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(CASE WHEN status='paid' THEN 1 END) AS n FROM t.x;");
        INFO("COUNT(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 3);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) AS n FROM t.x;");
        INFO("SUM(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_avg_case_no_else") {
    auto config = test_create_config("/tmp/test_correctness_bugs/min_max_avg_case_no_else");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.y (score INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.y (score) VALUES (50),(60),(72),(85);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MIN(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MIN(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 72);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MAX(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int32_t>() == 85);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT AVG(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("AVG(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->value(0, 0);
        const auto& t = v.type();
        if (t.type() == components::types::logical_type::DOUBLE) {
            REQUIRE(core::is_equals(v.value<double>(), 78.5));
        } else if (t.type() == components::types::logical_type::FLOAT) {
            REQUIRE(core::is_equals(v.value<float>(), 78.5f));
        } else {
            REQUIRE(v.value<int64_t>() == 78);
        }
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT MIN(CASE WHEN score >= 70 THEN score ELSE 999999 END) FROM t.y;");
        INFO("baseline MIN(CASE ELSE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // The CASE result type is the common type across its branches: the ELSE literal 999999 is a
        // BIGINT, so THEN score (INT) widens to BIGINT (issue #571 #2 — a wider branch is not truncated).
        auto v = cur->value(0, 0);
        if (v.type().type() == components::types::logical_type::BIGINT) {
            REQUIRE(v.value<int64_t>() == 72);
        } else {
            REQUIRE(v.value<int32_t>() == 72);
        }
    }
}

// A CASE-WHEN whose condition compares a NULL column value used to hit
// evaluate_row_condition's type-mismatch cast branch (the NULL resolves to an
// NA-typed value), where cast_as previously threw std::logic_error -> SIGABRT.
// Now cast_as returns an error and the condition is guarded: a NULL operand makes
// the comparison UNKNOWN, so the row falls through to ELSE. The query must succeed.
TEST_CASE("integration::cpp::correctness_bugs::case_condition_null_operand") {
    auto config = test_create_config("/tmp/test_correctness_bugs/case_condition_null_operand");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.z (id INT, score INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        // id=2 has a NULL score -> its CASE condition operand is NULL.
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.z (id, score) VALUES (1, 72), (2, NULL), (3, 50);")
                    ->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        // `score = 72` for the NULL row is UNKNOWN -> ELSE branch. Before the fix this
        // aborted the process; now it succeeds and returns the ELSE value.
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT id, CASE WHEN score = 72 THEN 1 ELSE 0 END AS hit FROM t.z ORDER BY id;");
        INFO("CASE null-operand error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(1, 0).value<int32_t>() == 1); // id=1: 72 = 72 -> 1
        REQUIRE(cur->value(1, 1).value<int32_t>() == 0); // id=2: NULL = 72 -> UNKNOWN -> ELSE 0
        REQUIRE(cur->value(1, 2).value<int32_t>() == 0); // id=3: 50 = 72 -> 0
    }
}

TEST_CASE("integration::cpp::correctness_bugs::enum_scan_predicate") {
    auto config = test_create_config("/tmp/test_correctness_bugs/enum_scan_predicate");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE oddness_t AS ENUM('even','odd');")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.e (n INT, kind oddness_t);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher
                ->execute_sql(session, "INSERT INTO t.e (n, kind) VALUES (1,'odd'),(2,'even'),(3,'odd'),(4,'even');")
                ->is_success());
    }

    SECTION("6a scan-pushed STRING compare to ENUM") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=CAST('even' AS oddness_t);");
        INFO("6a error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6c JOIN baseline") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT a.* FROM t.e a INNER JOIN t.e b ON a.n=b.n WHERE a.kind=CAST('even' AS oddness_t);");
        INFO("6c error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6d invalid ENUM string must error") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=CAST('invalid_xyz' AS oddness_t);");
        INFO("6d error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

    SECTION("6e invalid ENUM string under TRY_CAST is NULL, not an error") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=TRY_CAST('invalid_xyz' AS oddness_t);");
        INFO("6e error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    SECTION("6f TRY_CAST of a valid label still converts") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=TRY_CAST('even' AS oddness_t);");
        INFO("6f error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

// A constraint (CHECK / FK) that errors AFTER the DML operator already appended its
// rows, in AUTOCOMMIT, must leave NO physical trace: the appended (uncommitted) rows
// must be REVERTED, not lingered. Mechanism of the bug being guarded against: the
// insert's await_async_and_resume does the WAL-first storage_append and records the
// append range on the pipeline context; the constraint operator above it (driven
// bottom-up, AFTER the insert) then errors. If the executor's error path skips lifting
// the recorded append range into the result, the autocommit abort tail has nothing to
// revert and the bad row physically lingers (txn_abort alone does not scrub it). These
// tests assert the row is ABSENT after the violation — including the deterministic
// "re-insert the same id succeeds" probe, which is RED if a uniqueness-free physical
// row still sits in the table.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_no_linger") {
    auto config = test_create_config("/tmp/test_correctness_bugs/check_violation_autocommit_no_linger");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        // age is bigint so the CHECK constant compares same-type (mirrors the
        // existing streaming_dml::check_constraint test).
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.acc (id bigint, age bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "ALTER TABLE t.acc ADD CONSTRAINT chk_age CHECK (age > 0);")
                    ->is_success());
    }

    // AUTOCOMMIT INSERT that violates the CHECK (age = -5 fails age > 0). The
    // statement MUST error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, -5);");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

    // The bad row must be ABSENT: it was physically appended before the CHECK ran,
    // and the autocommit abort must have reverted that append.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.acc;");
        INFO("post-violation COUNT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM t.acc WHERE id = 1;");
        INFO("post-violation SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // Deterministic physical-leak probe: a VALID re-insert of the SAME id must
    // succeed and the table must then hold EXACTLY ONE row. If the reverted append
    // had lingered, a full scan / COUNT here would observe the stale row too.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, 42);");
        INFO("valid re-insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::fk_violation_autocommit_no_linger") {
    auto config = test_create_config("/tmp/test_correctness_bugs/fk_violation_autocommit_no_linger");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.parent (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.child (id bigint, parent_id bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "ALTER TABLE t.child ADD CONSTRAINT fk_p "
                                  "FOREIGN KEY (parent_id) REFERENCES t.parent (id);")
                    ->is_success());
    }
    // One parent row (id == 1) exists; a child referencing id == 99 has no parent.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.parent (id, name) VALUES (1, 'p1');")->is_success());
    }

    // AUTOCOMMIT INSERT into the child referencing a missing parent: MUST error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (7, 99);");
        INFO("FK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

    // The child row must be ABSENT (the append must have been reverted on abort).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.child;");
        INFO("post-FK-violation COUNT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM t.child WHERE id = 7;");
        INFO("post-FK-violation SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // Deterministic probe: a VALID insert referencing the existing parent succeeds
    // and the child table then holds exactly one row (the stale FK-violating row,
    // had it lingered, would push the count to 2).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (8, 1);");
        INFO("valid child insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.child;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1);
    }
}

// Deterministic RED/GREEN probe of the PHYSICAL revert. The black-box tests above
// assert the externally-visible "row absent" contract, but MVCC permanently masks the
// leaked row from every SQL read (its insert_id stays >= TRANSACTION_ID_START — a
// pending-txn id that is never lowered to a commit_id and is never reused), so they
// pass even with the leak present. This test observes the FIX MECHANISM directly via
// the DEV_MODE executor counter dml_appends_reverted(): a CHECK/FK violation in
// autocommit appends a row BEFORE the constraint fails, and the executor's failed-
// statement abort path MUST lift that recorded append range and physically revert it
// (storage_revert_appends → row_group_t::revert_append truncates the slot back). Before
// the fix the error path breaks BEFORE the dml_appends lift, so the counter does not
// move and the physical slot lingers — this assertion is RED. After the fix it bumps by
// exactly one per leaked range.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_reverts_physical_append") {
    auto config = test_create_config("/tmp/test_correctness_bugs/check_violation_reverts_physical_append");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.acc (id bigint, age bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "ALTER TABLE t.acc ADD CONSTRAINT chk_age CHECK (age > 0);")
                    ->is_success());
    }

    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, -5);");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();

    // The physically-appended (then constraint-rejected) row's base append range
    // must have been reverted on the abort path. RED before the fix (counter unchanged
    // because the error path skipped the dml_appends lift).
    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
}

TEST_CASE("integration::cpp::correctness_bugs::fk_violation_autocommit_reverts_physical_append") {
    auto config = test_create_config("/tmp/test_correctness_bugs/fk_violation_reverts_physical_append");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.parent (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.child (id bigint, parent_id bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "ALTER TABLE t.child ADD CONSTRAINT fk_p "
                                  "FOREIGN KEY (parent_id) REFERENCES t.parent (id);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.parent (id, name) VALUES (1, 'p1');")->is_success());
    }

    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (7, 99);");
        INFO("FK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();

    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
}

// A scalar aggregate over a COLUMN argument (not count(*)) over an EMPTY table must
// emit COUNT=0 (SUM/MIN/MAX/AVG=NULL), not crash. The global-aggregate empty path
// (operator_group_t::empty_aggregate_result) drives the aggregator over a batch with
// no chunks; operator_func_t::aggregate_batch_impl must not assert resolving the
// column key against a 0-column chunk. This is the deterministic, single-threaded
// reproduction of the integration::cpp::production::concurrent_read_write abort.
TEST_CASE("integration::cpp::correctness_bugs::aggregate_column_arg_empty_table") {
    auto config = test_create_config("/tmp/test_correctness_bugs/aggregate_column_arg_empty_table");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.empty_tbl (id bigint, value bigint);")->is_success());
    }

    SECTION("COUNT(column) over empty table is 0") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM t.empty_tbl;");
        INFO("COUNT(id) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 0);
    }

    SECTION("SUM(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT SUM(value) AS s FROM t.empty_tbl;");
        INFO("SUM(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).is_null());
        // Plan-time type resolution (variant 1): the empty SUM(bigint) result must be a
        // typed BIGINT NULL, not the 0-byte logical_type::NA sentinel (which crashes
        // downstream under gcc -O3). The type is config-invariant, so this also fails on
        // clang before the fix.
        REQUIRE(cur->chunks().front().types()[0].type() == components::types::logical_type::BIGINT);
    }

    SECTION("MIN(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MIN(value) AS m FROM t.empty_tbl;");
        INFO("MIN(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).is_null());
        REQUIRE(cur->chunks().front().types()[0].type() == components::types::logical_type::BIGINT);
    }

    SECTION("MAX(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(value) AS m FROM t.empty_tbl;");
        INFO("MAX(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).is_null());
        REQUIRE(cur->chunks().front().types()[0].type() == components::types::logical_type::BIGINT);
    }
}

// Projection (CASE / COALESCE / arithmetic) over an EMPTY table must yield 0 rows of a
// correctly-typed column, not an untyped logical_type::NA column (which crashes under
// gcc -O3, same class as the empty-aggregate bug). Type is config-invariant.
TEST_CASE("integration::cpp::correctness_bugs::projection_over_empty_table") {
    auto config = test_create_config("/tmp/test_correctness_bugs/projection_over_empty_table");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.e (id bigint, value bigint);")->is_success());
    }

    auto check_projection = [&](const char* sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO(sql << " error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
        // Over empty input the projection must still expose its (one) typed column, not
        // drop the schema (0 columns) or emit a 0-byte logical_type::NA column.
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->chunks().front().types()[0].type() != components::types::logical_type::NA);
    };

    SECTION("COALESCE over empty table keeps column type") {
        check_projection("SELECT COALESCE(value, value) AS c FROM t.e;");
    }
    SECTION("arithmetic over empty table keeps column type") {
        check_projection("SELECT value + value AS c FROM t.e;");
    }
    // NOTE: `SELECT CASE WHEN ... END FROM <empty>` (searched-CASE, scalar_type::case_when)
    // is routed through the node_group path, not node_select, so it is NOT covered by the
    // no-group projection type resolution and still drops to 0 columns over empty input.
    // Tracked as a follow-up (group-path case_when type resolution + operator) — its
    // red-first test lands with that fix.
}

// `col LIKE NULL` (and NOT LIKE / ILIKE / NOT ILIKE NULL) is UNKNOWN for every row in
// PostgreSQL (three-valued logic; NOT UNKNOWN is still UNKNOWN) -> ZERO rows for BOTH the
// plain and the negated form. The transformer used to feed the NULL pattern's (nullptr)
// string storage straight into like_to_regex and crash the process at transform time.
TEST_CASE("integration::cpp::correctness_bugs::like_null_pattern") {
    auto config = test_create_config("/tmp/test_correctness_bugs/like_null_pattern");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE t;")->is_success());
    REQUIRE(run("CREATE TABLE t.s (id bigint, name string);")->is_success());
    REQUIRE(run("INSERT INTO t.s (id, name) VALUES (1, 'alice'), (2, 'bob');")->is_success());

    INFO("LIKE NULL -> UNKNOWN for every row -> 0 rows");
    {
        auto cur = run("SELECT id FROM t.s WHERE name LIKE NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("NOT LIKE NULL -> NOT UNKNOWN is still UNKNOWN -> 0 rows, not match-everything");
    {
        auto cur = run("SELECT id FROM t.s WHERE name NOT LIKE NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    INFO("ILIKE NULL and NOT ILIKE NULL -> 0 rows each");
    {
        auto cur = run("SELECT id FROM t.s WHERE name ILIKE NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);

        auto neg = run("SELECT id FROM t.s WHERE name NOT ILIKE NULL;");
        REQUIRE(neg->is_success());
        REQUIRE(neg->size() == 0);
    }
}

// Scalar NOT LIKE / NOT ILIKE must DROP a NULL-subject row (PostgreSQL: `NULL NOT LIKE p`
// is UNKNOWN). The bare union_not(regex) shape flipped the regex's NULL-subject FALSE into
// TRUE and kept the row; the scalar negated form now carries the same is_not_null guard
// the negated ANY/ALL forms already had (one canonical shape, disk pushdown included —
// this test runs with disk on so the guarded filter goes through the storage scan).
TEST_CASE("integration::cpp::correctness_bugs::scalar_not_like_null_subject") {
    auto config = test_create_config("/tmp/test_correctness_bugs/scalar_not_like_null_subject");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE t;")->is_success());
    REQUIRE(run("CREATE TABLE t.s (id bigint, name string);")->is_success());
    REQUIRE(run("INSERT INTO t.s (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, NULL);")->is_success());

    INFO("NOT ILIKE drops the NULL-subject row: only 'bob' survives 'a%'");
    {
        auto cur = run("SELECT id FROM t.s WHERE name NOT ILIKE 'a%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
    }
    INFO("NOT LIKE drops the NULL-subject row (case-sensitive: alice + bob survive 'A%')");
    {
        auto cur = run("SELECT id FROM t.s WHERE name NOT LIKE 'A%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    INFO("positive LIKE is untouched by the guard: only 'alice' matches 'a%'");
    {
        auto cur = run("SELECT id FROM t.s WHERE name LIKE 'a%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
}

// A DECIMAL operand coerced to the other side's integer type in a comparison must be DESCALED
// (round, overflow -> NULL/unknown), never handed over as the raw scaled storage payload.
// logical_value_t::cast_as's raw-numeric branch used to fire for a DECIMAL SOURCE too and
// static_cast the scaled payload (NUMERIC(10,2) 3.00 -> 300; 100000.00 wraps int16 to -27008),
// leaving the dedicated descaling DECIMAL->numeric branch unreachable. Every comparator funnels
// through cast_as — simple_predicate's bidirectional coercion AND the pushed col-vs-col scan
// filter — so `a < b` over (SMALLINT, NUMERIC) compared garbage on both storage modes. Values
// are pinned, not counts: the wrong and the right row set both have 2 rows for `a < b`, and
// 1 row for `a > b`.
TEST_CASE("integration::cpp::correctness_bugs::decimal_operand_comparison_descale") {
    auto config = test_create_config("/tmp/test_correctness_bugs/decimal_operand_comparison_descale");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto run = [&](const std::string& sql) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(run("CREATE DATABASE t;")->is_success());
    REQUIRE(run("CREATE TABLE t.deccmp (a smallint, b numeric(10, 2));")->is_success());
    REQUIRE(run("INSERT INTO t.deccmp (a, b) VALUES (5, 3.00), (5, 100000.00), (40, 41.25);")->is_success());

    // NUMERIC(10,2) is INT64-backed; the cursor exposes the scaled payload (value * 100),
    // which pins WHICH physical row came back.
    constexpr int64_t payload_3_00 = 300;
    constexpr int64_t payload_41_25 = 4125;
    constexpr int64_t payload_100000_00 = 10000000;

    INFO("a < b matches (5, 100000.00) and (40, 41.25) but NOT (5, 3.00)");
    {
        auto cur = run("SELECT a, b FROM t.deccmp WHERE a < b ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int16_t>() == 5);
        REQUIRE(cur->value(1, 0).type().type() == components::types::logical_type::DECIMAL);
        REQUIRE(cur->value(1, 0).value<int64_t>() == payload_100000_00);
        REQUIRE(cur->value(0, 1).value<int16_t>() == 40);
        REQUIRE(cur->value(1, 1).value<int64_t>() == payload_41_25);
    }

    INFO("a > b matches ONLY (5, 3.00): 5 > 100000.00 must be false, never true via the "
         "int16 wraparound of the scaled payload (out-of-range coercion is unknown, not -27008)");
    {
        auto cur = run("SELECT a, b FROM t.deccmp WHERE a > b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int16_t>() == 5);
        REQUIRE(cur->value(1, 0).type().type() == components::types::logical_type::DECIMAL);
        REQUIRE(cur->value(1, 0).value<int64_t>() == payload_3_00);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::having_binds_aggregate_by_arguments") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/having_binds_by_args");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g bigint, v bigint);")->is_success());
    // g=1: three rows, sum(g)=3, sum(v)=300.  g=10: one row, sum(g)=10, sum(v)=1.
    // The two aggregates therefore disagree about which group passes the HAVING.
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.t (g, v) VALUES (1,100),(1,100),(1,100),(10,1);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT g, sum(v) FROM db.t GROUP BY g HAVING sum(g) > 5;");
    REQUIRE(cur->is_success());
    // Matching a HAVING aggregate to a SELECT one by function name alone bound sum(g)
    // to sum(v), which passes g=1 instead.
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::having_aggregate_over_expression") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/having_over_expression");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (g) VALUES (1),(1),(10);")->is_success());

    // An expression argument must resolve to the aggregate SELECT already registered:
    // built as a constant parameter instead, it never matched and a second, broken
    // aggregate was registered for the same expression.
    auto cur = test_helpers::exec(dispatcher, "SELECT g, SUM(g + 0) AS s FROM db.t GROUP BY g HAVING SUM(g + 0) > 5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::update_division_by_zero_errors") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/update_div_zero");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (10);")->is_success());

    // UPDATE computed x/0 through the unguarded kernel and stored a silent NULL over
    // the row; the guarded path errors and leaves the value alone.
    CHECK_FALSE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = x / 0;")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::field_selection_on_subquery_errors") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/field_select_subquery");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TYPE rec_t AS (f INT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.u (r rec_t);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.u (r) VALUES (ROW(1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (id) VALUES (1);")->is_success());

    // The indirection base is a SubLink, not a column reference. Casting it to
    // A_Indirection anyway read garbage and crashed; an unsupported base is an error.
    auto cur = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE ((SELECT r FROM db.u)).f = 1;");
    CHECK_FALSE(cur->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::varchar_and_text_column_types") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/varchar_text_types");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());

    // varchar and text were unmapped builtins; the element type of varchar[] was built
    // without an extension, and the type accessors dereferenced it.
    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.a (v varchar);")->is_success());
    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.c (v text);")->is_success());
    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.d (v varchar[]);")->is_success());
    // CHAR without VARYING arrives as bpchar, which the catalog already seeds as a string
    // type. It stays rejected in a column definition, but now for the true reason: the
    // grammar attaches the implicit length of char(1), and the engine has only unbounded
    // strings. Accepting it would store more than one character where postgres stores one.
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.e (v char[]);")->is_success());
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.f (v char);")->is_success());
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.g (v char(5));")->is_success());
    // A length modifier is not supported, and says so instead of resolving to something else.
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.b (v varchar(5));")->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_over_text") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/min_max_over_text");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.s (t TEXT);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.s (t) VALUES ('banana'), ('apple'), ('cherry');")->is_success());

    // The aggregate switch had no string branch and threw inside a noexcept coroutine,
    // which surfaces as a SIGSEGV rather than as an error.
    auto mn = test_helpers::exec(dispatcher, "SELECT MIN(t) FROM db.s;");
    REQUIRE(mn->is_success());
    CHECK(mn->value(0, 0).value<std::string_view>() == "apple");

    auto mx = test_helpers::exec(dispatcher, "SELECT MAX(t) FROM db.s;");
    REQUIRE(mx->is_success());
    CHECK(mx->value(0, 0).value<std::string_view>() == "cherry");

    // An aggregate that genuinely does not apply to strings is a returned error.
    CHECK_FALSE(test_helpers::exec(dispatcher, "SELECT SUM(t) FROM db.s;")->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::array_subscript_in_expression") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/subscript_in_expression");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (v) VALUES (ARRAY[10,20,30]);")->is_success());

    // Resolving v[2] to the array's flat child dropped the element index, so the
    // expression read element 0 of the row instead of element 2.
    auto cur = test_helpers::exec(dispatcher, "SELECT v[2] + 0 FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_array_subscript") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/order_by_subscript");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT, v INT[3]);")->is_success());
    // Chosen so the two readings disagree: sorting on the real v[2] gives {30,10,20} and
    // the order 2,3,1, while indexing the flat child by row number sees {10,30,0} -> 3,1,2.
    // With v[1] both readings happen to agree, which is why that shape proves nothing.
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.t (id, v) VALUES (1, ARRAY[10,30,0]), (2, ARRAY[20,10,0]), "
                               "(3, ARRAY[30,20,0]);")
                ->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT id FROM db.t ORDER BY v[2] ASC;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    CHECK(cur->value(0, 0).value<int64_t>() == 2);
    CHECK(cur->value(0, 1).value<int64_t>() == 3);
    CHECK(cur->value(0, 2).value<int64_t>() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::three_table_join_qualified_column") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/three_table_join");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.m (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.n (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (k, v) VALUES (1, 10);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.m (k, v) VALUES (1, 20);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.n (k, v) VALUES (1, 30);")->is_success());

    // Three tables share the column name v across two JOIN sides, so a binary side plus
    // the bare name cannot tell them apart: m.v used to resolve to the leftmost table's v.
    auto cur = test_helpers::exec(dispatcher, "SELECT m.v FROM db.l JOIN db.m ON l.k = m.k JOIN db.n ON m.k = n.k;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::correctness_bugs::cross_database_same_table_name_join") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/cross_database_join");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db1;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db2;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db1.t (id BIGINT, a BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db2.t (id BIGINT, b BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db1.t (id, a) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db2.t (id, b) VALUES (1, 111), (3, 333);")->is_success());

    // Both sides answer to the bare relname t, so both resolved LEFT and the ON became
    // always-true, returning the 2x2 cartesian product instead of the single match.
    auto cur = test_helpers::exec(dispatcher, "SELECT * FROM db1.t JOIN db2.t ON db1.t.id = db2.t.id;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::is_null_on_array_subscript") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/is_null_on_subscript");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());

    // The element has no vector of its own, so reading a validity bitmap for it answered
    // for the flat child at this row number instead — and once at() stopped resolving
    // subscripts, dereferenced null.
    auto present = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE v[1] IS NULL;");
    REQUIRE(present->is_success());
    CHECK(present->size() == 0);

    auto absent = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE v[1] IS NOT NULL;");
    REQUIRE(absent->is_success());
    CHECK(absent->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::update_modulo_by_zero_errors") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/update_mod_zero");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (10);")->is_success());

    // Same write path as division: the modulo kernel must not store a silent NULL either.
    CHECK_FALSE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = x % 0;")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_over_text_computing_table") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/min_max_text_computing");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    // A computing (schemaless) table reaches the same aggregate path with a type that is
    // only known per row, which is the second shape the crash was reported on.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.s ();")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.s (t) VALUES ('banana'), ('apple');")->is_success());

    auto mn = test_helpers::exec(dispatcher, "SELECT MIN(t) FROM db.s;");
    REQUIRE(mn->is_success());
    CHECK(mn->value(0, 0).value<std::string_view>() == "apple");
}

TEST_CASE("integration::cpp::correctness_bugs::aggregate_over_array_subscript") {
    auto config = test_helpers::make_test_config("/tmp/test_correctness_bugs/aggregate_over_subscript");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.t (g, v) VALUES (1, ARRAY[1,100,0]), (1, ARRAY[2,200,0]), "
                               "(2, ARRAY[3,300,0]);")
                ->is_success());

    // The aggregate argument dispatch had no case for an indirection, so v[2] was read as
    // a constant and the whole statement failed to parse. Reading the flat child by row
    // number instead of the element would give 101 and 0.
    auto cur = test_helpers::exec(dispatcher, "SELECT g, sum(v[2]) FROM db.t GROUP BY g ORDER BY g ASC;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(1, 0).value<int64_t>() == 300);
    CHECK(cur->value(1, 1).value<int64_t>() == 300);
}
