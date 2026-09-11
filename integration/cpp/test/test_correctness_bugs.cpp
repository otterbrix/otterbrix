#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/operations_helper.hpp>
#include <cstring>
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
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/array_int_slot_width"));
    test_clear_directory(config);
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
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/unsupported_boolean_text_arithmetic"));
    test_clear_directory(config);
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
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -s FROM t.bad_arith;");
        INFO("TEXT unary-minus error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::arithmetics_failure);
    }

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
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/alias_collision"));
    test_clear_directory(config);
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
        auto config = test_create_config(integration_fixture_path("test_correctness_bugs/star_prefix_table"));
        test_clear_directory(config);
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
        auto config = test_create_config(integration_fixture_path("test_correctness_bugs/star_prefix_struct"));
        test_clear_directory(config);
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
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/count_case_no_else"));
    test_clear_directory(config);
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
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/min_max_avg_case_no_else"));
    test_clear_directory(config);
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
        // CASE's common type across branches widens THEN score (INT) to BIGINT via ELSE 999999 (issue #571 #2).
        auto v = cur->value(0, 0);
        if (v.type().type() == components::types::logical_type::BIGINT) {
            REQUIRE(v.value<int64_t>() == 72);
        } else {
            REQUIRE(v.value<int32_t>() == 72);
        }
    }
}

// A NULL CASE-WHEN condition is UNKNOWN, not a crash: cast_as errors, so the row falls to ELSE.
TEST_CASE("integration::cpp::correctness_bugs::case_condition_null_operand") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/case_condition_null_operand"));
    test_clear_directory(config);
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
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.z (id, score) VALUES (1, 72), (2, NULL), (3, 50);")
                    ->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT id, CASE WHEN score = 72 THEN 1 ELSE 0 END AS hit FROM t.z ORDER BY id;");
        INFO("CASE null-operand error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(1, 0).value<int32_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int32_t>() == 0);
        REQUIRE(cur->value(1, 2).value<int32_t>() == 0);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::enum_scan_predicate") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/enum_scan_predicate"));
    test_clear_directory(config);
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

// Constraint checks run AFTER the insert's WAL-first append; a failed autocommit must revert it or the row lingers.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_no_linger") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/check_violation_autocommit_no_linger"));
    test_clear_directory(config);
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

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, -5);");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

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

    // Re-inserting the SAME id proves no leak: a lingering appended row would leave the count at 2, not 1.
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
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/fk_violation_autocommit_no_linger"));
    test_clear_directory(config);
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

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (7, 99);");
        INFO("FK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

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

    // A valid insert referencing the parent proves no leak: a lingering row would push the count to 2, not 1.
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

// MVCC masks a leaked append from SQL reads; this observes the revert via the DEV_MODE counter dml_appends_reverted().
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_reverts_physical_append") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/check_violation_reverts_physical_append"));
    test_clear_directory(config);
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

    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
}

// Issue #551: revert truncated the row count but left the dictionary size, leaking the rejected payload into offsets.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_revert_does_not_leak_string_payload") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/check_violation_string_leak"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.a (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "ALTER TABLE t.a ADD CONSTRAINT chk_id CHECK (id > 0);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (-1, 'REJECTED');");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (2, 'clean');")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const int name_col = find_column(*cur, "name");
        REQUIRE(name_col >= 0);
        auto name = cur->value(static_cast<uint64_t>(name_col), 0);
        INFO("name value: '" << name.value<std::string_view>() << "'");
        REQUIRE(name.value<std::string_view>() == "clean");
    }
}

// Mid-segment #551: revert rolls dictionary size back to the last KEPT offset, not zero (NULL copies the prior offset).
TEST_CASE("integration::cpp::correctness_bugs::check_violation_revert_mid_segment_keeps_prior_strings") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/check_violation_string_leak_mid_segment"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.a (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "ALTER TABLE t.a ADD CONSTRAINT chk_id CHECK (id > 0);")->is_success());
    }

    SECTION("last kept row is a plain string") {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (1, 'keep');")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(
                dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (-1, 'REJECTED');")->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (2, 'clean');")->is_success());
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT name FROM t.a ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        INFO("row0: '" << cur->value(0, 0).value<std::string_view>() << "' row1: '"
                       << cur->value(0, 1).value<std::string_view>() << "'");
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "keep");
        REQUIRE(cur->value(0, 1).value<std::string_view>() == "clean");
    }

    SECTION("last kept row is NULL") {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (1, 'keep');")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id) VALUES (3);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(
                dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (-1, 'REJECTED');")->is_error());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (id, name) VALUES (2, 'clean');")->is_success());
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT name FROM t.a ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        INFO("row0: '" << cur->value(0, 0).value<std::string_view>() << "' row1: '"
                       << cur->value(0, 1).value<std::string_view>() << "'");
        REQUIRE(cur->value(0, 0).value<std::string_view>() == "keep");
        REQUIRE(cur->value(0, 1).value<std::string_view>() == "clean");
        REQUIRE(cur->value(0, 2).is_null());
    }
}

TEST_CASE("integration::cpp::correctness_bugs::fk_violation_autocommit_reverts_physical_append") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/fk_violation_reverts_physical_append"));
    test_clear_directory(config);
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

// Issue #552: a failed DELETE's MVCC stamp is never un-stamped on autocommit abort, leaving the row stuck.
TEST_CASE("integration::cpp::correctness_bugs::fk_rejected_delete_leaves_row_updatable_and_deletable") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/fk_rejected_delete_row_identity"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.p (id bigint, val text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.ch (id bigint, pid bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "ALTER TABLE db.ch ADD CONSTRAINT fk_c "
                                  "FOREIGN KEY (pid) REFERENCES db.p (id);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.p (id, val) VALUES (1, 'p1');")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.ch (id, pid) VALUES (10, 1);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM db.p WHERE id = 1;");
        INFO("FK-violating DELETE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "UPDATE db.p SET val = 'renamed' WHERE id = 1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM db.p;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const int val_col = find_column(*cur, "val");
        REQUIRE(val_col >= 0);
        INFO("val: '" << cur->value(static_cast<uint64_t>(val_col), 0).value<std::string_view>() << "'");
        REQUIRE(cur->value(static_cast<uint64_t>(val_col), 0).value<std::string_view>() == "renamed");
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DELETE FROM db.ch WHERE id = 10;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DELETE FROM db.p WHERE id = 1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM db.p;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// UPDATE variant of #552; NOT NULL is used here since CHECK on the UPDATE path is a known gap that never rejects.
TEST_CASE("integration::cpp::correctness_bugs::constraint_rejected_update_leaves_row_updatable") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/rejected_update_row_identity"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.p (id bigint, val text NOT NULL);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.p (id, val) VALUES (1, 'p1');")->is_success());
    }
    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "UPDATE db.p SET val = NULL, id = 77 WHERE id = 1;");
        INFO("NOT-NULL-violating UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();
    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM db.p;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const int id_col = find_column(*cur, "id");
        REQUIRE(id_col >= 0);
        REQUIRE(cur->value(static_cast<uint64_t>(id_col), 0).value<int64_t>() == 1);
        const int val_col = find_column(*cur, "val");
        REQUIRE(val_col >= 0);
        REQUIRE(!cur->value(static_cast<uint64_t>(val_col), 0).is_null());
        REQUIRE(cur->value(static_cast<uint64_t>(val_col), 0).value<std::string_view>() == "p1");
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "UPDATE db.p SET val = 'renamed' WHERE id = 1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM db.p;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const int val_col = find_column(*cur, "val");
        REQUIRE(val_col >= 0);
        REQUIRE(!cur->value(static_cast<uint64_t>(val_col), 0).is_null());
        INFO("val: '" << cur->value(static_cast<uint64_t>(val_col), 0).value<std::string_view>() << "'");
        REQUIRE(cur->value(static_cast<uint64_t>(val_col), 0).value<std::string_view>() == "renamed");
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DELETE FROM db.p WHERE id = 1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM db.p;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// Repro of production::concurrent_read_write: a COLUMN aggregate (not count(*)) over an EMPTY table must be COUNT=0.
TEST_CASE("integration::cpp::correctness_bugs::aggregate_column_arg_empty_table") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/aggregate_column_arg_empty_table"));
    test_clear_directory(config);
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
        // Empty SUM must return typed BIGINT NULL, not NA (crashes gcc -O3); config-invariant, holds on clang too.
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

// Projection over an EMPTY table must yield a typed 0-row column, not NA (same gcc -O3 crash class); config-invariant.
TEST_CASE("integration::cpp::correctness_bugs::projection_over_empty_table") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/projection_over_empty_table"));
    test_clear_directory(config);
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
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->chunks().front().types()[0].type() != components::types::logical_type::NA);
    };

    SECTION("COALESCE over empty table keeps column type") {
        check_projection("SELECT COALESCE(value, value) AS c FROM t.e;");
    }
    SECTION("arithmetic over empty table keeps column type") {
        check_projection("SELECT value + value AS c FROM t.e;");
    }
    // NOTE: CASE over an empty table via the node_group path still drops to 0 columns; tracked as a follow-up.
}

// LIKE/ILIKE (plain or NOT) against a NULL pattern are UNKNOWN for every row (PG three-valued logic) -> 0 rows.
TEST_CASE("integration::cpp::correctness_bugs::like_null_pattern") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/like_null_pattern"));
    test_clear_directory(config);
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

// NOT LIKE/NOT ILIKE drops a NULL-subject row (NULL NOT LIKE p is UNKNOWN); disk on exercises the pushdown guard too.
TEST_CASE("integration::cpp::correctness_bugs::scalar_not_like_null_subject") {
    auto config = test_create_config(integration_fixture_path("test_correctness_bugs/scalar_not_like_null_subject"));
    test_clear_directory(config);
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

// A DECIMAL operand coerced to the other side's type must be DESCALED, not handed over raw (overflow -> NULL).
// Values are pinned, not counts: both readings give 2 rows for a<b and 1 for a>b, so counts alone wouldn't catch this.
TEST_CASE("integration::cpp::correctness_bugs::decimal_operand_comparison_descale") {
    auto config =
        test_create_config(integration_fixture_path("test_correctness_bugs/decimal_operand_comparison_descale"));
    test_clear_directory(config);
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

    // NUMERIC(10,2) is INT64-backed; the cursor exposes the payload scaled by 100, identifying the physical row.
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
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/having_binds_by_args"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g bigint, v bigint);")->is_success());
    // Chosen so sum(g) and sum(v) disagree on which group passes the HAVING.
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.t (g, v) VALUES (1,100),(1,100),(1,100),(10,1);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT g, sum(v) FROM db.t GROUP BY g HAVING sum(g) > 5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::having_aggregate_over_expression") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/having_over_expression"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (g) VALUES (1),(1),(10);")->is_success());

    // A HAVING expression argument must resolve to the SELECT's already-registered aggregate, not register a new one.
    auto cur = test_helpers::exec(dispatcher, "SELECT g, SUM(g + 0) AS s FROM db.t GROUP BY g HAVING SUM(g + 0) > 5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::update_division_by_zero_errors") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/update_div_zero"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (10);")->is_success());

    CHECK_FALSE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = x / 0;")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::field_selection_on_subquery_errors") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/field_select_subquery"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TYPE rec_t AS (f INT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.u (r rec_t);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.u (r) VALUES (ROW(1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (id) VALUES (1);")->is_success());

    // The indirection base is a SubLink, not a column reference, so field selection on it is unsupported.
    auto cur = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE ((SELECT r FROM db.u)).f = 1;");
    CHECK_FALSE(cur->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::varchar_and_text_column_types") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/varchar_text_types"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());

    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.a (v varchar);")->is_success());
    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.c (v text);")->is_success());
    CHECK(test_helpers::exec(dispatcher, "CREATE TABLE db.d (v varchar[]);")->is_success());
    // CHAR arrives as bpchar with implicit length char(1); rejected since the engine only has unbounded strings.
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.e (v char[]);")->is_success());
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.f (v char);")->is_success());
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.g (v char(5));")->is_success());
    // Length modifiers (e.g. varchar(5)) are unsupported and rejected, not silently resolved to something else.
    CHECK_FALSE(test_helpers::exec(dispatcher, "CREATE TABLE db.b (v varchar(5));")->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_over_text") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/min_max_over_text"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.s (t TEXT);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.s (t) VALUES ('banana'), ('apple'), ('cherry');")->is_success());

    auto mn = test_helpers::exec(dispatcher, "SELECT MIN(t) FROM db.s;");
    REQUIRE(mn->is_success());
    CHECK(mn->value(0, 0).value<std::string_view>() == "apple");

    auto mx = test_helpers::exec(dispatcher, "SELECT MAX(t) FROM db.s;");
    REQUIRE(mx->is_success());
    CHECK(mx->value(0, 0).value<std::string_view>() == "cherry");

    CHECK_FALSE(test_helpers::exec(dispatcher, "SELECT SUM(t) FROM db.s;")->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::array_subscript_in_expression") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/subscript_in_expression"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (v) VALUES (ARRAY[10,20,30]);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT v[2] + 0 FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_array_subscript") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/order_by_subscript"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT, v INT[3]);")->is_success());
    // Data is chosen so real v[2] and row-number indexing disagree; v[1] would make them agree and prove nothing.
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

    // The key above always has a value, so it says nothing about the shapes where a subscript has
    // none: a LIST row too short to reach it, a NULL element, or a NULL cell. All three sort as
    // NULL — last for ASC, first for DESC.
    auto order = [&](const std::string& sql) {
        auto rows = test_helpers::exec(dispatcher, sql);
        REQUIRE(rows);
        INFO(sql);
        REQUIRE(rows->is_success());
        std::vector<int64_t> ids;
        for (uint64_t row = 0; row < rows->size(); ++row) {
            ids.push_back(rows->value(0, row).value<int64_t>());
        }
        return ids;
    };

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (id BIGINT, v INT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.l (id, v) VALUES (1, ARRAY[10,30]), (2, ARRAY[20,10]), "
                               "(3, ARRAY[30]), (4, NULL), (5, ARRAY[5,NULL]);")
                ->is_success());
    CHECK(order("SELECT id FROM db.l ORDER BY v[2] ASC;") == std::vector<int64_t>{2, 1, 3, 4, 5});
    CHECK(order("SELECT id FROM db.l ORDER BY v[2] DESC;") == std::vector<int64_t>{3, 4, 5, 1, 2});
    CHECK(order("SELECT id FROM db.l ORDER BY v[1] ASC;") == std::vector<int64_t>{5, 1, 2, 3, 4});

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.a (id BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.a (id, v) VALUES (1, ARRAY[10,30,0]), (2, ARRAY[20,10,0]), "
                               "(3, ARRAY[30,NULL,0]), (4, NULL);")
                ->is_success());
    CHECK(order("SELECT id FROM db.a ORDER BY v[2] ASC;") == std::vector<int64_t>{2, 1, 3, 4});
    CHECK(order("SELECT id FROM db.a ORDER BY v[2] DESC;") == std::vector<int64_t>{3, 4, 1, 2});
    // An explicit NULLS FIRST overrides the ASC default.
    CHECK(order("SELECT id FROM db.a ORDER BY v[2] ASC NULLS FIRST;") == std::vector<int64_t>{3, 4, 2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::three_table_join_qualified_column") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/three_table_join"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.m (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.n (k bigint, v bigint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (k, v) VALUES (1, 10);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.m (k, v) VALUES (1, 20);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.n (k, v) VALUES (1, 30);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT m.v FROM db.l JOIN db.m ON l.k = m.k JOIN db.n ON m.k = n.k;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::correctness_bugs::cross_database_same_table_name_join") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/cross_database_join"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db1;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db2;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db1.t (id BIGINT, a BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db2.t (id BIGINT, b BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db1.t (id, a) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db2.t (id, b) VALUES (1, 111), (3, 333);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT * FROM db1.t JOIN db2.t ON db1.t.id = db2.t.id;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::is_null_on_array_subscript") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/is_null_on_subscript"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (id, v) VALUES (1, ARRAY[10,20,30]);")->is_success());

    auto present = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE v[1] IS NULL;");
    REQUIRE(present->is_success());
    CHECK(present->size() == 0);

    auto absent = test_helpers::exec(dispatcher, "SELECT id FROM db.t WHERE v[1] IS NOT NULL;");
    REQUIRE(absent->is_success());
    CHECK(absent->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::update_modulo_by_zero_errors") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/update_mod_zero"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (10);")->is_success());

    CHECK_FALSE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = x % 0;")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK_FALSE(cur->value(0, 0).is_null());
    CHECK(cur->value(0, 0).value<int64_t>() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_over_text_computing_table") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/min_max_text_computing"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    // A computing (schemaless) table reaches the same aggregate path, but its column type is known only per row.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.s ();")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.s (t) VALUES ('banana'), ('apple');")->is_success());

    auto mn = test_helpers::exec(dispatcher, "SELECT MIN(t) FROM db.s;");
    REQUIRE(mn->is_success());
    CHECK(mn->value(0, 0).value<std::string_view>() == "apple");
}

TEST_CASE("integration::cpp::correctness_bugs::aggregate_over_array_subscript") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/aggregate_over_subscript"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (g BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.t (g, v) VALUES (1, ARRAY[1,100,0]), (1, ARRAY[2,200,0]), "
                               "(2, ARRAY[3,300,0]);")
                ->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT g, sum(v[2]) FROM db.t GROUP BY g ORDER BY g ASC;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(cur->value(1, 0).value<int64_t>() == 300);
    CHECK(cur->value(1, 1).value<int64_t>() == 300);
}

TEST_CASE("integration::cpp::correctness_bugs::cross_signed_128bit_comparison") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/cross_signed_128"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.small (v UHUGEINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.large (v UHUGEINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.small (v) VALUES (5);")->is_success());
    // parsed values are signed by default
    // 2^127: one past hugeint::max() should be processed as uhugeint without an error
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.large (v) VALUES (170141183460469231731687303715884105728);")
                ->is_success());

    auto matched = test_helpers::exec(dispatcher, "SELECT v FROM db.small WHERE v = 5;");
    REQUIRE(matched->is_success());
    CHECK(matched->size() == 1);
    auto missed = test_helpers::exec(dispatcher, "SELECT v FROM db.small WHERE v = 4;");
    REQUIRE(missed->is_success());
    CHECK(missed->size() == 0);

    // Double avoids the out-of-range failure but makes comparisons non-strict, so integer type wins.
    auto refused =
        test_helpers::exec(dispatcher, "SELECT v FROM db.large WHERE v = 170141183460469231731687303715884105727;");
    CHECK(refused->is_error());
}

TEST_CASE("integration::cpp::correctness_bugs::nested_element_null_assignment") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/nested_element_null"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id BIGINT, arr INT[3], lst INT[]);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.t (id, arr, lst) VALUES (1, ARRAY[10,20,30], ARRAY[10,20,30]);")
            ->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.t (id, arr, lst) VALUES (2, ARRAY[40,50,60], ARRAY[40,50,60]);")
            ->is_success());

    auto element = [&](const char* column, size_t index, int64_t id) {
        auto cur =
            test_helpers::exec(dispatcher,
                               "SELECT " + std::string(column) + " FROM db.t WHERE id = " + std::to_string(id) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur->value(0, 0).children()[index];
    };

    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET arr[1] = NULL WHERE id = 1;")->is_success());
    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET arr[2] = NULL::int4 WHERE id = 2;")->is_success());
    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET lst[3] = CAST(NULL AS int4) WHERE id = 1;")->is_success());

    CHECK(element("arr", 0, 1).is_null());
    CHECK(element("arr", 1, 2).is_null());
    CHECK(element("lst", 2, 1).is_null());

    // Only the addressed element of the addressed row goes NULL.
    CHECK(element("arr", 1, 1).value<int32_t>() == 20);
    CHECK(element("arr", 2, 1).value<int32_t>() == 30);
    CHECK(element("arr", 0, 2).value<int32_t>() == 40);
    CHECK(element("arr", 2, 2).value<int32_t>() == 60);
    CHECK(element("lst", 0, 1).value<int32_t>() == 10);
    CHECK(element("lst", 2, 2).value<int32_t>() == 60);
}

TEST_CASE("integration::cpp::correctness_bugs::operator_spelling_fixity") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/operator_fixity"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (4);")->is_success());

    auto assign = [&](const char* value_expr) {
        auto cur = test_helpers::exec(dispatcher, "UPDATE db.t SET x = " + std::string(value_expr) + ";");
        const bool success = cur->is_success();
        REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = 4;")->is_success());
        return success;
    };
    auto assigned_value = [&](const char* value_expr) {
        REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = " + std::string(value_expr) + ";")->is_success());
        auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto value = cur->value(0, 0).value<int64_t>();
        REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.t SET x = 4;")->is_success());
        return value;
    };

    CHECK(assigned_value("x !") == 24);   // postfix factorial
    CHECK(assigned_value("!! x") == 24);  // prefix factorial
    CHECK(assigned_value("|/ x") == 2);   // prefix sqrt
    CHECK(assigned_value("||/ x") == 2);  // prefix cbrt
    CHECK(assigned_value("@ x") == 4);    // prefix abs
    CHECK(assigned_value("x ^ 2") == 16); // infix pow

    CHECK_FALSE(assign("! x"));    // no prefix '!' -- that spelling is '!!'
    CHECK_FALSE(assign("x !!"));   // no postfix '!!' -- that spelling is '!'
    CHECK_FALSE(assign("x |/"));   // no postfix '|/'
    CHECK_FALSE(assign("x ||/"));  // no postfix '||/'
    CHECK_FALSE(assign("x @"));    // no postfix '@'
    CHECK_FALSE(assign("^ x"));    // no prefix '^'
    CHECK_FALSE(assign("x ^"));    // no postfix '^'
    CHECK_FALSE(assign("x |/ 2")); // no infix '|/'
}

TEST_CASE("integration::cpp::correctness_bugs::operator_spelling_is_its_function_call") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/operator_as_function"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (4);")->is_success());

    // These operators lower to the function they denote, the same in a SELECT target, a predicate, or an UPDATE SET.
    auto projected_double = [&](const std::string& expr) {
        auto cur = test_helpers::exec(dispatcher, "SELECT " + expr + " FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).type().type() == components::types::logical_type::DOUBLE);
        return cur->value(0, 0).value<double>();
    };
    auto projected_bigint = [&](const std::string& expr) {
        auto cur = test_helpers::exec(dispatcher, "SELECT " + expr + " FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).type().type() == components::types::logical_type::BIGINT);
        return cur->value(0, 0).value<int64_t>();
    };
    auto matched_rows = [&](const std::string& predicate) {
        auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t WHERE " + predicate + ";");
        REQUIRE(cur->is_success());
        return cur->size();
    };
    // The two spellings compute the same thing, so doubles must come back bit-identical, not merely close.
    auto same_double = [](double lhs, double rhs) { return std::memcmp(&lhs, &rhs, sizeof(double)) == 0; };

    CHECK(same_double(projected_double("|/ x"), projected_double("sqrt(x)")));
    CHECK(same_double(projected_double("||/ x"), projected_double("cbrt(x)")));
    CHECK(same_double(projected_double("x ^ 2"), projected_double("pow(x, 2)")));
    CHECK(projected_bigint("@ x") == projected_bigint("abs(x)"));
    CHECK(projected_bigint("!! x") == projected_bigint("factorial(x)"));
    CHECK(projected_bigint("x !") == projected_bigint("factorial(x)"));
    CHECK(projected_bigint("!! x") == projected_bigint("x !"));
    CHECK(same_double(projected_double("|/ x AS r"), projected_double("sqrt(x) AS r")));

    CHECK(matched_rows("(|/ x) > 1") == matched_rows("sqrt(x) > 1"));
    CHECK(matched_rows("(!! x) > 20") == matched_rows("factorial(x) > 20"));
    CHECK(matched_rows("(x !) > 20") == matched_rows("factorial(x) > 20"));
    CHECK(same_double(projected_double("x + (|/ x)"), projected_double("x + sqrt(x)")));

    CHECK(test_helpers::exec(dispatcher, "SELECT ! x FROM db.t;")->is_error());
    CHECK(test_helpers::exec(dispatcher, "SELECT x |/ FROM db.t;")->is_error());
    CHECK(test_helpers::exec(dispatcher, "SELECT x FROM db.t WHERE (! x) > 20;")->is_error());
    CHECK(test_helpers::exec(dispatcher, "SELECT x FROM db.t WHERE (x |/) > 1;")->is_error());
}

TEST_CASE("integration::cpp::correctness_bugs::expression_syntax_is_clause_independent") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/clause_independent"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "CREATE TABLE db.t (g BIGINT, x BIGINT, y BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (g, x, y, v) VALUES (1, 4, 2, ARRAY[10,20,30]);")
                ->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.u (k BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.u (k) VALUES (7);")->is_success());

    // Which expressions a clause admits is validation's job; every clause parses via one shared transform_expression.
    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET x = (SELECT max(k) FROM db.u);")->is_success());
    auto after_subquery_set = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(after_subquery_set->is_success());
    REQUIRE(after_subquery_set->size() == 1);
    CHECK(after_subquery_set->value(0, 0).value<int64_t>() == 7);

    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET x = CASE WHEN y > 1 THEN 9 ELSE 0 END;")->is_success());
    auto after_case_set = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(after_case_set->is_success());
    REQUIRE(after_case_set->size() == 1);
    CHECK(after_case_set->value(0, 0).value<int64_t>() == 9);

    auto case_predicate =
        test_helpers::exec(dispatcher, "SELECT g FROM db.t WHERE (CASE WHEN y > 1 THEN 1 ELSE 0 END) = 1;");
    REQUIRE(case_predicate->is_success());
    CHECK(case_predicate->size() == 1);

    auto having_subscript = test_helpers::exec(dispatcher, "SELECT g FROM db.t GROUP BY g HAVING sum(v[2]) > 1;");
    REQUIRE(having_subscript->is_success());
    CHECK(having_subscript->size() == 1);

    auto subquery_projection = test_helpers::exec(dispatcher, "SELECT (SELECT max(k) FROM db.u) FROM db.t;");
    REQUIRE(subquery_projection->is_success());
    REQUIRE(subquery_projection->size() == 1);
    CHECK(subquery_projection->value(0, 0).value<int64_t>() == 7);
}

TEST_CASE("integration::cpp::correctness_bugs::out_of_bounds_subscript_update_extends") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/oob_subscript_update"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (id BIGINT, v INT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (1, ARRAY[10,20]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (2, ARRAY[30,40,50]);")->is_success());

    // Assigning past the end used to skip the row silently while reporting it as updated. It now
    // extends the list, filling the gap between the old end and the new element with NULLs.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[5] = 77 WHERE id = 1;")->is_success());

    auto grown = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 1;");
    REQUIRE(grown->is_success());
    REQUIRE(grown->size() == 1);
    auto value = grown->value(0, 0);
    REQUIRE(value.children().size() == 5);
    CHECK(value.children()[0].value<int32_t>() == 10);
    CHECK(value.children()[1].value<int32_t>() == 20);
    // The gap EXISTS and is NULL — distinct from being absent.
    CHECK(value.children()[2].is_null());
    CHECK(value.children()[3].is_null());
    CHECK_FALSE(value.children()[4].is_null());
    CHECK(value.children()[4].value<int32_t>() == 77);

    // An untouched row keeps its own length: the rebuild re-lays out every row.
    auto untouched = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(untouched->is_success());
    auto other = untouched->value(0, 0);
    REQUIRE(other.children().size() == 3);
    CHECK(other.children()[0].value<int32_t>() == 30);
    CHECK(other.children()[2].value<int32_t>() == 50);

    // An in-range subscript still writes in place, without changing the length.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[2] = 99 WHERE id = 2;")->is_success());
    auto in_range = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(in_range->is_success());
    auto updated = in_range->value(0, 0);
    REQUIRE(updated.children().size() == 3);
    CHECK(updated.children()[0].value<int32_t>() == 30);
    CHECK(updated.children()[1].value<int32_t>() == 99);
    CHECK(updated.children()[2].value<int32_t>() == 50);
}

TEST_CASE("integration::cpp::correctness_bugs::subscript_update_of_null_list_cell") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/subscript_null_cell"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (id BIGINT, v INT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (1, NULL);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (2, NULL);")->is_success());

    // A NULL cell is an empty list: assigning into it materialises the list rather than leaving the
    // row untouched while reporting it as updated.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[3] = 42 WHERE id = 1;")->is_success());
    auto grown = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 1;");
    REQUIRE(grown->is_success());
    REQUIRE(grown->size() == 1);
    auto value = grown->value(0, 0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.children().size() == 3);
    CHECK(value.children()[0].is_null());
    CHECK(value.children()[1].is_null());
    CHECK_FALSE(value.children()[2].is_null());
    CHECK(value.children()[2].value<int32_t>() == 42);

    // The very first element of a NULL cell is the same rule with no gap to fill.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[1] = 7 WHERE id = 2;")->is_success());
    auto single = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(single->is_success());
    auto only = single->value(0, 0);
    REQUIRE_FALSE(only.is_null());
    REQUIRE(only.children().size() == 1);
    CHECK(only.children()[0].value<int32_t>() == 7);
}

TEST_CASE("integration::cpp::correctness_bugs::list_equality_respects_length") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/list_equality_length"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto rows = [&](const std::string& sql) {
        auto cur = test_helpers::exec(dispatcher, sql);
        REQUIRE(cur);
        INFO(sql);
        REQUIRE(cur->is_success());
        return cur->size();
    };

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (id BIGINT, v INT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (1, ARRAY[1,2]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (2, ARRAY[1,2,3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (3, ARRAY[1,2,3,4]);")->is_success());

    // A shared prefix is not equality: length is part of the value. Matching only the front would
    // make every one of these match its longer neighbours.
    CHECK(rows("SELECT id FROM db.l WHERE v = ARRAY[1,2];") == 1);
    CHECK(rows("SELECT id FROM db.l WHERE v = ARRAY[1,2,3];") == 1);
    CHECK(rows("SELECT id FROM db.l WHERE v = ARRAY[1,2,3,4];") == 1);
    // A prefix that matches nothing in full length matches no row at all.
    CHECK(rows("SELECT id FROM db.l WHERE v = ARRAY[1];") == 0);
    CHECK(rows("SELECT id FROM db.l WHERE v = ARRAY[1,2,3,4,5];") == 0);
    CHECK(rows("SELECT id FROM db.l WHERE v <> ARRAY[1,2];") == 2);

    // Ordering falls back to length once the shared prefix is equal.
    CHECK(rows("SELECT id FROM db.l WHERE v < ARRAY[1,2,3];") == 1);
    CHECK(rows("SELECT id FROM db.l WHERE v > ARRAY[1,2,3];") == 1);

    // Column against column, so neither side is a literal.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.pair (a INT[], b INT[]);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.pair (a, b) VALUES (ARRAY[1,2], ARRAY[1,2,3]);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO db.pair (a, b) VALUES (ARRAY[5,6], ARRAY[5,6]);")->is_success());
    CHECK(rows("SELECT a FROM db.pair WHERE a = b;") == 1);
    CHECK(rows("SELECT a FROM db.pair WHERE a <> b;") == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::whole_array_update_over_null_row") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/whole_array_over_null"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (id BIGINT, v INT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.a (id BIGINT, v INT[3]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (id, v) VALUES (1, NULL);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.a (id, v) VALUES (1, NULL);")->is_success());

    // Assigning a whole array over a NULL row must clear the cell's NULL: the elements were written
    // underneath it, so a stale NULL discarded the assignment entirely.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v = ARRAY[1,2,3] WHERE id = 1;")->is_success());
    auto list_row = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 1;");
    REQUIRE(list_row->is_success());
    auto list_value = list_row->value(0, 0);
    REQUIRE_FALSE(list_value.is_null());
    REQUIRE(list_value.children().size() == 3);
    CHECK(list_value.children()[0].value<int32_t>() == 1);
    CHECK(list_value.children()[2].value<int32_t>() == 3);

    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.a SET v = ARRAY[1,2,3] WHERE id = 1;")->is_success());
    auto array_row = test_helpers::exec(dispatcher, "SELECT v FROM db.a WHERE id = 1;");
    REQUIRE(array_row->is_success());
    auto array_value = array_row->value(0, 0);
    REQUIRE_FALSE(array_value.is_null());
    REQUIRE(array_value.children().size() == 3);
    CHECK(array_value.children()[0].value<int32_t>() == 1);
    CHECK(array_value.children()[2].value<int32_t>() == 3);

    // The reverse still works: assigning NULL over a populated row makes the cell NULL again.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v = NULL WHERE id = 1;")->is_success());
    auto cleared_list = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 1;");
    REQUIRE(cleared_list->is_success());
    CHECK(cleared_list->value(0, 0).is_null());

    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.a SET v = NULL WHERE id = 1;")->is_success());
    auto cleared_array = test_helpers::exec(dispatcher, "SELECT v FROM db.a WHERE id = 1;");
    REQUIRE(cleared_array->is_success());
    CHECK(cleared_array->value(0, 0).is_null());
}
