#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/types/types.hpp>
#include <core/date/date_parse.hpp>
#include <core/operations_helper.hpp>
#include <cstring>
#include <services/collection/executor.hpp>
#include <set>
#include <string>
#include <vector>

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
        auto length = cur.list_size(0, 0);
        REQUIRE_FALSE(length.has_error());
        REQUIRE(length.value() == 3);
        auto first = cur.get<Int>(0, 0, {0});
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value() == static_cast<Int>(1));
        auto second = cur.get<Int>(0, 0, {1});
        REQUIRE_FALSE(second.has_error());
        REQUIRE(second.value() == static_cast<Int>(2));
        auto third = cur.get<Int>(0, 0, {2});
        REQUIRE_FALSE(third.has_error());
        REQUIRE(third.value() == static_cast<Int>(3));
    }

    components::types::logical_type column_type(const components::cursor::cursor_t& cur, size_t column) {
        return cur.chunks().front().data[column].type().type();
    }

    bool seed_outer_and_inner(otterbrix::wrapper_dispatcher_t* dispatcher) {
        return test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success() &&
               test_helpers::exec(dispatcher, "CREATE TABLE s.o (id BIGINT);")->is_success() &&
               test_helpers::exec(dispatcher, "INSERT INTO s.o (id) VALUES (NULL), (1), (2);")->is_success() &&
               test_helpers::exec(dispatcher, "CREATE TABLE s.n (k BIGINT, v BIGINT);")->is_success() &&
               test_helpers::exec(dispatcher, "INSERT INTO s.n (k, v) VALUES (1, 100), (2, 200);")->is_success();
    }

    void check_lateral_pairs(const components::cursor::cursor_t& cur) {
        REQUIRE(cur.is_success());
        REQUIRE(cur.size() == 2);
        std::multiset<std::pair<int64_t, int64_t>> rows;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto cell0 = cur.get<int64_t>(0, r);
            REQUIRE_FALSE(cell0.has_error());
            auto cell1 = cur.get<int64_t>(1, r);
            REQUIRE_FALSE(cell1.has_error());
            rows.insert({cell0.value(), cell1.value()});
        }
        CHECK(rows == std::multiset<std::pair<int64_t, int64_t>>{{1, 100}, {2, 200}});
    }

    void check_single_bool(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, bool expected) {
        auto cur = test_helpers::exec(dispatcher, sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 1);
        auto cell2 = cur->get<bool>(0, 0);
        REQUIRE_FALSE(cell2.has_error());
        CHECK(cell2.value() == expected);
    }

    // INSERT INTO <table_and_columns> VALUES (row(0)), ... (row(rows - 1)), in one statement.
    template<typename RowFn>
    bool insert_generated(otterbrix::wrapper_dispatcher_t* dispatcher,
                          const std::string& table_and_columns,
                          int rows,
                          RowFn row) {
        std::string sql = "INSERT INTO " + table_and_columns + " VALUES ";
        for (int i = 0; i < rows; ++i) {
            sql += (i == 0 ? "(" : ", (") + row(i) + ")";
        }
        sql += ";";
        auto cur = test_helpers::exec(dispatcher, sql);
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        return cur->is_success();
    }

    bool run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = test_helpers::exec(dispatcher, sql);
        UNSCOPED_INFO(sql << " -> error: " << (cur->is_error() ? cur->get_error().what : "none"));
        return cur->is_success();
    }

    std::vector<int64_t> first_column_ints(const components::cursor::cursor_t& cur) {
        std::vector<int64_t> ids;
        for (uint64_t r = 0; r < cur.size(); ++r) {
            auto cell3 = cur.get<int64_t>(0, r);
            REQUIRE_FALSE(cell3.has_error());
            ids.push_back(cell3.value());
        }
        return ids;
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
        auto cell4 = cur->get<bool>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value());
        auto cell5 = cur->get<std::string_view>(1, 0);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == "hello");
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
        auto cell6 = cur->get<std::string_view>(static_cast<uint64_t>(ai), 0);
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == "A1");
        auto cell7 = cur->get<std::string_view>(static_cast<uint64_t>(bi), 0);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE(cell7.value() == "B1");
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
            auto cell8 = cur->get<int32_t>(static_cast<uint64_t>(id_i), 0);
            REQUIRE_FALSE(cell8.has_error());
            REQUIRE(cell8.value() == 1);
            auto cell9 = cur->get<std::string_view>(static_cast<uint64_t>(a_i), 0);
            REQUIRE_FALSE(cell9.has_error());
            REQUIRE(cell9.value() == "a");
            auto cell10 = cur->get<std::string_view>(static_cast<uint64_t>(b_i), 0);
            REQUIRE_FALSE(cell10.has_error());
            REQUIRE(cell10.value() == "b");
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
        auto cell11 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == 3);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) AS n FROM t.x;");
        INFO("SUM(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell12 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell12.has_error());
        REQUIRE(cell12.value() == 3);
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
        auto cell13 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell13.has_error());
        REQUIRE(cell13.value() == 72);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MAX(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell14 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell14.has_error());
        REQUIRE(cell14.value() == 85);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT AVG(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("AVG(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        if (cur->type_data()[0].type() == components::types::logical_type::DOUBLE) {
            auto cell162 = cur->get<double>(0, 0);
            REQUIRE_FALSE(cell162.has_error());
            REQUIRE(core::is_equals(cell162.value(), 78.5));
        } else if (cur->type_data()[0].type() == components::types::logical_type::FLOAT) {
            auto cell163 = cur->get<float>(0, 0);
            REQUIRE_FALSE(cell163.has_error());
            REQUIRE(core::is_equals(cell163.value(), 78.5f));
        } else {
            auto cell164 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell164.has_error());
            REQUIRE(cell164.value() == 78);
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
        if (cur->type_data()[0].type() == components::types::logical_type::BIGINT) {
            auto cell165 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell165.has_error());
            REQUIRE(cell165.value() == 72);
        } else {
            auto cell166 = cur->get<int32_t>(0, 0);
            REQUIRE_FALSE(cell166.has_error());
            REQUIRE(cell166.value() == 72);
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
        auto cell15 = cur->get<int32_t>(1, 0);
        REQUIRE_FALSE(cell15.has_error());
        REQUIRE(cell15.value() == 1);
        auto cell16 = cur->get<int32_t>(1, 1);
        REQUIRE_FALSE(cell16.has_error());
        REQUIRE(cell16.value() == 0);
        auto cell17 = cur->get<int32_t>(1, 2);
        REQUIRE_FALSE(cell17.has_error());
        REQUIRE(cell17.value() == 0);
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
        auto cell18 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell18.has_error());
        REQUIRE(cell18.value() == 0);
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
        auto cell19 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell19.has_error());
        REQUIRE(cell19.value() == 1);
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
        auto cell20 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell20.has_error());
        REQUIRE(cell20.value() == 0);
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
        auto cell21 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell21.has_error());
        REQUIRE(cell21.value() == 1);
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
        auto name = cur->get<std::string_view>(static_cast<uint64_t>(name_col), 0);
        REQUIRE_FALSE(name.has_error());
        INFO("name value: '" << name.value() << "'");
        REQUIRE(name.value() == "clean");
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
        auto cell22 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell22.has_error());
        auto cell23 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell23.has_error());
        INFO("row0: '" << cell22.value() << "' row1: '"
                       << cell23.value() << "'");
        auto cell24 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell24.has_error());
        REQUIRE(cell24.value() == "keep");
        auto cell25 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell25.has_error());
        REQUIRE(cell25.value() == "clean");
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
        auto cell26 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell26.has_error());
        auto cell27 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell27.has_error());
        INFO("row0: '" << cell26.value() << "' row1: '"
                       << cell27.value() << "'");
        auto cell28 = cur->get<std::string_view>(0, 0);
        REQUIRE_FALSE(cell28.has_error());
        REQUIRE(cell28.value() == "keep");
        auto cell29 = cur->get<std::string_view>(0, 1);
        REQUIRE_FALSE(cell29.has_error());
        REQUIRE(cell29.value() == "clean");
        auto cell30 = cur->is_null(0, 2);
        REQUIRE_FALSE(cell30.has_error());
        REQUIRE(cell30.value());
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
        auto cell31 = cur->get<std::string_view>(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell31.has_error());
        INFO("val: '" << cell31.value() << "'");
        auto cell32 = cur->get<std::string_view>(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell32.has_error());
        REQUIRE(cell32.value() == "renamed");
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
        auto cell33 = cur->get<int64_t>(static_cast<uint64_t>(id_col), 0);
        REQUIRE_FALSE(cell33.has_error());
        REQUIRE(cell33.value() == 1);
        const int val_col = find_column(*cur, "val");
        REQUIRE(val_col >= 0);
        auto cell34 = cur->is_null(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell34.has_error());
        REQUIRE(!cell34.value());
        auto cell35 = cur->get<std::string_view>(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell35.has_error());
        REQUIRE(cell35.value() == "p1");
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
        auto cell36 = cur->is_null(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell36.has_error());
        REQUIRE(!cell36.value());
        auto cell37 = cur->get<std::string_view>(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell37.has_error());
        INFO("val: '" << cell37.value() << "'");
        auto cell38 = cur->get<std::string_view>(static_cast<uint64_t>(val_col), 0);
        REQUIRE_FALSE(cell38.has_error());
        REQUIRE(cell38.value() == "renamed");
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
        auto cell39 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell39.has_error());
        REQUIRE(cell39.value() == 0);
    }

    SECTION("SUM(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT SUM(value) AS s FROM t.empty_tbl;");
        INFO("SUM(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell40 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell40.has_error());
        REQUIRE(cell40.value());
        // Empty SUM must return typed BIGINT NULL, not NA (crashes gcc -O3); config-invariant, holds on clang too.
        REQUIRE(cur->chunks().front().types()[0].type() == components::types::logical_type::BIGINT);
    }

    SECTION("MIN(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MIN(value) AS m FROM t.empty_tbl;");
        INFO("MIN(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell41 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell41.has_error());
        REQUIRE(cell41.value());
        REQUIRE(cur->chunks().front().types()[0].type() == components::types::logical_type::BIGINT);
    }

    SECTION("MAX(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(value) AS m FROM t.empty_tbl;");
        INFO("MAX(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell42 = cur->is_null(0, 0);
        REQUIRE_FALSE(cell42.has_error());
        REQUIRE(cell42.value());
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
        auto cell43 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell43.has_error());
        REQUIRE(cell43.value() == 2);
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
        auto cell44 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell44.has_error());
        REQUIRE(cell44.value() == 1);
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
        auto cell45 = cur->get<int16_t>(0, 0);
        REQUIRE_FALSE(cell45.has_error());
        REQUIRE(cell45.value() == 5);
        REQUIRE(cur->type_data()[1].type() == components::types::logical_type::DECIMAL);
        auto cell46 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell46.has_error());
        REQUIRE(cell46.value() == payload_100000_00);
        auto cell47 = cur->get<int16_t>(0, 1);
        REQUIRE_FALSE(cell47.has_error());
        REQUIRE(cell47.value() == 40);
        auto cell48 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(cell48.has_error());
        REQUIRE(cell48.value() == payload_41_25);
    }

    INFO("a > b matches ONLY (5, 3.00): 5 > 100000.00 must be false, never true via the "
         "int16 wraparound of the scaled payload (out-of-range coercion is unknown, not -27008)");
    {
        auto cur = run("SELECT a, b FROM t.deccmp WHERE a > b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell49 = cur->get<int16_t>(0, 0);
        REQUIRE_FALSE(cell49.has_error());
        REQUIRE(cell49.value() == 5);
        REQUIRE(cur->type_data()[1].type() == components::types::logical_type::DECIMAL);
        auto cell50 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell50.has_error());
        REQUIRE(cell50.value() == payload_3_00);
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
    auto cell51 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell51.has_error());
    CHECK(cell51.value() == 10);
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
    auto cell52 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell52.has_error());
    CHECK(cell52.value() == 10);
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
    auto cell53 = cur->is_null(0, 0);
    REQUIRE_FALSE(cell53.has_error());
    CHECK_FALSE(cell53.value());
    auto cell54 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell54.has_error());
    CHECK(cell54.value() == 10);
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
    auto cell55 = mn->get<std::string_view>(0, 0);
    REQUIRE_FALSE(cell55.has_error());
    CHECK(cell55.value() == "apple");

    auto mx = test_helpers::exec(dispatcher, "SELECT MAX(t) FROM db.s;");
    REQUIRE(mx->is_success());
    auto cell56 = mx->get<std::string_view>(0, 0);
    REQUIRE_FALSE(cell56.has_error());
    CHECK(cell56.value() == "cherry");

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
    auto cell57 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell57.has_error());
    CHECK(cell57.value() == 20);
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
    auto cell58 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell58.has_error());
    CHECK(cell58.value() == 2);
    auto cell59 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell59.has_error());
    CHECK(cell59.value() == 3);
    auto cell60 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell60.has_error());
    CHECK(cell60.value() == 1);

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
            auto cell61 = rows->get<int64_t>(0, row);
            REQUIRE_FALSE(cell61.has_error());
            ids.push_back(cell61.value());
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
    auto cell62 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell62.has_error());
    CHECK(cell62.value() == 20);
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
    auto cell63 = cur->is_null(0, 0);
    REQUIRE_FALSE(cell63.has_error());
    CHECK_FALSE(cell63.value());
    auto cell64 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell64.has_error());
    CHECK(cell64.value() == 10);
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
    auto cell65 = mn->get<std::string_view>(0, 0);
    REQUIRE_FALSE(cell65.has_error());
    CHECK(cell65.value() == "apple");
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
    auto cell66 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell66.has_error());
    CHECK(cell66.value() == 300);
    auto cell67 = cur->get<int64_t>(1, 1);
    REQUIRE_FALSE(cell67.has_error());
    CHECK(cell67.value() == 300);
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

    auto row_of = [&](const char* column, int64_t id) {
        auto cur =
            test_helpers::exec(dispatcher,
                               "SELECT " + std::string(column) + " FROM db.t WHERE id = " + std::to_string(id) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur;
    };
    auto element_is_null = [&](const char* column, size_t index, int64_t id) {
        auto cur = row_of(column, id);
        auto cell = cur->is_null(0, 0, {index});
        REQUIRE_FALSE(cell.has_error());
        return cell.value();
    };
    auto element_value = [&](const char* column, size_t index, int64_t id) {
        auto cur = row_of(column, id);
        auto cell = cur->get<int32_t>(0, 0, {index});
        REQUIRE_FALSE(cell.has_error());
        return cell.value();
    };

    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET arr[1] = NULL WHERE id = 1;")->is_success());
    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET arr[2] = NULL::int4 WHERE id = 2;")->is_success());
    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET lst[3] = CAST(NULL AS int4) WHERE id = 1;")->is_success());

    CHECK(element_is_null("arr", 0, 1));
    CHECK(element_is_null("arr", 1, 2));
    CHECK(element_is_null("lst", 2, 1));

    // Only the addressed element of the addressed row goes NULL.
    CHECK(element_value("arr", 1, 1) == 20);
    CHECK(element_value("arr", 2, 1) == 30);
    CHECK(element_value("arr", 0, 2) == 40);
    CHECK(element_value("arr", 2, 2) == 60);
    CHECK(element_value("lst", 0, 1) == 10);
    CHECK(element_value("lst", 2, 2) == 60);
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
        auto cell68 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell68.has_error());
        auto value = cell68.value();
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
        REQUIRE(cur->type_data()[0].type() == components::types::logical_type::DOUBLE);
        auto cell69 = cur->get<double>(0, 0);
        REQUIRE_FALSE(cell69.has_error());
        return cell69.value();
    };
    auto projected_bigint = [&](const std::string& expr) {
        auto cur = test_helpers::exec(dispatcher, "SELECT " + expr + " FROM db.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->type_data()[0].type() == components::types::logical_type::BIGINT);
        auto cell70 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell70.has_error());
        return cell70.value();
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
    auto cell71 = after_subquery_set->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell71.has_error());
    CHECK(cell71.value() == 7);

    CHECK(test_helpers::exec(dispatcher, "UPDATE db.t SET x = CASE WHEN y > 1 THEN 9 ELSE 0 END;")->is_success());
    auto after_case_set = test_helpers::exec(dispatcher, "SELECT x FROM db.t;");
    REQUIRE(after_case_set->is_success());
    REQUIRE(after_case_set->size() == 1);
    auto cell72 = after_case_set->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell72.has_error());
    CHECK(cell72.value() == 9);

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
    auto cell73 = subquery_projection->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell73.has_error());
    CHECK(cell73.value() == 7);
}

TEST_CASE("integration::cpp::correctness_bugs::out_of_bounds_subscript_update_extends") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/oob_subscript_update"));
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
    auto cell167 = grown->list_size(0, 0);
    REQUIRE_FALSE(cell167.has_error());
    REQUIRE(cell167.value() == 5);
    auto cell168 = grown->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell168.has_error());
    CHECK(cell168.value() == 10);
    auto cell169 = grown->get<int32_t>(0, 0, {1});
    REQUIRE_FALSE(cell169.has_error());
    CHECK(cell169.value() == 20);
    // The gap EXISTS and is NULL — distinct from being absent.
    auto cell170 = grown->is_null(0, 0, {2});
    REQUIRE_FALSE(cell170.has_error());
    CHECK(cell170.value());
    auto cell171 = grown->is_null(0, 0, {3});
    REQUIRE_FALSE(cell171.has_error());
    CHECK(cell171.value());
    auto cell172 = grown->is_null(0, 0, {4});
    REQUIRE_FALSE(cell172.has_error());
    CHECK_FALSE(cell172.value());
    auto cell173 = grown->get<int32_t>(0, 0, {4});
    REQUIRE_FALSE(cell173.has_error());
    CHECK(cell173.value() == 77);

    // An untouched row keeps its own length: the rebuild re-lays out every row.
    auto untouched = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(untouched->is_success());
    auto cell174 = untouched->list_size(0, 0);
    REQUIRE_FALSE(cell174.has_error());
    REQUIRE(cell174.value() == 3);
    auto cell175 = untouched->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell175.has_error());
    CHECK(cell175.value() == 30);
    auto cell176 = untouched->get<int32_t>(0, 0, {2});
    REQUIRE_FALSE(cell176.has_error());
    CHECK(cell176.value() == 50);

    // An in-range subscript still writes in place, without changing the length.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[2] = 99 WHERE id = 2;")->is_success());
    auto in_range = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(in_range->is_success());
    auto cell177 = in_range->list_size(0, 0);
    REQUIRE_FALSE(cell177.has_error());
    REQUIRE(cell177.value() == 3);
    auto cell178 = in_range->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell178.has_error());
    CHECK(cell178.value() == 30);
    auto cell179 = in_range->get<int32_t>(0, 0, {1});
    REQUIRE_FALSE(cell179.has_error());
    CHECK(cell179.value() == 99);
    auto cell180 = in_range->get<int32_t>(0, 0, {2});
    REQUIRE_FALSE(cell180.has_error());
    CHECK(cell180.value() == 50);
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
    auto cell181 = grown->is_null(0, 0);
    REQUIRE_FALSE(cell181.has_error());
    REQUIRE_FALSE(cell181.value());
    auto cell182 = grown->list_size(0, 0);
    REQUIRE_FALSE(cell182.has_error());
    REQUIRE(cell182.value() == 3);
    auto cell183 = grown->is_null(0, 0, {0});
    REQUIRE_FALSE(cell183.has_error());
    CHECK(cell183.value());
    auto cell184 = grown->is_null(0, 0, {1});
    REQUIRE_FALSE(cell184.has_error());
    CHECK(cell184.value());
    auto cell185 = grown->is_null(0, 0, {2});
    REQUIRE_FALSE(cell185.has_error());
    CHECK_FALSE(cell185.value());
    auto cell186 = grown->get<int32_t>(0, 0, {2});
    REQUIRE_FALSE(cell186.has_error());
    CHECK(cell186.value() == 42);

    // The very first element of a NULL cell is the same rule with no gap to fill.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v[1] = 7 WHERE id = 2;")->is_success());
    auto single = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 2;");
    REQUIRE(single->is_success());
    auto cell187 = single->is_null(0, 0);
    REQUIRE_FALSE(cell187.has_error());
    REQUIRE_FALSE(cell187.value());
    auto cell188 = single->list_size(0, 0);
    REQUIRE_FALSE(cell188.has_error());
    REQUIRE(cell188.value() == 1);
    auto cell189 = single->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell189.has_error());
    CHECK(cell189.value() == 7);
}

TEST_CASE("integration::cpp::correctness_bugs::list_equality_respects_length") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/list_equality_length"));
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
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/whole_array_over_null"));
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
    auto cell190 = list_row->is_null(0, 0);
    REQUIRE_FALSE(cell190.has_error());
    REQUIRE_FALSE(cell190.value());
    auto cell191 = list_row->list_size(0, 0);
    REQUIRE_FALSE(cell191.has_error());
    REQUIRE(cell191.value() == 3);
    auto cell192 = list_row->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell192.has_error());
    CHECK(cell192.value() == 1);
    auto cell193 = list_row->get<int32_t>(0, 0, {2});
    REQUIRE_FALSE(cell193.has_error());
    CHECK(cell193.value() == 3);

    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.a SET v = ARRAY[1,2,3] WHERE id = 1;")->is_success());
    auto array_row = test_helpers::exec(dispatcher, "SELECT v FROM db.a WHERE id = 1;");
    REQUIRE(array_row->is_success());
    auto cell194 = array_row->is_null(0, 0);
    REQUIRE_FALSE(cell194.has_error());
    REQUIRE_FALSE(cell194.value());
    auto cell195 = array_row->list_size(0, 0);
    REQUIRE_FALSE(cell195.has_error());
    REQUIRE(cell195.value() == 3);
    auto cell196 = array_row->get<int32_t>(0, 0, {0});
    REQUIRE_FALSE(cell196.has_error());
    CHECK(cell196.value() == 1);
    auto cell197 = array_row->get<int32_t>(0, 0, {2});
    REQUIRE_FALSE(cell197.has_error());
    CHECK(cell197.value() == 3);

    // The reverse still works: assigning NULL over a populated row makes the cell NULL again.
    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.l SET v = NULL WHERE id = 1;")->is_success());
    auto cleared_list = test_helpers::exec(dispatcher, "SELECT v FROM db.l WHERE id = 1;");
    REQUIRE(cleared_list->is_success());
    auto cell74 = cleared_list->is_null(0, 0);
    REQUIRE_FALSE(cell74.has_error());
    CHECK(cell74.value());

    REQUIRE(test_helpers::exec(dispatcher, "UPDATE db.a SET v = NULL WHERE id = 1;")->is_success());
    auto cleared_array = test_helpers::exec(dispatcher, "SELECT v FROM db.a WHERE id = 1;");
    REQUIRE(cleared_array->is_success());
    auto cell75 = cleared_array->is_null(0, 0);
    REQUIRE_FALSE(cell75.has_error());
    CHECK(cell75.value());
}

TEST_CASE("integration::cpp::correctness_bugs::hugeint_duplicates_collapse") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/hugeint_dedup"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.h (x HUGEINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.g (x HUGEINT);")->is_success());
    // 2^64 and 2^64 + 1 differ only in the low half, 2^70 only in the high half.
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.h (x) VALUES (18446744073709551616), (18446744073709551616), "
                               "(18446744073709551617), (1180591620717411303424), (1180591620717411303424);")
                ->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.g (x) VALUES (18446744073709551616), (1180591620717411303424), "
                               "(1180591620717411303424);")
                ->is_success());

    auto count_distinct = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT x) FROM db.h;");
    REQUIRE(count_distinct->is_success());
    REQUIRE(count_distinct->size() == 1);
    auto cell76 = count_distinct->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell76.has_error());
    CHECK(cell76.value() == 3);

    auto grouped = test_helpers::exec(dispatcher, "SELECT x, COUNT(*) FROM db.h GROUP BY x;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 3);

    auto unioned = test_helpers::exec(dispatcher, "SELECT x FROM db.h UNION SELECT x FROM db.g;");
    REQUIRE(unioned->is_success());
    CHECK(unioned->size() == 3);

    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT x FROM db.h;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 3);
}

TEST_CASE("integration::cpp::correctness_bugs::hugeint_inside_nested_duplicates_collapse") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/hugeint_nested"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (v HUGEINT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.l (v) VALUES (ARRAY[18446744073709551616]), "
                               "(ARRAY[18446744073709551616]), (ARRAY[18446744073709551617]);")
                ->is_success());

    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.l;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 2);

    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.l GROUP BY v;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 2);

    auto unioned = test_helpers::exec(dispatcher, "SELECT v FROM db.l UNION SELECT v FROM db.l;");
    REQUIRE(unioned->is_success());
    CHECK(unioned->size() == 2);

    auto count_distinct = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT v) FROM db.l;");
    REQUIRE(count_distinct->is_success());
    REQUIRE(count_distinct->size() == 1);
    auto cell77 = count_distinct->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell77.has_error());
    CHECK(cell77.value() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::union_keeps_null_and_zero_apart") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_null_zero"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.a (v BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.b (v BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.a (v) VALUES (NULL);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.b (v) VALUES (0);")->is_success());

    auto across = test_helpers::exec(dispatcher, "SELECT v FROM db.a UNION SELECT v FROM db.b;");
    REQUIRE(across->is_success());
    CHECK(across->size() == 2);

    auto reversed = test_helpers::exec(dispatcher, "SELECT v FROM db.b UNION SELECT v FROM db.a;");
    REQUIRE(reversed->is_success());
    CHECK(reversed->size() == 2);

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.both (v BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.both (v) VALUES (NULL), (0), (NULL), (0);")->is_success());
    auto within = test_helpers::exec(dispatcher, "SELECT v FROM db.both UNION SELECT v FROM db.both;");
    REQUIRE(within->is_success());
    CHECK(within->size() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::union_nulls_with_different_payload_collapse") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_null_payload"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.v (a BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.v (a) VALUES (1), (5);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.n (a BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.n (a) VALUES (NULL);")->is_success());

    // The NULL a CASE yields keeps whatever bytes were in the slot, the inserted NULL has payload 0;
    // a NULL is one value whatever bytes sit under it.
    auto mixed =
        test_helpers::exec(dispatcher, "SELECT a FROM db.n UNION SELECT CASE WHEN a = 5 THEN a END FROM db.v;");
    REQUIRE(mixed->is_success());
    CHECK(mixed->size() == 2);

    auto only_nulls = test_helpers::exec(dispatcher,
                                         "SELECT CASE WHEN a = 7 THEN a END FROM db.v UNION "
                                         "SELECT CASE WHEN a = 7 THEN a END FROM db.v;");
    REQUIRE(only_nulls->is_success());
    CHECK(only_nulls->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::hugeint_inside_struct_duplicates_collapse") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/hugeint_struct"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TYPE hp AS (h hugeint);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.s (v hp);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher,
                               "INSERT INTO db.s (v) VALUES (ROW(18446744073709551616)), "
                               "(ROW(18446744073709551616)), (ROW(18446744073709551617));")
                ->is_success());

    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.s;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 2);

    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.s GROUP BY v;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 2);

    auto unioned = test_helpers::exec(dispatcher, "SELECT v FROM db.s UNION SELECT v FROM db.s;");
    REQUIRE(unioned->is_success());
    CHECK(unioned->size() == 2);

    auto count_distinct = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT v) FROM db.s;");
    REQUIRE(count_distinct->is_success());
    REQUIRE(count_distinct->size() == 1);
    auto cell78 = count_distinct->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell78.has_error());
    CHECK(cell78.value() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_narrow_anchor_is_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_narrow_anchor"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT 1::INTEGER AS n UNION ALL "
                                  "SELECT n + 0 FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("recursive query") != std::string::npos);
    CHECK(what.find("column 1 has type integer in non-recursive term but type bigint overall") != std::string::npos);
    CHECK(what.find("Cast the output of the non-recursive term to the correct type.") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_wide_anchor_casts_the_step") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_wide_anchor"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL "
                                  "SELECT (n + 1)::INTEGER FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    CHECK(column_type(*cur, 0) == components::types::logical_type::BIGINT);
    auto cell79 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell79.has_error());
    CHECK(cell79.value() == 1);
    auto cell80 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell80.has_error());
    CHECK(cell80.value() == 2);
    auto cell81 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell81.has_error());
    CHECK(cell81.value() == 3);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_union_collapses_rows_equal_only_after_the_cast") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_union_cast"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT 1::BIGINT AS n UNION "
                                  "SELECT (n % 2 + 1)::INTEGER FROM seq) SELECT n FROM seq ORDER BY n;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(column_type(*cur, 0) == components::types::logical_type::BIGINT);
    auto cell82 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell82.has_error());
    CHECK(cell82.value() == 1);
    auto cell83 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell83.has_error());
    CHECK(cell83.value() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_null_in_the_step_takes_the_anchor_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_null_step"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT 1::BIGINT AS n UNION ALL "
                                  "SELECT NULL FROM seq WHERE n < 2) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    CHECK(column_type(*cur, 0) == components::types::logical_type::BIGINT);
    auto cell84 = cur->is_null(0, 1);
    REQUIRE_FALSE(cell84.has_error());
    CHECK(cell84.value());
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_null_in_the_anchor_against_a_typed_step_is_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_null_anchor"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT NULL AS n UNION ALL "
                                  "SELECT 1::BIGINT FROM seq) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("recursive query") != std::string::npos);
    CHECK(what.find("column 1 has type text in non-recursive term but type bigint overall") != std::string::npos);
    CHECK(what.find("Cast the output of the non-recursive term to the correct type.") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_refuses_decimal_precision_mismatch") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_dec_precision"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec41 (x DECIMAL(4,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec41 (x) VALUES (1.5);")->is_success());

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT x AS n FROM db.t_dec41 UNION ALL "
                                  "SELECT CAST(n AS DECIMAL(18,1)) FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("recursive query") != std::string::npos);
    CHECK(what.find("column 1 has type numeric(4,1) in non-recursive term but type numeric overall") !=
          std::string::npos);
    CHECK(what.find("Cast the output of the non-recursive term to the correct type.") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_refuses_a_narrower_decimal_step") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_dec_narrow"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec181 (x DECIMAL(18,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec181 (x) VALUES (1.5);")->is_success());

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT x AS n FROM db.t_dec181 UNION ALL "
                                  "SELECT CAST(n AS DECIMAL(4,1)) FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("column 1 has type numeric(18,1) in non-recursive term but type numeric overall") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_refuses_an_integer_step_under_a_decimal_anchor") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_dec_int"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec41 (x DECIMAL(4,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec41 (x) VALUES (1.5);")->is_success());

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT x AS n FROM db.t_dec41 UNION ALL "
                                  "SELECT CAST(n AS INTEGER) FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("column 1 has type numeric(4,1) in non-recursive term but type numeric overall") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_refuses_a_step_with_another_column_count") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_width"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL "
                                  "SELECT n, n FROM seq WHERE n < 3) SELECT n FROM seq;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    CHECK(std::string(cur->get_error().what).find("each UNION query must have the same number of columns") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_refuses_a_narrower_decimal_array_step") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_dec_array"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (x DECIMAL(18,1)[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (x) VALUES (ARRAY[1.5]);")->is_success());

    for (const std::string set_op : {"UNION ALL", "UNION"}) {
        auto cur = test_helpers::exec(dispatcher,
                                      "WITH RECURSIVE s AS (SELECT x AS n FROM db.l " + set_op +
                                          " SELECT CAST(n AS DECIMAL(4,1)[]) FROM s WHERE n IS NULL) SELECT n FROM s;");
        INFO(set_op << " error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
        CHECK(std::string(cur->get_error().what)
                  .find("column 1 has type numeric(18,1)[] in non-recursive term but type numeric[] overall") !=
              std::string::npos);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_accepts_an_identical_array_step_and_a_widened_one") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_array_ok"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.l (x DECIMAL(18,1)[], y BIGINT[]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.l (x, y) VALUES (ARRAY[1.5], ARRAY[1]);")->is_success());

    auto same = test_helpers::exec(dispatcher,
                                   "WITH RECURSIVE s AS (SELECT x AS n FROM db.l UNION ALL "
                                   "SELECT n FROM s WHERE n IS NULL) SELECT n FROM s;");
    INFO("identical error: " << (same->is_error() ? same->get_error().what : "none"));
    REQUIRE(same->is_success());

    auto widened = test_helpers::exec(dispatcher,
                                      "WITH RECURSIVE s AS (SELECT y AS n FROM db.l UNION ALL "
                                      "SELECT CAST(n AS INTEGER[]) FROM s WHERE n IS NULL) SELECT n FROM s;");
    INFO("widened error: " << (widened->is_error() ? widened->get_error().what : "none"));
    REQUIRE(widened->is_success());
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_names_the_length_of_a_fixed_array") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_fixed_array"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.f (x INTEGER[2]);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.f (x) VALUES (ARRAY[1, 2]);")->is_success());

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE s AS (SELECT x AS n FROM db.f UNION ALL "
                                  "SELECT CAST(n AS INTEGER[]) FROM s WHERE n IS NULL) SELECT n FROM s;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    CHECK(std::string(cur->get_error().what)
              .find("column 1 has type integer[2] in non-recursive term but type integer[] overall") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::recursive_cte_without_a_common_type_cannot_be_matched") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/rcte_no_common"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "WITH RECURSIVE s AS (SELECT 1::INTEGER AS n UNION ALL "
                                  "SELECT DATE '2020-01-01' FROM s WHERE n < 2) SELECT n FROM s;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    CHECK(std::string(cur->get_error().what).find("UNION types integer and date cannot be matched") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::union_refuses_decimal_precision_mismatch") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_dec_precision"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec41 (x DECIMAL(4,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec181 (y DECIMAL(18,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec41 (x) VALUES (1.5);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec181 (y) VALUES (2.5);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t_dec41 UNION SELECT y FROM db.t_dec181;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("UNION") != std::string::npos);
    CHECK(what.find("type mismatch") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::union_refuses_decimal_scale_mismatch") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_dec_scale"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec101 (x DECIMAL(10,1));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_dec102 (y DECIMAL(10,2));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec101 (x) VALUES (1.5);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_dec102 (y) VALUES (2.25);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t_dec101 UNION SELECT y FROM db.t_dec102;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("UNION") != std::string::npos);
    CHECK(what.find("type mismatch") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::union_refuses_different_enum_types") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_enums"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TYPE mood AS ENUM ('sad', 'ok');")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TYPE size_t_ AS ENUM ('s', 'm');")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_mood (x mood);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t_size (y size_t_);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_mood (x) VALUES ('sad');")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t_size (y) VALUES ('s');")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.t_mood UNION SELECT y FROM db.t_size;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("UNION") != std::string::npos);
    CHECK(what.find("type mismatch") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::union_null_takes_the_decimal_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_null_decimal"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.d (x DECIMAL(10,2));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.e (y DECIMAL(10,2));")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.d (x) VALUES (1.25);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.e (y) VALUES (2.50);")->is_success());

    SECTION("NULL on the left") {
        auto cur = test_helpers::exec(dispatcher, "SELECT NULL UNION SELECT x FROM db.d;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
        CHECK(column_type(*cur, 0) == components::types::logical_type::DECIMAL);
    }
    SECTION("NULL on the right") {
        auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.d UNION SELECT NULL;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
        CHECK(column_type(*cur, 0) == components::types::logical_type::DECIMAL);
    }
    SECTION("equal DECIMAL types") {
        auto cur = test_helpers::exec(dispatcher, "SELECT x FROM db.d UNION SELECT y FROM db.e;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
        CHECK(column_type(*cur, 0) == components::types::logical_type::DECIMAL);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::union_with_null_scalar_subquery_keeps_subquery_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/union_null_subquery"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x INTEGER);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (7);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT (SELECT x FROM db.t WHERE false) UNION SELECT 1::INTEGER;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 2);
    CHECK(column_type(*cur, 0) == components::types::logical_type::INTEGER);

    auto reversed = test_helpers::exec(dispatcher, "SELECT 1::INTEGER UNION SELECT (SELECT x FROM db.t WHERE false);");
    INFO("reversed error: " << (reversed->is_error() ? reversed->get_error().what : "none"));
    REQUIRE(reversed->is_success());
    CHECK(reversed->size() == 2);
    CHECK(column_type(*reversed, 0) == components::types::logical_type::INTEGER);
}

TEST_CASE("integration::cpp::correctness_bugs::values_mixed_types_bigint_and_decimal_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_int_dec"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "VALUES (1), (2.5);");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    CHECK(cur->is_error());
}

TEST_CASE("integration::cpp::correctness_bugs::values_mixed_types_text_and_bigint_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_text_int"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "VALUES ('a'), (1);");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    CHECK(cur->is_error());
}

TEST_CASE("integration::cpp::correctness_bugs::values_null_then_value_takes_the_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_null_first"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "VALUES (NULL), (1);");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell85 = cur->is_null(0, 0);
    REQUIRE_FALSE(cell85.has_error());
    CHECK(cell85.value());
    auto cell86 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell86.has_error());
    CHECK(cell86.value() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::values_value_then_null_takes_the_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_null_last"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "VALUES (1), (NULL);");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell87 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell87.has_error());
    CHECK(cell87.value() == 1);
    auto cell88 = cur->is_null(0, 1);
    REQUIRE_FALSE(cell88.has_error());
    CHECK(cell88.value());
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_out_of_range_for_bigint_is_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/insert_ubigint_wrap"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.b (x BIGINT);")->is_success());

    auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.b (x) VALUES (1), (18446744073709551615::UBIGINT);");
    INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
    CHECK(ins->is_error());

    auto back = test_helpers::exec(dispatcher, "SELECT x FROM db.b;");
    REQUIRE(back->is_success());
    INFO("stored " << back->size() << " rows");
    CHECK(back->size() == 0);
}

TEST_CASE("integration::cpp::correctness_bugs::lateral_first_outer_row_null") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_null_first"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.outer_t (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO s.outer_t (id) VALUES (NULL), (1), (2);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.inner_t (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO s.inner_t (k, v) VALUES (1, 100), (2, 200), (2, 201);")
                ->is_success());

    auto cur = test_helpers::exec(
        dispatcher,
        "SELECT outer_t.id, sub.v FROM s.outer_t, LATERAL (SELECT inner_t.v FROM s.inner_t WHERE inner_t.k = "
        "outer_t.id) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    std::multiset<std::pair<int64_t, int64_t>> rows;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        auto cell89 = cur->get<int64_t>(0, r);
        REQUIRE_FALSE(cell89.has_error());
        auto cell90 = cur->get<int64_t>(1, r);
        REQUIRE_FALSE(cell90.has_error());
        rows.insert({cell89.value(), cell90.value()});
    }
    CHECK(rows == std::multiset<std::pair<int64_t, int64_t>>{{1, 100}, {2, 200}, {2, 201}});

    auto left = test_helpers::exec(
        dispatcher,
        "SELECT outer_t.id, sub.v FROM s.outer_t LEFT JOIN LATERAL (SELECT inner_t.v FROM s.inner_t WHERE "
        "inner_t.k = outer_t.id) sub ON true;");
    INFO("left error: " << (left->is_error() ? left->get_error().what : "none"));
    REQUIRE(left->is_success());
    CHECK(left->size() == 4);
}

TEST_CASE("integration::cpp::correctness_bugs::nested_null_scalar_subquery_keeps_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/nested_null_sub"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x INTEGER);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (7);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT (SELECT (SELECT x FROM db.t WHERE false));");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(column_type(*cur, 0) == components::types::logical_type::INTEGER);
}

TEST_CASE("integration::cpp::correctness_bugs::union_nested_null_subquery_against_text_is_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/null_union_text"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x INTEGER);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (7);")->is_success());

    auto cur = test_helpers::exec(dispatcher, "SELECT (SELECT (SELECT x FROM db.t WHERE false)) UNION SELECT 'a';");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    CHECK(what.find("UNION") != std::string::npos);
    CHECK(what.find("type mismatch") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::union_nested_null_subquery_matches_non_null_form") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/null_union_int"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (x INTEGER);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO db.t (x) VALUES (7);")->is_success());

    auto non_null = test_helpers::exec(dispatcher, "SELECT (SELECT (SELECT x FROM db.t)) UNION SELECT 1;");
    auto null_form = test_helpers::exec(dispatcher, "SELECT (SELECT (SELECT x FROM db.t WHERE false)) UNION SELECT 1;");
    INFO("non-NULL form error: " << (non_null->is_error() ? non_null->get_error().what : "none"));
    INFO("NULL form error: " << (null_form->is_error() ? null_form->get_error().what : "none"));
    REQUIRE(null_form->is_error() == non_null->is_error());
    if (non_null->is_error()) {
        CHECK(std::string(null_form->get_error().what) == std::string(non_null->get_error().what));
    }
}

TEST_CASE("integration::cpp::correctness_bugs::lateral_null_outer_row_through_function") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_null_abs"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur =
        test_helpers::exec(dispatcher,
                           "SELECT o.id, sub.v FROM s.o, LATERAL (SELECT n.v FROM s.n WHERE n.k = abs(o.id)) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    check_lateral_pairs(*cur);
}

TEST_CASE("integration::cpp::correctness_bugs::lateral_null_outer_row_through_arithmetic") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_null_plus"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur =
        test_helpers::exec(dispatcher,
                           "SELECT o.id, sub.v FROM s.o, LATERAL (SELECT n.v FROM s.n WHERE n.k = o.id + 0) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    check_lateral_pairs(*cur);
}

TEST_CASE("integration::cpp::correctness_bugs::left_lateral_null_outer_row_through_arithmetic") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_left_null"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur = test_helpers::exec(
        dispatcher,
        "SELECT o.id, sub.v FROM s.o LEFT JOIN LATERAL (SELECT n.v FROM s.n WHERE n.k = o.id + 0) sub ON true;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    uint64_t null_outer_rows = 0;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        auto cell91 = cur->is_null(0, r);
        REQUIRE_FALSE(cell91.has_error());
        if (cell91.value()) {
            ++null_outer_rows;
            auto cell92 = cur->is_null(1, r);
            REQUIRE_FALSE(cell92.has_error());
            CHECK(cell92.value());
        } else {
            auto cell93 = cur->get<int64_t>(1, r);
            REQUIRE_FALSE(cell93.has_error());
            auto cell94 = cur->get<int64_t>(0, r);
            REQUIRE_FALSE(cell94.has_error());
            CHECK(cell93.value() == cell94.value() * 100);
        }
    }
    CHECK(null_outer_rows == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::null_scalar_subquery_inside_arithmetic") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/null_subquery_plus"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur = test_helpers::exec(dispatcher, "SELECT v FROM s.n WHERE k = (SELECT id FROM s.o WHERE id IS NULL) + 0;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);
}

TEST_CASE("integration::cpp::correctness_bugs::non_null_scalar_subquery_inside_arithmetic") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/subquery_plus"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur = test_helpers::exec(dispatcher, "SELECT v FROM s.n WHERE k = (SELECT id FROM s.o WHERE id = 1) + 0;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell95 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell95.has_error());
    CHECK(cell95.value() == 100);
}

TEST_CASE("integration::cpp::correctness_bugs::list_of_struct_equal_true") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/los_eq_true"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT ARRAY[ROW(1,'a')] = ARRAY[ROW(1,'a')];", true);
}

TEST_CASE("integration::cpp::correctness_bugs::list_of_struct_equal_false") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/los_eq_false"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT ARRAY[ROW(1,'a')] = ARRAY[ROW(1,'b')];", false);
}

TEST_CASE("integration::cpp::correctness_bugs::list_of_struct_not_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/los_ne"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT ARRAY[ROW(1,'a')] <> ARRAY[ROW(1,'b')];", true);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_array_then_null") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ins_arr_null"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.tv (id BIGINT, v VARCHAR[]);")->is_success());
    {
        auto cur = test_helpers::exec(dispatcher, "INSERT INTO s.tv (id, v) VALUES (1, ARRAY['a','b']), (2, NULL);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, v FROM s.tv ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell96 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell96.has_error());
    CHECK(cell96.value() == 1);
    auto cell198 = cur->list_size(1, 0);
    REQUIRE_FALSE(cell198.has_error());
    REQUIRE(cell198.value() == 2);
    auto cell199 = cur->get<std::string_view>(1, 0, {0});
    REQUIRE_FALSE(cell199.has_error());
    CHECK(cell199.value() == "a");
    auto cell200 = cur->get<std::string_view>(1, 0, {1});
    REQUIRE_FALSE(cell200.has_error());
    CHECK(cell200.value() == "b");
    auto cell97 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell97.has_error());
    CHECK(cell97.value() == 2);
    auto cell98 = cur->is_null(1, 1);
    REQUIRE_FALSE(cell98.has_error());
    CHECK(cell98.value());
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_null_then_array") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ins_null_arr"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.tv (id BIGINT, v VARCHAR[]);")->is_success());
    {
        auto cur = test_helpers::exec(dispatcher, "INSERT INTO s.tv (id, v) VALUES (1, NULL), (2, ARRAY['a']);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, v FROM s.tv ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell99 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell99.has_error());
    CHECK(cell99.value() == 1);
    auto cell100 = cur->is_null(1, 0);
    REQUIRE_FALSE(cell100.has_error());
    CHECK(cell100.value());
    auto cell101 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell101.has_error());
    CHECK(cell101.value() == 2);
    auto cell201 = cur->list_size(1, 1);
    REQUIRE_FALSE(cell201.has_error());
    REQUIRE(cell201.value() == 1);
    auto cell202 = cur->get<std::string_view>(1, 1, {0});
    REQUIRE_FALSE(cell202.has_error());
    CHECK(cell202.value() == "a");
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_array_with_null_element") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ins_arr_null_elem"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.tv (id BIGINT, v VARCHAR[]);")->is_success());
    {
        auto cur = test_helpers::exec(dispatcher, "INSERT INTO s.tv (id, v) VALUES (1, ARRAY['a','b','c',NULL]);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, v FROM s.tv ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell203 = cur->list_size(1, 0);
    REQUIRE_FALSE(cell203.has_error());
    REQUIRE(cell203.value() == 4);
    auto cell204 = cur->get<std::string_view>(1, 0, {0});
    REQUIRE_FALSE(cell204.has_error());
    CHECK(cell204.value() == "a");
    auto cell205 = cur->get<std::string_view>(1, 0, {1});
    REQUIRE_FALSE(cell205.has_error());
    CHECK(cell205.value() == "b");
    auto cell206 = cur->get<std::string_view>(1, 0, {2});
    REQUIRE_FALSE(cell206.has_error());
    CHECK(cell206.value() == "c");
    auto cell207 = cur->is_null(1, 0, {3});
    REQUIRE_FALSE(cell207.has_error());
    CHECK(cell207.value());
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_two_arrays_with_null_elements") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ins_arr_null_elems2"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE s;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE s.tv (id BIGINT, v VARCHAR[]);")->is_success());
    {
        auto cur = test_helpers::exec(
            dispatcher,
            "INSERT INTO s.tv (id, v) VALUES (1, ARRAY['a','b','c']), (2, ARRAY['d','e',NULL,'f',NULL]);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, v FROM s.tv ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell208 = cur->list_size(1, 0);
    REQUIRE_FALSE(cell208.has_error());
    REQUIRE(cell208.value() == 3);
    auto cell209 = cur->get<std::string_view>(1, 0, {2});
    REQUIRE_FALSE(cell209.has_error());
    CHECK(cell209.value() == "c");
    auto cell210 = cur->list_size(1, 1);
    REQUIRE_FALSE(cell210.has_error());
    REQUIRE(cell210.value() == 5);
    auto cell211 = cur->get<std::string_view>(1, 1, {0});
    REQUIRE_FALSE(cell211.has_error());
    CHECK(cell211.value() == "d");
    auto cell212 = cur->get<std::string_view>(1, 1, {1});
    REQUIRE_FALSE(cell212.has_error());
    CHECK(cell212.value() == "e");
    auto cell213 = cur->is_null(1, 1, {2});
    REQUIRE_FALSE(cell213.has_error());
    CHECK(cell213.value());
    auto cell214 = cur->get<std::string_view>(1, 1, {3});
    REQUIRE_FALSE(cell214.has_error());
    CHECK(cell214.value() == "f");
    auto cell215 = cur->is_null(1, 1, {4});
    REQUIRE_FALSE(cell215.has_error());
    CHECK(cell215.value());
}

TEST_CASE("integration::cpp::correctness_bugs::count_distinct_hugeint_across_chunks") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/cd_hugeint_chunks"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.h (g BIGINT, x HUGEINT);"));
    // 5000 rows, 50 distinct values that all recur in every chunk of the scan; 50 and 3 are coprime, so
    // every group sees all 50.
    REQUIRE(insert_generated(dispatcher, "db.h (g, x)", 5000, [](int i) {
        return std::to_string(i % 3) + ", 1000000000000000000000" + std::to_string(100 + i % 50);
    }));

    auto total = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT x) FROM db.h;");
    REQUIRE(total->is_success());
    REQUIRE(total->size() == 1);
    auto cell102 = total->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell102.has_error());
    CHECK(cell102.value() == 50);

    auto grouped = test_helpers::exec(dispatcher, "SELECT g, COUNT(DISTINCT x) FROM db.h GROUP BY g;");
    REQUIRE(grouped->is_success());
    REQUIRE(grouped->size() == 3);
    for (uint64_t r = 0; r < grouped->size(); ++r) {
        auto cell103 = grouped->get<int64_t>(1, r);
        REQUIRE_FALSE(cell103.has_error());
        CHECK(cell103.value() == 50);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::count_distinct_varchar_across_chunks") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/cd_varchar_chunks"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.s (g BIGINT, x VARCHAR);"));
    // Longer than any inline string, so the kept values live in the arena of the chunk that first saw them.
    REQUIRE(insert_generated(dispatcher, "db.s (g, x)", 5000, [](int i) {
        return std::to_string(i % 3) + ", 'a_string_that_is_not_inline_" + std::to_string(i % 40) + "'";
    }));

    auto total = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT x) FROM db.s;");
    REQUIRE(total->is_success());
    REQUIRE(total->size() == 1);
    auto cell104 = total->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell104.has_error());
    CHECK(cell104.value() == 40);

    auto grouped = test_helpers::exec(dispatcher, "SELECT g, COUNT(DISTINCT x) FROM db.s GROUP BY g;");
    REQUIRE(grouped->is_success());
    REQUIRE(grouped->size() == 3);
    for (uint64_t r = 0; r < grouped->size(); ++r) {
        auto cell105 = grouped->get<int64_t>(1, r);
        REQUIRE_FALSE(cell105.has_error());
        CHECK(cell105.value() == 40);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::count_distinct_double_zero_and_negative_zero") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/cd_double_zero"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.f (x DOUBLE);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.f (x) VALUES (0.0), (-0.0), (1.5);"));

    auto count_distinct = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT x) FROM db.f;");
    REQUIRE(count_distinct->is_success());
    REQUIRE(count_distinct->size() == 1);
    auto cell106 = count_distinct->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell106.has_error());
    CHECK(cell106.value() == 2);

    auto only_zeros = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT x) FROM db.f WHERE x < 1.0;");
    REQUIRE(only_zeros->is_success());
    REQUIRE(only_zeros->size() == 1);
    auto cell107 = only_zeros->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell107.has_error());
    CHECK(cell107.value() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_hugeint_array") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_hugeint_arr"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.t (id BIGINT, v HUGEINT[]);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.t (id, v) VALUES (3, ARRAY[3]), (1, ARRAY[1]), (2, ARRAY[2]);"));

    auto cur = test_helpers::exec(dispatcher, "SELECT id FROM db.t ORDER BY v;");
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{1, 2, 3});
    auto desc = test_helpers::exec(dispatcher, "SELECT id FROM db.t ORDER BY v DESC;");
    REQUIRE(desc->is_success());
    CHECK(first_column_ints(*desc) == std::vector<int64_t>{3, 2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_decimal_array_with_negatives") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_decimal_arr"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.t (id BIGINT, v DECIMAL(5,1)[]);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.t (id, v) VALUES (1, ARRAY[2.0]), (2, ARRAY[-1.5]);"));

    auto cur = test_helpers::exec(dispatcher, "SELECT id FROM db.t ORDER BY v;");
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::interval_inside_list_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_list_eq"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT ARRAY[INTERVAL '1 day'] = ARRAY[INTERVAL '1 day'];", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_inside_list_not_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_list_ne"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT ARRAY[INTERVAL '1 day'] = ARRAY[INTERVAL '2 days'];", false);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_inside_struct_inside_list_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_struct_eq"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(),
                      "SELECT ARRAY[ROW(INTERVAL '1 day')] = ARRAY[ROW(INTERVAL '1 day')];",
                      true);
}

TEST_CASE("integration::cpp::correctness_bugs::timetz_inside_list_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_list_eq"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(),
                      "SELECT ARRAY[TIMETZ '10:00:00+00'] = ARRAY[TIMETZ '10:00:00+00'];",
                      true);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_interval_array") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_interval_arr"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // A table column cannot hold INTERVAL[], so the rows come from VALUES.
    auto cur = test_helpers::exec(dispatcher,
                                  "SELECT id FROM (VALUES (1, ARRAY[INTERVAL '2 days']), "
                                  "(2, ARRAY[INTERVAL '1 day'])) AS t(id, v) ORDER BY v;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::interval_month_equals_thirty_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_month_30d"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '1 month' = INTERVAL '30 days';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_month_less_than_thirty_one_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_month_lt_31d"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '1 month' < INTERVAL '31 days';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_day_equals_twenty_four_hours") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_day_24h"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '1 day' = INTERVAL '24 hours';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_inside_list_month_equals_thirty_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_list_month_30d"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(),
                      "SELECT ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days'];",
                      true);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_interval_array_follows_the_total") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_interval_total"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // 1 month is 30 days, which is before 40 days; compared month first it would be after.
    auto cur = test_helpers::exec(dispatcher,
                                  "SELECT id FROM (VALUES (1, ARRAY[INTERVAL '40 days']), "
                                  "(2, ARRAY[INTERVAL '1 month'])) AS t(id, v) ORDER BY v;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_interval_follows_the_total") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_interval_top"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "SELECT id FROM (VALUES (1, INTERVAL '40 days'), "
                                  "(2, INTERVAL '1 month')) AS t(id, v) ORDER BY v;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::group_distinct_union_interval_month_and_thirty_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_group_30d"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.i (v INTERVAL);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.i (v) VALUES (INTERVAL '1 month'), (INTERVAL '30 days');"));

    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.i GROUP BY v;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 1);
    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.i;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 1);
    auto count_distinct = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT v) FROM db.i;");
    REQUIRE(count_distinct->is_success());
    REQUIRE(count_distinct->size() == 1);
    auto cell108 = count_distinct->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell108.has_error());
    CHECK(cell108.value() == 1);
    auto unioned = test_helpers::exec(dispatcher, "SELECT v FROM db.i UNION SELECT v FROM db.i;");
    REQUIRE(unioned->is_success());
    CHECK(unioned->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::group_distinct_union_interval_array_month_and_thirty_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_arr_group_30d"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    const std::string rows = "(VALUES (ARRAY[INTERVAL '1 month']), (ARRAY[INTERVAL '30 days'])) AS t(v)";
    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM " + rows + " GROUP BY v;");
    INFO("error: " << (grouped->is_error() ? grouped->get_error().what : "none"));
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 1);
    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM " + rows + ";");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 1);
    auto unioned = test_helpers::exec(dispatcher, "SELECT v FROM " + rows + " UNION SELECT v FROM " + rows + ";");
    REQUIRE(unioned->is_success());
    CHECK(unioned->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::group_distinct_interval_day_and_twenty_four_hours") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_group_24h"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.i (v INTERVAL);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.i (v) VALUES (INTERVAL '1 day'), (INTERVAL '24 hours');"));

    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.i GROUP BY v;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 1);
    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.i;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::timetz_same_instant_other_zone_is_not_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_zone_ne"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT TIMETZ '10:00:00+00' = TIMETZ '13:00:00+03';", false);
}

TEST_CASE("integration::cpp::correctness_bugs::timetz_same_instant_same_zone_is_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_zone_eq"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT TIMETZ '10:00:00+00' = TIMETZ '10:00:00+00';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::timetz_inside_list_other_zone_is_not_equal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_list_zone_ne"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(),
                      "SELECT ARRAY[TIMETZ '10:00:00+00'] = ARRAY[TIMETZ '13:00:00+03'];",
                      false);
}

TEST_CASE("integration::cpp::correctness_bugs::order_by_timetz_instant_then_zone") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ob_timetz_zone"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher,
                                  "SELECT id FROM (VALUES (1, TIMETZ '10:00:00+00'), (2, TIMETZ '13:00:00+03'), "
                                  "(3, TIMETZ '09:00:00+00')) AS t(id, v) ORDER BY v;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    CHECK(first_column_ints(*cur) == std::vector<int64_t>{3, 2, 1});
}

TEST_CASE("integration::cpp::correctness_bugs::group_distinct_timetz_by_instant_and_zone") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_group"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.z (v TIMETZ);"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.s (v TIMETZ);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.z (v) VALUES (TIMETZ '10:00:00+00'), (TIMETZ '13:00:00+03');"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.s (v) VALUES (TIMETZ '10:00:00+00'), (TIMETZ '10:00:00+00');"));

    auto grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.z GROUP BY v;");
    REQUIRE(grouped->is_success());
    CHECK(grouped->size() == 2);
    auto distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.z;");
    REQUIRE(distinct->is_success());
    CHECK(distinct->size() == 2);
    auto same_grouped = test_helpers::exec(dispatcher, "SELECT v, COUNT(*) FROM db.s GROUP BY v;");
    REQUIRE(same_grouped->is_success());
    CHECK(same_grouped->size() == 1);
    auto same_distinct = test_helpers::exec(dispatcher, "SELECT DISTINCT v FROM db.s;");
    REQUIRE(same_distinct->is_success());
    CHECK(same_distinct->size() == 1);
}

TEST_CASE("integration::cpp::correctness_bugs::hash_join_on_interval_month_and_thirty_days") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_join_30d"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.a (id BIGINT, v INTERVAL);"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.b (id BIGINT, v INTERVAL);"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.a (id, v) VALUES (1, INTERVAL '1 month'), (2, INTERVAL '5 days');"));
    REQUIRE(run_ok(dispatcher, "INSERT INTO db.b (id, v) VALUES (10, INTERVAL '30 days'), (20, INTERVAL '6 days');"));

    auto cur = test_helpers::exec(dispatcher, "SELECT a.id, b.id FROM db.a JOIN db.b ON a.v = b.v;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell109 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell109.has_error());
    CHECK(cell109.value() == 1);
    auto cell110 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell110.has_error());
    CHECK(cell110.value() == 10);
}

TEST_CASE("integration::cpp::correctness_bugs::count_distinct_timetz") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ttz_count_distinct"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(run_ok(dispatcher, "CREATE DATABASE db;"));
    REQUIRE(run_ok(dispatcher, "CREATE TABLE db.z (v TIMETZ);"));
    REQUIRE(run_ok(dispatcher,
                   "INSERT INTO db.z (v) VALUES (TIMETZ '10:00:00+00'), (TIMETZ '13:00:00+03'), "
                   "(TIMETZ '10:00:00+00');"));

    auto cur = test_helpers::exec(dispatcher, "SELECT COUNT(DISTINCT v) FROM db.z;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell111 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell111.has_error());
    CHECK(cell111.value() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_huge_months_compare_greater") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_huge_gt"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '300000 years' > INTERVAL '1 day';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_huge_months_equal_themselves") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_huge_eq"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '300000 years' = INTERVAL '300000 years';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_huge_months_compare_with_neighbour") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_huge_neighbour"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '300000 years' > INTERVAL '299999 years';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::interval_huge_days_compare_greater") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/iv_huge_days"));
    test_spaces space(config);
    check_single_bool(space.dispatcher(), "SELECT INTERVAL '200000000 days' > INTERVAL '1 day';", true);
}

TEST_CASE("integration::cpp::correctness_bugs::lateral_reference_to_missing_outer_column_is_refused") {
    auto config =
        test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_missing_outer"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto cur = test_helpers::exec(dispatcher,
                                  "SELECT o.id, sub.v FROM s.o, LATERAL (SELECT n.v FROM s.n WHERE n.k = o.nope) sub;");
    REQUIRE(cur->is_error());
    const std::string what(cur->get_error().what);
    INFO("error: " << what);
    CHECK(what.find("column o.nope does not exist") != std::string::npos);
}

// A LATERAL key is rebound per outer row, so no index lookup may freeze it at plan time.
TEST_CASE("integration::cpp::correctness_bugs::lateral_inner_key_with_an_index_gives_the_same_rows") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/lateral_indexed_key"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO s.n (k, v) VALUES (NULL, 300);")->is_success());

    const std::string comma = "SELECT o.id, l.v FROM s.o o, LATERAL (SELECT v FROM s.n WHERE k = o.id) l;";
    const std::string left =
        "SELECT o.id, l.v FROM s.o o LEFT JOIN LATERAL (SELECT v FROM s.n WHERE k = o.id) l ON true;";
    auto check_both = [&] {
        auto inner = test_helpers::exec(dispatcher, comma);
        INFO("error: " << (inner->is_error() ? inner->get_error().what : "none"));
        check_lateral_pairs(*inner);

        auto outer = test_helpers::exec(dispatcher, left);
        INFO("error: " << (outer->is_error() ? outer->get_error().what : "none"));
        REQUIRE(outer->is_success());
        REQUIRE(outer->size() == 3);
        uint64_t unmatched = 0;
        for (uint64_t r = 0; r < outer->size(); ++r) {
            auto cell112 = outer->is_null(0, r);
            REQUIRE_FALSE(cell112.has_error());
            if (cell112.value()) {
                auto cell113 = outer->is_null(1, r);
                REQUIRE_FALSE(cell113.has_error());
                CHECK(cell113.value());
                ++unmatched;
            }
        }
        CHECK(unmatched == 1);
    };

    check_both();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE INDEX ik ON s.n (k);")->is_success());
    check_both();
}

TEST_CASE("integration::cpp::correctness_bugs::parameter_in_the_projection_list") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/projected_parameter"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(seed_outer_and_inner(dispatcher));

    auto* resource = dispatcher->resource();
    auto bound = dispatcher->execute_sql_with_params(otterbrix::session_id_t(),
                                                     "SELECT k, $1 FROM s.n;",
                                                     {{1, components::vector::make_scalar(resource, int64_t{5})}});
    INFO("error: " << (bound->is_error() ? bound->get_error().what : "none"));
    REQUIRE(bound->is_success());
    REQUIRE(bound->size() == 2);
    REQUIRE(bound->column_count() == 2);
    std::multiset<std::pair<int64_t, int64_t>> rows;
    for (uint64_t r = 0; r < bound->size(); ++r) {
        auto cell114 = bound->get<int64_t>(0, r);
        REQUIRE_FALSE(cell114.has_error());
        auto cell115 = bound->get<int64_t>(1, r);
        REQUIRE_FALSE(cell115.has_error());
        rows.insert({cell114.value(), cell115.value()});
    }
    CHECK(rows == std::multiset<std::pair<int64_t, int64_t>>{{1, 5}, {2, 5}});

    auto literal = test_helpers::exec(dispatcher, "SELECT k, 5 FROM s.n;");
    INFO("error: " << (literal->is_error() ? literal->get_error().what : "none"));
    REQUIRE(literal->is_success());
    REQUIRE(literal->size() == 2);
    REQUIRE(literal->column_count() == 2);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_integer_with_string") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_int"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, n integer);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, n) VALUES (1, 5), (2, '6');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, n FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell116 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell116.has_error());
    CHECK(cell116.value() == 1);
    auto cell117 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell117.has_error());
    CHECK(cell117.value() == 2);
    auto cell118 = cur->get<int32_t>(1, 0);
    REQUIRE_FALSE(cell118.has_error());
    CHECK(cell118.value() == 5);
    auto cell119 = cur->get<int32_t>(1, 1);
    REQUIRE_FALSE(cell119.has_error());
    CHECK(cell119.value() == 6);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_string_then_integer") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_int_rev"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, n integer);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, n) VALUES (1, '6'), (2, 5);");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, n FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell120 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell120.has_error());
    CHECK(cell120.value() == 1);
    auto cell121 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell121.has_error());
    CHECK(cell121.value() == 2);
    auto cell122 = cur->get<int32_t>(1, 0);
    REQUIRE_FALSE(cell122.has_error());
    CHECK(cell122.value() == 6);
    auto cell123 = cur->get<int32_t>(1, 1);
    REQUIRE_FALSE(cell123.has_error());
    CHECK(cell123.value() == 5);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_date_with_string") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_date"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, d date);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, d) VALUES (1, DATE '2024-01-15'), (2, '2024-01-16');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, d FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell124 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell124.has_error());
    CHECK(cell124.value() == 1);
    auto cell125 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell125.has_error());
    CHECK(cell125.value() == 2);
    auto cell126 = cur->get<core::date::date_t>(1, 0);
    REQUIRE_FALSE(cell126.has_error());
    CHECK(cell126.value() == *core::date::parse_date("2024-01-15"));
    auto cell127 = cur->get<core::date::date_t>(1, 1);
    REQUIRE_FALSE(cell127.has_error());
    CHECK(cell127.value() == *core::date::parse_date("2024-01-16"));
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_string_then_date") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_date_rev"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, d date);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, d) VALUES (1, '2024-01-16'), (2, DATE '2024-01-15');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, d FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell128 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell128.has_error());
    CHECK(cell128.value() == 1);
    auto cell129 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell129.has_error());
    CHECK(cell129.value() == 2);
    auto cell130 = cur->get<core::date::date_t>(1, 0);
    REQUIRE_FALSE(cell130.has_error());
    CHECK(cell130.value() == *core::date::parse_date("2024-01-16"));
    auto cell131 = cur->get<core::date::date_t>(1, 1);
    REQUIRE_FALSE(cell131.has_error());
    CHECK(cell131.value() == *core::date::parse_date("2024-01-15"));
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_timestamptz_with_string") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_tz"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, t timestamptz);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, t) VALUES (1, TIMESTAMPTZ '2024-01-15 10:00:00+00'), (2, '2024-01-16 11:00:00+00');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, t FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell132 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell132.has_error());
    CHECK(cell132.value() == 1);
    auto cell133 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell133.has_error());
    CHECK(cell133.value() == 2);
    auto cell134 = cur->get<core::date::timestamptz_t>(1, 0);
    REQUIRE_FALSE(cell134.has_error());
    CHECK(cell134.value() == *core::date::parse_timestamptz("2024-01-15 10:00:00+00"));
    auto cell135 = cur->get<core::date::timestamptz_t>(1, 1);
    REQUIRE_FALSE(cell135.has_error());
    CHECK(cell135.value() == *core::date::parse_timestamptz("2024-01-16 11:00:00+00"));
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_string_then_timestamptz") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_tz_rev"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, t timestamptz);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, t) VALUES (1, '2024-01-16 11:00:00+00'), (2, TIMESTAMPTZ '2024-01-15 10:00:00+00');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, t FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell136 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell136.has_error());
    CHECK(cell136.value() == 1);
    auto cell137 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell137.has_error());
    CHECK(cell137.value() == 2);
    auto cell138 = cur->get<core::date::timestamptz_t>(1, 0);
    REQUIRE_FALSE(cell138.has_error());
    CHECK(cell138.value() == *core::date::parse_timestamptz("2024-01-16 11:00:00+00"));
    auto cell139 = cur->get<core::date::timestamptz_t>(1, 1);
    REQUIRE_FALSE(cell139.has_error());
    CHECK(cell139.value() == *core::date::parse_timestamptz("2024-01-15 10:00:00+00"));
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_text_target") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_text"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, s text);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, s) VALUES (1, 'a'), (2, 5);");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, s FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell140 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell140.has_error());
    CHECK(cell140.value() == 1);
    auto cell141 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell141.has_error());
    CHECK(cell141.value() == 2);
    auto cell142 = cur->get<std::string_view>(1, 0);
    REQUIRE_FALSE(cell142.has_error());
    CHECK(cell142.value() == "a");
    auto cell143 = cur->get<std::string_view>(1, 1);
    REQUIRE_FALSE(cell143.has_error());
    CHECK(cell143.value() == "5");
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_typed_and_quoted_double_target") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_double"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.ev (id bigint, d double precision);")->is_success());
    {
        auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.ev (id, d) VALUES (1, 1), (2, '2.5');");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    auto cur = test_helpers::exec(dispatcher, "SELECT id, d FROM db.ev ORDER BY id;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell144 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell144.has_error());
    CHECK(cell144.value() == 1);
    auto cell145 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell145.has_error());
    CHECK(cell145.value() == 2);
    auto cell146 = cur->get<double>(1, 0);
    REQUIRE_FALSE(cell146.has_error());
    CHECK_THAT(cell146.value(), Catch::Matchers::WithinULP(1.0, 0));
    auto cell147 = cur->get<double>(1, 1);
    REQUIRE_FALSE(cell147.has_error());
    CHECK_THAT(cell147.value(), Catch::Matchers::WithinULP(2.5, 0));
}

TEST_CASE("integration::cpp::correctness_bugs::values_mismatch_error_names_both_types") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_msg"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto cur = test_helpers::exec(dispatcher, "VALUES (1), (2.5);");
    REQUIRE(cur->is_error());
    CHECK(cur->get_error().what.find("VALUES types bigint and double precision cannot be matched") !=
          std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_range_error_names_the_column_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/insert_range_msg"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.b (x BIGINT);")->is_success());
    auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.b (x) VALUES (1), (18446744073709551615::UBIGINT);");
    REQUIRE(ins->is_error());
    CHECK(ins->get_error().what.find("bigint out of range") != std::string::npos);
}

namespace {
    std::string insert_error(const std::string& fixture,
                             const std::string& table_ddl,
                             const std::string& insert_sql,
                             bool* inserted = nullptr) {
        auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/" + fixture));
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, table_ddl)->is_success());
        auto ins = test_helpers::exec(dispatcher, insert_sql);
        if (inserted) {
            *inserted = ins->is_success();
        }
        return ins->is_error() ? std::string(ins->get_error().what) : std::string();
    }

    std::string values_error_with_rows(const std::string& fixture, size_t rows, const std::string& tail) {
        auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/" + fixture));
        test_spaces space(config);
        std::string sql = "VALUES (1)";
        for (size_t i = 1; i < rows; ++i) {
            sql += ", (1)";
        }
        sql += ", " + tail + ";";
        auto cur = test_helpers::exec(space.dispatcher(), sql);
        return cur->is_error() ? std::string(cur->get_error().what) : std::string();
    }
} // namespace

TEST_CASE("integration::cpp::correctness_bugs::insert_values_array_then_empty_array_is_refused") {
    auto error = insert_error("ins_arr_empty1",
                              "CREATE TABLE db.t (a int[]);",
                              "INSERT INTO db.t (a) VALUES (ARRAY[1,2]), (ARRAY[]);");
    INFO("error: " << error);
    CHECK(error.find("cannot determine type of empty array") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_empty_array_then_array_is_refused") {
    auto error = insert_error("ins_arr_empty2",
                              "CREATE TABLE db.t (a int[]);",
                              "INSERT INTO db.t (a) VALUES (ARRAY[]), (ARRAY[1,2]);");
    INFO("error: " << error);
    CHECK(error.find("cannot determine type of empty array") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_lone_empty_array_still_works") {
    bool inserted = false;
    auto error = insert_error("ins_arr_empty3",
                              "CREATE TABLE db.t (a int[]);",
                              "INSERT INTO db.t (a) VALUES (ARRAY[]);",
                              &inserted);
    INFO("error: " << error);
    CHECK(inserted);
}

TEST_CASE("integration::cpp::correctness_bugs::values_text_after_a_full_chunk_is_refused") {
    auto error = values_error_with_rows("values_chunk_text", 1024, "('a')");
    INFO("error: " << error);
    CHECK(error.find("cannot be matched") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::values_decimal_after_a_full_chunk_is_refused") {
    auto error = values_error_with_rows("values_chunk_dec", 1024, "(2.5)");
    INFO("error: " << error);
    CHECK(error.find("cannot be matched") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::values_null_chunk_then_value_has_one_type") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/values_chunk_null"));
    test_spaces space(config);
    std::string sql = "SELECT count(column1), sum(column1), count(*) FROM (VALUES (NULL)";
    for (size_t i = 1; i < 1024; ++i) {
        sql += ", (NULL)";
    }
    sql += ", (1)) AS v;";
    auto cur = test_helpers::exec(space.dispatcher(), sql);
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto cell148 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell148.has_error());
    CHECK(cell148.value() == 1);
    auto cell149 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell149.has_error());
    CHECK(cell149.value() == 1);
    auto cell150 = cur->get<int64_t>(2, 0);
    REQUIRE_FALSE(cell150.has_error());
    CHECK(cell150.value() == 1025);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_typed_then_text_into_text_column") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_typed_text"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id bigint, s text);")->is_success());
    auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.t (id, s) VALUES (1, 5), (2, 'a'::text);");
    INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
    REQUIRE(ins->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT id, s FROM db.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell151 = cur->get<std::string_view>(1, 0);
    REQUIRE_FALSE(cell151.has_error());
    CHECK(cell151.value() == "5");
    auto cell152 = cur->get<std::string_view>(1, 1);
    REQUIRE_FALSE(cell152.has_error());
    CHECK(cell152.value() == "a");
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_quoted_typed_text_into_text_column") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_q_typed_text"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id bigint, s text);")->is_success());
    auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.t (id, s) VALUES (1, 'a'), (2, 5), (3, 'b'::text);");
    INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
    REQUIRE(ins->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT id, s FROM db.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell153 = cur->get<std::string_view>(1, 0);
    REQUIRE_FALSE(cell153.has_error());
    CHECK(cell153.value() == "a");
    auto cell154 = cur->get<std::string_view>(1, 1);
    REQUIRE_FALSE(cell154.has_error());
    CHECK(cell154.value() == "5");
    auto cell155 = cur->get<std::string_view>(1, 2);
    REQUIRE_FALSE(cell155.has_error());
    CHECK(cell155.value() == "b");
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_typed_text_into_integer_column_is_refused") {
    auto error = insert_error("ev_text_int",
                              "CREATE TABLE db.t (n integer);",
                              "INSERT INTO db.t (n) VALUES ('5'::text), (6);");
    INFO("error: " << error);
    CHECK(error.find("column \"n\" is of type integer but expression is of type text") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_integer_widths_into_bigint_column") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_widths"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (n bigint);")->is_success());
    auto ins = test_helpers::exec(dispatcher, "INSERT INTO db.t (n) VALUES (1::int), (2::bigint), (3::smallint);");
    INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
    REQUIRE(ins->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT n FROM db.t ORDER BY n;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell156 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell156.has_error());
    CHECK(cell156.value() == 1);
    auto cell157 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell157.has_error());
    CHECK(cell157.value() == 2);
    auto cell158 = cur->get<int64_t>(0, 2);
    REQUIRE_FALSE(cell158.has_error());
    CHECK(cell158.value() == 3);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_deferred_quoted_text_parses_like_a_lone_one") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_correctness_bugs/ev_padded"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (id bigint, n integer);")->is_success());
    auto lone = test_helpers::exec(dispatcher, "INSERT INTO db.t (id, n) VALUES (1, ' 8 ');");
    INFO("lone error: " << (lone->is_error() ? lone->get_error().what : "none"));
    REQUIRE(lone->is_success());
    auto mixed = test_helpers::exec(dispatcher, "INSERT INTO db.t (id, n) VALUES (2, 1), (3, ' 7 ');");
    INFO("mixed error: " << (mixed->is_error() ? mixed->get_error().what : "none"));
    REQUIRE(mixed->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT id, n FROM db.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell159 = cur->get<int32_t>(1, 0);
    REQUIRE_FALSE(cell159.has_error());
    CHECK(cell159.value() == 8);
    auto cell160 = cur->get<int32_t>(1, 1);
    REQUIRE_FALSE(cell160.has_error());
    CHECK(cell160.value() == 1);
    auto cell161 = cur->get<int32_t>(1, 2);
    REQUIRE_FALSE(cell161.has_error());
    CHECK(cell161.value() == 7);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_deferred_quoted_text_fails_like_a_lone_one") {
    auto lone = insert_error("ev_frac_lone", "CREATE TABLE db.t (n integer);", "INSERT INTO db.t (n) VALUES ('1.5');");
    auto mixed =
        insert_error("ev_frac_mixed", "CREATE TABLE db.t (n integer);", "INSERT INTO db.t (n) VALUES (1), ('1.5');");
    INFO("lone: " << lone);
    INFO("mixed: " << mixed);
    CHECK(!lone.empty());
    CHECK(mixed == lone);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_mixed_into_dynamic_schema_table") {
    bool inserted = false;
    auto error = insert_error("ev_dynamic",
                              "CREATE TABLE db.g ();",
                              "INSERT INTO db.g (s) VALUES ('c'), (5);",
                              &inserted);
    INFO("inserted: " << inserted << " error: " << error);
    CHECK_FALSE(inserted);
    CHECK(error.find("differing types") != std::string::npos);
}

namespace {
    std::string repeated_rows(size_t count, const std::string& row) {
        std::string rows;
        for (size_t i = 0; i < count; ++i) {
            rows += ", " + row;
        }
        return rows;
    }
} // namespace

TEST_CASE("integration::cpp::correctness_bugs::insert_values_null_chunk_then_mixed_chunk_is_refused") {
    auto error = insert_error("ev_chunks_null",
                              "CREATE TABLE db.t (n integer);",
                              "INSERT INTO db.t (n) VALUES (NULL)" + repeated_rows(1023, "(NULL)") + ", (1), ('2');");
    INFO("error: " << error);
    CHECK(error.find("several chunks") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_chunk_types_differ_into_double_is_refused") {
    auto error = insert_error("ev_chunks_dbl",
                              "CREATE TABLE db.t (d double precision);",
                              "INSERT INTO db.t (d) VALUES ('1')" + repeated_rows(1100, "(3)") + ", (2.5);");
    INFO("error: " << error);
    CHECK(error.find("several chunks") != std::string::npos);
}

TEST_CASE("integration::cpp::correctness_bugs::insert_values_chunk_types_differ_into_text_is_refused") {
    auto error = insert_error("ev_chunks_text",
                              "CREATE TABLE db.t (s text);",
                              "INSERT INTO db.t (s) VALUES (1), ('a'::text)" + repeated_rows(1022, "(3)") +
                                  ", ('b'), ('c');");
    INFO("error: " << error);
    CHECK(error.find("several chunks") != std::string::npos);
}
