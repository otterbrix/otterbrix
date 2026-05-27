#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/operations_helper.hpp>

namespace {

    int find_column(const components::cursor::cursor_t& cur, std::string_view name) {
        const auto& chunk = cur.chunk_data();
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
        REQUIRE(cur.chunk_data().column_count() == 1);
        auto v = cur.chunk_data().value(0, 0);
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
        REQUIRE(cur->chunk_data().column_count() == 2);

        int ai = find_column(*cur, "aname");
        int bi = find_column(*cur, "bname");
        REQUIRE(ai >= 0);
        REQUIRE(bi >= 0);
        REQUIRE(ai != bi);
        REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(ai), 0).value<std::string_view>() == "A1");
        REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(bi), 0).value<std::string_view>() == "B1");
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
            REQUIRE(cur->chunk_data().column_count() == 3);

            int id_i = find_column(*cur, "id");
            int a_i = find_column(*cur, "a");
            int b_i = find_column(*cur, "b");
            REQUIRE(id_i >= 0);
            REQUIRE(a_i >= 0);
            REQUIRE(b_i >= 0);
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(id_i), 0).value<int32_t>() == 1);
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(a_i), 0).value<std::string_view>() == "a");
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(b_i), 0).value<std::string_view>() == "b");
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
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 3);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) AS n FROM t.x;");
        INFO("SUM(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3);
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
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 72);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MAX(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 85);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT AVG(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("AVG(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->chunk_data().value(0, 0);
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
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 72);
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
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind='even';");
        INFO("6a error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6b ordinal baseline") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=0;");
        INFO("6b error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6c JOIN baseline") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT a.* FROM t.e a INNER JOIN t.e b ON a.n=b.n WHERE a.kind='even';");
        INFO("6c error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6d invalid ENUM string must error") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind='invalid_xyz';");
        INFO("6d error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
}
