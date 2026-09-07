#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>
#include <unistd.h>

namespace {
    // Pid-qualified so parallel test processes sharing /tmp don't clear and reseed each other's directory.
    std::string cs_fixture_dir(const char* leaf) {
        return integration_fixture_path(std::string("test_computed_schema/") + leaf).string();
    }

    // Regression #622: table-valued jsonb ops in the select list are refused; each keeps its answer as 'correct:'.
    void require_value_position_refusal(const components::cursor::cursor_t_ptr& cur) {
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_success());
        CHECK(std::string(cur->get_error().what).find("is not valid in a value position") != std::string::npos);
    }
} // namespace

// Each INSERT can add new (field_name, type) columns; a repeat name with a different type adds another column.


TEST_CASE("integration::cpp::test_computed_schema::basic_insert_and_select") {
    auto config = test_create_config(cs_fixture_dir("basic"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t1 ();");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t1 (id, name) VALUES (3, 'Charlie');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::evolving_schema") {
    auto config = test_create_config(cs_fixture_dir("evolving"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t2 ();");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id) VALUES (1), (2), (3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id, value) VALUES (4, 100);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->column_count() == 2);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2 WHERE value = 100;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::multitype_select_star") {
    auto config = test_create_config(cs_fixture_dir("multitype_star"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.mt ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mt (id, val) VALUES (1, 1), (2, 2);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mt (id, val) VALUES (3, 'hello');")->is_success());

    SECTION("SELECT * returns both 'val' variants with their own values") {
        auto cur = exec("SELECT * FROM cs_testdb.mt ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 3);

        const auto& chunk = cur->chunks().front();
        int bigint_val = -1, string_val = -1;
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) != "val") {
                continue;
            }
            if (chunk.data[c].type().type() == components::types::logical_type::BIGINT) {
                bigint_val = static_cast<int>(c);
            } else {
                string_val = static_cast<int>(c);
            }
        }
        REQUIRE(bigint_val >= 0);
        REQUIRE(string_val >= 0);

        REQUIRE(chunk.get_value<int64_t>(static_cast<size_t>(bigint_val), 0) == 1);
        REQUIRE(chunk.get_value<int64_t>(static_cast<size_t>(bigint_val), 1) == 2);
        REQUIRE(chunk.value(static_cast<size_t>(bigint_val), 2).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(string_val), 0).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(string_val), 1).is_null());
        REQUIRE(chunk.get_value<std::string_view>(static_cast<size_t>(string_val), 2) == "hello");
    }

    SECTION("an explicit reference to the multi-type name is ambiguous") {
        REQUIRE_FALSE(exec("SELECT val FROM cs_testdb.mt;")->is_success());
    }

    SECTION("an unambiguous column still works") {
        auto cur = exec("SELECT id FROM cs_testdb.mt ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::delete_rows") {
    auto config = test_create_config(cs_fixture_dir("delete"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t3 ();");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t3 (id, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM cs_testdb.t3 WHERE id <= 2;");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 2);
    }
}

// Nested fields flatten: INSERT (a.b, a.c) creates columns 'a/b','a/c', addressed by a chain ending ->>/#>>.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_scalar_navigation") {
    auto config = test_create_config(cs_fixture_dir("jsonb_scalar"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.j ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.j (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);")->is_success());

    SECTION("-> ... ->> resolves a nested leaf to its native value") {
        auto cur = exec("SELECT x, j -> 'a' ->> 'b' AS b FROM cs_testdb.j ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 10);
    }

    SECTION("->> on a top-level field") {
        auto cur = exec("SELECT j ->> 'x' AS xx FROM cs_testdb.j ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 9);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 90);
        REQUIRE(std::string(cur->chunks().front().data[0].type().alias()) == "xx");
    }

    SECTION("#>> with dotted path and PG-array path are equivalent") {
        auto dotted = exec("SELECT j #>> 'a.c' AS c FROM cs_testdb.j ORDER BY x;");
        REQUIRE(dotted->is_success());
        REQUIRE(dotted->value(0, 0).value<int64_t>() == 2);
        REQUIRE(dotted->value(0, 1).value<int64_t>() == 20);

        auto arr = exec("SELECT j #>> '{a,c}' AS c FROM cs_testdb.j ORDER BY x;");
        REQUIRE(arr->is_success());
        REQUIRE(arr->value(0, 0).value<int64_t>() == 2);
        REQUIRE(arr->value(0, 1).value<int64_t>() == 20);
    }

    SECTION("jsonb scalar usable in WHERE") {
        auto cur = exec("SELECT x FROM cs_testdb.j WHERE j -> 'a' ->> 'b' = 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 90);

        auto cur2 = exec("SELECT x FROM cs_testdb.j WHERE j #>> '{a,c}' = 2;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->size() == 1);
        REQUIRE(cur2->value(0, 0).value<int64_t>() == 9);
    }

    SECTION("a chain still ending in -> is refused in the select list (regression)") {
        // correct: j -> 'a' -> 'b' resolves to one column 'b' with 1, 10.
        require_value_position_refusal(exec("SELECT j -> 'a' -> 'b' FROM cs_testdb.j ORDER BY x;"));
    }
}

TEST_CASE("integration::cpp::test_computed_schema::jsonb_exists") {
    auto config = test_create_config(cs_fixture_dir("jsonb_exists"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.qe ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.qe (a.b, x) VALUES (1, 9);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.qe (a.b) VALUES (5);")->is_success());

    SECTION("? matches rows where the key is present and non-null") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ? 'x';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }

    SECTION("?| any-of") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ?| '{x}';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }

    SECTION("?& all-of") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe ?& '{x}';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }

    SECTION("existence on a nested path prefix") {
        auto cur = exec("SELECT qe -> 'a' ->> 'b' AS b FROM cs_testdb.qe WHERE qe -> 'a' ? 'b';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::jsonb_delete") {
    auto config = test_create_config(cs_fixture_dir("jsonb_delete"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.jd ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.jd (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);")->is_success());

    // correct: - 'x' -> {a/b, a/c}; - 'a' -> {x}; #- 'a.b' -> {a/c, x}.
    SECTION("select-list key deletion is refused (regression)") {
        require_value_position_refusal(exec("SELECT jd - 'x' FROM cs_testdb.jd;"));
        require_value_position_refusal(exec("SELECT jd - 'a' FROM cs_testdb.jd;"));
        require_value_position_refusal(exec("SELECT jd #- 'a.b' FROM cs_testdb.jd;"));
    }
}

// `SELECT * FROM t -> 'a'` is intentionally unsupported; it would need a parser/grammar change.
TEST_CASE("integration::cpp::test_computed_schema::jsonb_expand") {
    auto config = test_create_config(cs_fixture_dir("jsonb_expand"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.je ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.je (a.b, a.c, x) VALUES (1, 2, 9);")->is_success());

    // correct: -> 'a' expands to {b, c}; #> 'a' the same; -> 'a' -> 'c' yields column 'c' = 2.
    SECTION("select-list expansion is refused (regression)") {
        require_value_position_refusal(exec("SELECT je -> 'a' FROM cs_testdb.je;"));
        require_value_position_refusal(exec("SELECT je #> 'a' FROM cs_testdb.je;"));
        require_value_position_refusal(exec("SELECT je -> 'a' -> 'c' FROM cs_testdb.je;"));
    }
}

TEST_CASE("integration::cpp::test_computed_schema::multitype_variant_select") {
    auto config = test_create_config(cs_fixture_dir("multitype_variant"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.t4 ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.t4 (id, val) VALUES (1, 1), (2, 2);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.t4 (id, val) VALUES (3, 'hello');")->is_success());

    SECTION("::?string selects the string variant (bigint rows are NULL)") {
        auto cur = exec("SELECT id, val::?string FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(1, 0).is_null());
        REQUIRE(cur->value(1, 1).is_null());
        REQUIRE(cur->value(1, 2).value<std::string_view>() == "hello");
    }

    SECTION("::?bigint selects the bigint variant (string row is NULL)") {
        auto cur = exec("SELECT id, val::?bigint FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 2).is_null());
    }

    SECTION("::?type works in WHERE") {
        auto cur = exec("SELECT id, val::?bigint FROM cs_testdb.t4 WHERE val::?bigint > 0 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 2);
    }

    SECTION("an unselected multi-type reference is an error") {
        REQUIRE_FALSE(exec("SELECT val FROM cs_testdb.t4;")->is_success());
    }
}

TEST_CASE("integration::cpp::test_computed_schema::jsonb_operators_with_multitype") {
    auto config = test_create_config(cs_fixture_dir("jsonb_multitype"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.m ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.m (x, v, a.b) VALUES (1, 10, 100);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.m (x, v) VALUES (2, 'str');")->is_success());

    SECTION("->> resolves a single-type field next to a multi-type one") {
        auto cur = exec("SELECT m ->> 'x' FROM cs_testdb.m ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
    }

    SECTION("-> ... ->> resolves a nested single-type leaf") {
        auto cur = exec("SELECT m -> 'a' ->> 'b' FROM cs_testdb.m ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 100);
        REQUIRE(cur->value(0, 1).is_null());
    }

    SECTION("'-' in the select list is refused (regression, #622)") {
        // correct: m - 'v' drops every variant of the multi-type field, leaving {x, a/b}.
        require_value_position_refusal(exec("SELECT m - 'v' FROM cs_testdb.m;"));
    }

    SECTION("'?' on a present field") {
        auto cur = exec("SELECT x FROM cs_testdb.m WHERE m ? 'x' ORDER BY x;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("scalar nav to the multi-type name is ambiguous") {
        REQUIRE_FALSE(exec("SELECT m ->> 'v' FROM cs_testdb.m;")->is_success());
    }

    SECTION("::? still selects the multi-type variant") {
        auto cur = exec("SELECT v::?bigint FROM cs_testdb.m WHERE x = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::jsonb_multitype_semantics") {
    auto config = test_create_config(cs_fixture_dir("jsonb_mt_sem"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.mm ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id, v, a.b) VALUES (1, 10, 100);")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id, v, a.b) VALUES (2, 'sv', 'sb');")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.mm (id) VALUES (3);")->is_success());

    SECTION("'?' is true if ANY variant is non-null (false only if all null)") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm ? 'v' ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
    }

    SECTION("nested '?' over a multi-type leaf") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm -> 'a' ? 'b' ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("'::?' composes onto a jsonb-nav chain (bigint variant)") {
        auto cur = exec("SELECT id, mm -> 'a' ->> 'b' ::? bigint AS b FROM cs_testdb.mm ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 100);
        REQUIRE(cur->value(1, 1).is_null());
        REQUIRE(cur->value(1, 2).is_null());
    }

    SECTION("'::?' composes onto a jsonb-nav chain (string variant)") {
        auto cur = exec("SELECT id, mm -> 'a' ->> 'b' ::? string AS b FROM cs_testdb.mm ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(1, 0).is_null());
        REQUIRE(cur->value(1, 1).value<std::string_view>() == "sb");
        REQUIRE(cur->value(1, 2).is_null());
    }

    SECTION("scalar nav to a multi-type leaf without ::? is ambiguous") {
        REQUIRE_FALSE(exec("SELECT mm -> 'a' ->> 'b' FROM cs_testdb.mm;")->is_success());
    }

    SECTION("'::?' over a nav chain works in WHERE") {
        auto cur = exec("SELECT id FROM cs_testdb.mm WHERE mm -> 'a' ->> 'b' ::? bigint > 50 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    }
}

// Regression: unary minus ('-x') has a null left operand; jsonb-delete detection must not mistake it for a delete.
TEST_CASE("integration::cpp::test_computed_schema::unary_minus_not_jsonb_delete") {
    auto config = test_create_config(cs_fixture_dir("unary_minus"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE cs_testdb;")->is_success());
    REQUIRE(exec("CREATE TABLE cs_testdb.u ();")->is_success());
    REQUIRE(exec("INSERT INTO cs_testdb.u (x) VALUES (5), (7);")->is_success());

    auto cur = exec("SELECT -x FROM cs_testdb.u ORDER BY x;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->value(0, 0).value<int64_t>() == -5);
    REQUIRE(cur->value(0, 1).value<int64_t>() == -7);
}
